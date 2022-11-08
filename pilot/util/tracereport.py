# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Alexey Anisenkov, alexey.anisenkov@cern.ch, 2017
# - Pavlo Svirin, pavlo.svirin@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import hashlib
import os
import socket
import time
from sys import exc_info
from json import dumps  #, loads
from os import environ, getuid

from pilot.util.config import config
from pilot.util.constants import get_pilot_version, get_rucio_client_version
from pilot.util.container import execute
#from pilot.util.https import request

import logging
logger = logging.getLogger(__name__)


class TraceReport(dict):

    ipv = 'IPv6'

    def __init__(self, *args, **kwargs):

        event_version = "%s+%s" % (get_pilot_version(), get_rucio_client_version())
        defs = {  # for reference, see Tracing report document in wiki area of Pilot GitHub repository
            'eventType': '',
            'eventVersion': event_version,  # Pilot+Rucio client version
            'protocol': None,               # set by specific copy tool
            'clientState': 'INIT_REPORT',
            'localSite': environ.get('RUCIO_LOCAL_SITE_ID', ''),
            'remoteSite': '',
            'timeStart': None,
            'catStart': None,
            'relativeStart': None,
            'transferStart': None,
            'validateStart': None,
            'timeEnd': None,
            'dataset': '',
            'version': None,
            'duid': None,
            'filename': None,
            'guid': None,
            'filesize': None,
            'usr': None,
            'appid': None,
            'hostname': '',
            'ip': '',
            'suspicious': '0',
            'usrdn': '',
            'url': None,
            'stateReason': None,
            'uuid': None,
            'taskid': '',
            'pq': environ.get('PILOT_SITENAME', '')
        }

        super(TraceReport, self).__init__(defs)
        self.update(dict(*args, **kwargs))  # apply extra input
        self.ipv = kwargs.get('ipv', 'IPv6')  # ipv (internet protocol version) is needed below for the curl command, but should not be included in the report

    # sitename, dsname, eventType
    def init(self, job):
        """
        Initialization.

        :param job: job object.
        :return:
        """

        data = {
            'clientState': 'INIT_REPORT',
            'usr': hashlib.md5(job.produserid.encode('utf-8')).hexdigest(),  # anonymise user and pilot id's, Python 2/3
            'appid': job.jobid,
            'usrdn': job.produserid,
            'taskid': job.taskid
        }
        self.update(data)
        self['timeStart'] = time.time()

        hostname = os.environ.get('PAMDA_HOSTNAME', socket.gethostname())
        try:
            self['hostname'] = socket.gethostbyaddr(hostname)[0]
        except Exception:
            logger.debug("unable to detect hostname for trace report")

        try:
            self['ip'] = socket.gethostbyname(hostname)
        except Exception:
            logger.debug("unable to detect host IP for trace report")

        if job.jobdefinitionid:
            s = 'ppilot_%s' % job.jobdefinitionid
            self['uuid'] = hashlib.md5(s.encode('utf-8')).hexdigest()  # hash_pilotid, Python 2/3
        else:
            #self['uuid'] = commands.getoutput('uuidgen -t 2> /dev/null').replace('-', '')  # all LFNs of one request have the same uuid
            cmd = 'uuidgen -t 2> /dev/null'
            exit_code, stdout, stderr = execute(cmd)
            self['uuid'] = stdout.replace('-', '')

    def get_value(self, key):
        """

        """

        return self.get(key, None)

    def verify_trace(self):
        """
        Verify the trace consistency.
        Are all required fields set? Remove escape chars from stateReason if present.

        :return: Boolean.
        """

        # remove any escape characters that might be present in the stateReason field
        state_reason = self.get('stateReason', '')
        if not state_reason:
            state_reason = ''
        self.update(stateReason=state_reason.replace('\\', ''))

        # overwrite any localSite if RUCIO_LOCAL_SITE_ID is set
        localsite = environ.get('RUCIO_LOCAL_SITE_ID', '')
        if localsite:
            self['localSite'] = localsite

        if not self['eventType'] or not self['localSite'] or not self['remoteSite']:
            return False
        else:
            return True

    def send(self):
        """
        Send trace to rucio server using curl.

        :return: Boolean.
        """

        # only send trace if it is actually required (can be turned off with pilot option)
        if environ.get('PILOT_USE_RUCIO_TRACES', 'True') == 'False':
            logger.debug('rucio trace does not need to be sent')
            return True

        url = config.Rucio.url
        logger.info("tracing server: %s" % url)
        logger.info("sending tracing report: %s" % str(self))

        if not self.verify_trace():
            logger.warning('cannot send trace since not all fields are set')
            return False

        try:
            # take care of the encoding
            data = dumps(self).replace('"', '\\"')
            # remove the ipv item since it's for internal pilot use only
            data = data.replace(f'\"ipv\": \"{self.ipv}\", ', '')

            ssl_certificate = self.get_ssl_certificate()

            # create the command
            command = 'curl'
            if self.ipv == 'IPv4':
                command += ' -4'
            cmd = f'{command} --connect-timeout 20 --max-time 120 --cacert {ssl_certificate} -v -k -d \"%{data}\" {url}'
            exit_code, stdout, stderr = execute(cmd, mute=True)
            if exit_code:
                logger.warning('failed to send traces to rucio: %s' % stdout)
        except Exception:
            # if something fails, log it but ignore
            logger.error('tracing failed: %s' % str(exc_info()))
        else:
            logger.info("tracing report sent")

        return True

    def get_ssl_certificate(self):
        """
        Return the path to the SSL certificate

        :return: path (string).
        """

        return environ.get('X509_USER_PROXY', '/tmp/x509up_u%s' % getuid())
