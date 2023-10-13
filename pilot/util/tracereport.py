# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Authors:
# - Alexey Anisenkov, alexey.anisenkov@cern.ch, 2017
# - Pavlo Svirin, pavlo.svirin@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-23

import hashlib
import os
import socket
import time
from sys import exc_info
from json import dumps
from os import environ, getuid

from pilot.util.config import config
from pilot.util.constants import get_pilot_version, get_rucio_client_version
from pilot.util.container import execute, execute2
from pilot.common.exception import FileHandlingFailure
from pilot.util.filehandling import append_to_file, write_file

import logging
logger = logging.getLogger(__name__)


class TraceReport(dict):

    ipv = 'IPv6'
    workdir = ''

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
        self.workdir = kwargs.get('workdir', '')  # workdir is needed for streaming the curl output, but should not be included in the report

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

        hostname = os.environ.get('PANDA_HOSTNAME', socket.gethostname())
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
        Return trace report value for given key.

        :param key: key (str)
        :return: trace report value (Any).
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

        out = None
        err = None
        try:
            # take care of the encoding
            data = dumps(self).replace('"', '\\"')
            # remove the ipv and workdir items since they are for internal pilot use only
            data = data.replace(f'\"ipv\": \"{self.ipv}\", ', '')
            data = data.replace(f'\"workdir\": \"{self.workdir}\", ', '')

            ssl_certificate = self.get_ssl_certificate()

            # create the command
            command = 'curl'
            if self.ipv == 'IPv4':
                command += ' -4'

            # stream the output to files to prevent massive reponses that could overwhelm subprocess.communicate() in execute()
            outname, errname = self.get_trace_curl_filenames(name='trace_curl_last')
            out, err = self.get_trace_curl_files(outname, errname)
            logger.debug(f'using {outname} and {errname} to store curl output')
            cmd = f'{command} --connect-timeout 100 --max-time 120 --cacert {ssl_certificate} -v -k -d \"{data}\" {url}'
            exit_code = execute2(cmd, out, err, 300)
            logger.debug(f'exit_code={exit_code}')

            # always append the output to trace_curl.std{out|err}
            outname_final, errname_final = self.get_trace_curl_filenames(name='trace_curl')
            _ = append_to_file(outname, outname_final)
            _ = append_to_file(errname, errname_final)
            self.close(out, err)

            # handle errors that only appear in stdout/err (curl)
            if not exit_code:
                out, err = self.get_trace_curl_files(outname, errname, mode='r')
                exit_code = self.assign_error(out)
                if not exit_code:
                    exit_code = self.assign_error(err)
                logger.debug(f'curl exit_code from stdout/err={exit_code}')
                self.close(out, err)
            if not exit_code:
                logger.info('no errors were detected from curl operation')
            else:
                # better to store exit code in file since env var will not be seen outside container in case middleware
                # container is used
                path = os.path.join(self.workdir, config.Rucio.rucio_trace_error_file)
                try:
                    write_file(path, str(exit_code))
                except FileHandlingFailure as exc:
                    logger.warning(f'failed to store curl exit code to file: {exc}')
                else:
                    logger.info(f'wrote rucio trace exit code {exit_code} to file {path}')
                logger.debug(f"setting env var RUCIO_TRACE_ERROR to \'{exit_code}\' to be sent with job metrics")
                os.environ['RUCIO_TRACE_ERROR'] = str(exit_code)

        except Exception:
            # if something fails, log it but ignore
            logger.error('tracing failed: %s' % str(exc_info()))
        else:
            logger.info("tracing report sent")

        return True

    def close(self, out, err):
        """
        Close all open file streams.
        """

        if out:
            out.close()
        if err:
            err.close()

    def assign_error(self, out):
        """
        Browse the stdout from curl line by line and look for errors.
        """

        exit_code = 0
        count = 0
        while True:
            count += 1

            # Get next line from file
            line = out.readline()

            # if line is empty
            # end of file is reached
            if not line:
                break
            if 'ExceptionClass' in line.strip():
                logger.warning(f'curl failure: {line.strip()}')
                exit_code = 1
                break

        return exit_code

    def get_trace_curl_filenames(self, name='trace_curl'):
        """
        Return file names for the curl stdout and stderr.

        :param name: name pattern (str)
        :return: stdout file name (str), stderr file name (str).
        """

        workdir = self.workdir if self.workdir else os.getcwd()
        return os.path.join(workdir, f'{name}.stdout'), os.path.join(workdir, f'{name}.stderr')

    def get_trace_curl_files(self, outpath, errpath, mode='wb'):
        """
        Return file objects for the curl stdout and stderr.

        :param outpath: path for stdout (str)
        :param errpath: path for stderr (str)
        :return: out (file), err (file).
        """

        try:
            out = open(outpath, mode=mode)
            err = open(errpath, mode=mode)
        except IOError as error:
            logger.warning(f'failed to open curl stdout/err: {error}')
            out = None
            err = None

        return out, err

    def get_ssl_certificate(self):
        """
        Return the path to the SSL certificate

        :return: path (string).
        """

        return environ.get('X509_USER_PROXY', '/tmp/x509up_u%s' % getuid())
