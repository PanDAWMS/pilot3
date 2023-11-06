#!/usr/bin/env python
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
# - Shuwei Ye, yesw@bnl.gov, 2021
# - Paul Nilsson, paul.nilsson@cern.ch, 2021-23

import os
import time
import json
from pilot.util.config import config
from pilot.util.https import cacert
# from pilot.util.proxy import create_cert_files
from pilot.util.transport import HttpTransport
from logging import Logger, INFO
import logging

try:
    import google.cloud.logging
    from google.cloud.logging_v2.handlers import CloudLoggingHandler
except ImportError:
    pass

logger = logging.getLogger(__name__)


def get_realtime_logger(args=None, info_dic=None, workdir=None, secrets=""):
    """
    Helper function for real-time logger.

    The info_dic dictionary has the format: {'logging_type': .., 'protocol': .., 'url': .., 'port': .., 'logname': ..}

    :param args: pilot arguments object.
    :param info_dic: info dictionary.
    :param workdir: job working directory (string).
    :return: RealTimeLogger instance (self).
    """

    if RealTimeLogger.glogger is None:
        RealTimeLogger(args, info_dic, workdir, secrets)
    return RealTimeLogger.glogger


def cleanup():
    """
    Clean-up function for external use.
    """

    logger.debug('attempting real-time logger cleanup')
    if RealTimeLogger.glogger:
        RealTimeLogger.glogger.cleanup()
        logger.debug('real-time logger has been cleaned up')


# RealTimeLogger is called if args.realtimelogger is on
class RealTimeLogger(Logger):
    """
    RealTimeLogger class definition.
    """

    glogger = None
    jobinfo = {}
    logfiles = []
    logfiles_default = []
    openfiles = {}
    _cacert = ""
    current_handler = None  # needed for removing logger object from outside function

    def __init__(self, args, info_dic, workdir, secrets, level=INFO):
        """
        Default init function.

        The info_dic has the format: {'logging_type': ..,
                                      'protocol': ..,
                                      'url': ..,
                                      'port': ..,
                                      'logname': ..,
                                      'logfiles': [..]}

        :param args: pilot arguments object.
        :param info_dic: info dictionary.
        :param workdir: job working directory (string).
        :param level: logging level (constant).
        :return:
        """

        super(RealTimeLogger, self).__init__(name="realTimeLogger", level=level)
        RealTimeLogger.glogger = self

        if not info_dic:
            logger.warning('info dictionary not set - add \'logging=type:protocol://host:port\' to PQ.catchall)')
            RealTimeLogger.glogger = None
            return

        self._cacert = cacert(args)
        name = info_dic.get('logname')
        protocol = info_dic.get('protocol')  # needed for at least logstash
        server = protocol + '://' + info_dic.get('url')
        port = info_dic.get('port')
        logtype = info_dic.get('logging_type')
        self.logfiles_default = info_dic.get('logfiles')
        if 'http://' in server:
            server = server.replace('http://', '')
        logger.info(f'name={name}, protocol={protocol}, server={server}, port={port}, logtype={logtype}')
        if not name or not protocol or not server or not port or not logtype:
            if logtype != "google-cloud-logging":
                logger.warning('not enough information for setting up logging')
                RealTimeLogger.glogger = None
                return

        _handler = None

        try:
            if logtype == "google-cloud-logging":
                client = google.cloud.logging.Client()
                _handler = CloudLoggingHandler(client, name=name)
                api_logger = logging.getLogger('google.cloud.logging_v2')
                api_logger.setLevel(INFO)
            elif logtype == "fluent":
                from fluent import handler
                _handler = handler.FluentHandler(name, host=server, port=port)
            elif logtype == "logstash":
                # from logstash_async.transport import HttpTransport
                from logstash_async.handler import AsynchronousLogstashHandler
                # from logstash_async.handler import LogstashFormatter

                # certificate method (still in development):

                #certdir = os.environ.get('SSL_CERT_DIR', '')
                #path = os.path.join(certdir, "CERN-GridCA.pem")
                #crt, key = create_cert_files(workdir)
                #if not crt or not key:
                #    logger.warning('failed to create crt/key')
                #    _handler = None
                #    return
                #transport = HttpTransport(
                #    server,
                #    port,
                #    timeout=5.0,
                #    ssl_enable=True,
                #    ssl_verify=path,
                #    cert=(crt, key)
                #)

                # login+password method:
                if isinstance(secrets, str):
                    secrets = json.loads(secrets)

                ssl_enable, ssl_verify = self.get_rtlogging_ssl()
                transport = HttpTransport(
                    server,
                    port,
                    ssl_enable=ssl_enable,
                    ssl_verify=ssl_verify,
                    timeout=5.0,
                    username=secrets.get('logstash_login', 'unknown_login'),
                    password=secrets.get('logstash_password', 'unknown_password')
                )

                # create the handler
                _handler = AsynchronousLogstashHandler(
                    server,
                    port,
                    transport=transport,
                    database_path='logstash_test.db'
                )

            else:
                logger.warning(f'unknown logtype: {logtype}')
                _handler = None
        except (ModuleNotFoundError, ImportError) as exc:
            logger.warning(f'exception caught while setting up log handlers: {exc}')
            _handler = None

        if _handler is not None:
            self.addHandler(_handler)
            self.current_handler = _handler
        else:
            RealTimeLogger.glogger = None
            del self

    def cleanup(self):
        """
        Clean-up.
        """

        # close open files, if anything is still open
        self.close_files()

        # remove handler
        if self.current_handler:
            logger.debug(f'removing current handler: {self.current_handler}')
            self.removeHandler(self.current_handler)

        # commit suicide
        RealTimeLogger.glogger = None
        del self

    def set_jobinfo(self, job):
        self.jobinfo = {"TaskID": job.taskid, "PandaJobID": job.jobid}
        if 'HARVESTER_WORKER_ID' in os.environ:
            self.jobinfo["Harvester_WorkerID"] = os.environ.get('HARVESTER_WORKER_ID')
        if 'HARVESTER_ID' in os.environ:
            self.jobinfo["Harvester_ID"] = os.environ.get('HARVESTER_ID')
        if job.requestid:
            self.jobinfo["RequestID"] = job.requestid

    # prepend some panda job info
    # check if the msg is a dict-based object via isinstance(msg,dict),
    # then decide how to insert the PandaJobInf
    def send_with_jobinfo(self, msg):
        logobj = self.jobinfo.copy()
        logobj['PilotTimeStamp'] = time.time()
        try:
            msg = json.loads(msg)
            logobj.update(msg)
        except Exception:
            logobj["message"] = msg

        self.info(logobj)

    def add_logfiles(self, job_or_filenames, reset=True):
        self.close_files()
        if reset:
            self.logfiles = []
        if isinstance(job_or_filenames, list):
            for logfile in job_or_filenames:
                self.logfiles += [logfile]
        else:
            job = job_or_filenames
            for logfile in self.logfiles_default:
                if not logfile.startswith('/'):
                    logfile = os.path.join(job.workdir, logfile)
                self.logfiles += [logfile]
            if len(self.logfiles_default) == 0:
                stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
                self.logfiles += [stdout]
                # stderr = os.path.join(job.workdir, config.Payload.payloadstderr)
                # self.logfiles += [stderr]
        if len(self.logfiles) > 0:
            logger.info(f'added log files: {self.logfiles}')

    def close_files(self):
        for openfile in self.openfiles.values():
            if openfile is not None:
                openfile.close()
        self.openfiles = {}
        self.logfiles = []

    def send_loginfiles(self):
        for openfile in self.openfiles.values():
            if openfile is not None:
                lines = openfile.readlines()
                for line in lines:
                    self.send_with_jobinfo(line.strip())

    def sending_logs(self, args, job):
        logger.info('starting RealTimeLogger.sending_logs')
        self.set_jobinfo(job)
        self.add_logfiles(job)
        i = 0
        t_start = time.time()
        cutoff = 10 * 60   # 10 minutes
        while not args.graceful_stop.is_set():
            i += 1
            if i % 10 == 1:
                logger.debug(f'RealTimeLogger iteration #{i} (job state={job.state}, logfiles={self.logfiles})')
            # there might be special cases when RT logs should be sent, e.g. for pilot logs
            if job.state == '' or job.state == 'starting' or job.state == 'running':
                if len(self.logfiles) > len(self.openfiles):
                    for logfile in self.logfiles:
                        if logfile not in self.openfiles:
                            if os.path.exists(logfile):
                                openfile = open(logfile)
                                openfile.seek(0)
                                self.openfiles[logfile] = openfile
                                logger.debug(f'opened logfile: {logfile}')

                # logger.debug(f'real-time logging: sending logs for state={job.state} [1]')
                self.send_loginfiles()
            elif job.state == 'stagein' or job.state == 'stageout':
                logger.debug('no real-time logging during stage-in/out')
                pass
            else:
                # run longer for pilotlog
                # wait for job.completed=True, for a maximum of N minutes
                if ['pilotlog.txt' in logfile for logfile in self.logfiles] == [True]:
                    if not job.completed and (time.time() - t_start < cutoff):
                        time.sleep(5)
                        continue
                    logger.info(f'aborting real-time logging of pilot log after {time.time() - t_start} s (cut off: {cutoff} s)')

                logger.info(f'sending last real-time logs for job {job.jobid} (state={job.state})')
                self.send_loginfiles()  # send the remaining logs after the job completion
                self.close_files()
                break
            time.sleep(5)
        else:
            logger.debug('sending last real-time logs')
            self.send_loginfiles()  # send the remaining logs after the job completion
            self.close_files()
        logger.info('finished sending real-time logs')

    def get_rtlogging_ssl(self):
        """
        Return the proper rtlogging value from the experiment specific plug-in or the config file.

        :return: ssl_enable (bool), ssl_verify (bool) (tuple).
        """

        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        try:
            user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)
            ssl_enable, ssl_verify = user.get_rtlogging_ssl()
        except Exception:
            ssl_enable = config.Pilot.ssl_enable
            ssl_verify = config.Pilot.ssl_verify
            logger.warning(f'found no experiment specific ssl_enable, ssl_verify, using config values ({ssl_enable}, {ssl_verify})')

        return ssl_enable, ssl_verify
