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
# - Paul Nilsson, paul.nilsson@cern.ch, 2021-24
# - Wen Guan, wen.guan@cern.ch, 2024

"""Real-time logger."""

import json
import logging
import os
import time

try:
    from fluent import handler as fluent_handler
except ImportError:
    pass

try:
    import google.cloud.logging
    from google.cloud.logging_v2.handlers import CloudLoggingHandler
except ImportError:
    pass

try:
    from logstash_async.handler import AsynchronousLogstashHandler
except ImportError:
    pass

try:
    from pilot.util.lokirealtimelogger import setup_loki_handler
except ImportError:
    pass

from typing import Any

from pilot.util.config import config
from pilot.util.https import cacert
# from pilot.util.proxy import create_cert_files
from pilot.util.transport import HttpTransport

logger = logging.getLogger(__name__)


def get_realtime_logger(args: Any = None, info_dic: dict = None, workdir: str = None, secrets: str = ""):
    """Return the singleton RealTimeLogger instance, creating it if needed.

    *info_dic* must have the format::

        {'logging_type': .., 'protocol': .., 'url': .., 'port': .., 'logname': ..}

    Args:
        args: Pilot arguments object.
        info_dic: Logging configuration dictionary.
        workdir: Job working directory.
        secrets: JSON-encoded credentials string for authenticated log transports.

    Returns:
        The global ``RealTimeLogger`` instance, or ``None`` if initialisation failed.
    """
    if RealTimeLogger.glogger is None:
        RealTimeLogger(args, info_dic, workdir, secrets)

    return RealTimeLogger.glogger


def cleanup():
    """Clean-up function for external use."""
    logger.debug('attempting real-time logger cleanup')
    if RealTimeLogger.glogger:
        RealTimeLogger.glogger.cleanup()
        logger.debug('real-time logger has been cleaned up')


# RealTimeLogger is called if args.realtimelogger is on
class RealTimeLogger(logging.Logger):
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

    def __init__(self, args: Any, info_dic: dict, workdir: str, secrets: str, level: Any = logging.INFO):
        """Initialize the RealTimeLogger and configure the appropriate log handler.

        *info_dic* must have the format::

            {
                'logging_type': ..,
                'protocol': ..,
                'url': ..,
                'port': ..,
                'logname': ..,
                'logfiles': [..]
            }

        Args:
            args: Pilot arguments object used for SSL certificate resolution.
            info_dic: Logging configuration dictionary. If ``None`` or empty,
                initialisation aborts and ``glogger`` is set to ``None``.
            workdir: Job working directory (reserved for future use).
            secrets: JSON-encoded credentials for authenticated transports
                (e.g. Logstash login and password).
            level: Python logging level for this logger.
        """
        super().__init__(name="realTimeLogger", level=level)
        RealTimeLogger.glogger = self

        if workdir:  # bypass pylint warning - keep workdir for possible future development
            pass
        if not info_dic:
            logger.warning('info dictionary not set - add \'logging=type;protocol://host:port\' to PQ.catchall)')
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

        if logtype == "google-cloud-logging":
            client = google.cloud.logging.Client()
            _handler = CloudLoggingHandler(client, name=name)
            api_logger = logging.getLogger('google.cloud.logging_v2')
            api_logger.setLevel(logging.INFO)
        elif logtype == "fluent":
            _handler = fluent_handler.FluentHandler(name, host=server, port=port)
        elif logtype == "logstash":
            # from logstash_async.transport import HttpTransport
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
        elif logtype == 'loki':
            logger.info("setting up loki handler")
            _handler = setup_loki_handler(name)
        else:
            logger.warning(f'unknown logtype: {logtype}')
            _handler = None

        if _handler is not None:
            self.addHandler(_handler)
            self.current_handler = _handler
        else:
            RealTimeLogger.glogger = None
            del self

    def cleanup(self):
        """Clean-up."""
        # close open files, if anything is still open
        self.close_files()

        # remove handler
        if self.current_handler:
            logger.debug(f'removing current handler: {self.current_handler}')
            self.removeHandler(self.current_handler)

        # commit suicide
        RealTimeLogger.glogger = None
        del self

    def set_jobinfo(self, job: Any) -> None:
        """Populate the job info dict from the given job object.

        Sets ``self.jobinfo`` with the task ID, PanDA job ID, and optional
        Harvester worker ID, Harvester ID, and request ID read from environment
        variables and *job* attributes.

        Args:
            job: Job object with ``taskid``, ``jobid``, and ``requestid`` attributes.
        """
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
    def send_with_jobinfo(self, msg: Any) -> None:
        """Send a log message prefixed with current job metadata and timestamp.

        Merges ``self.jobinfo`` and the current pilot timestamp with *msg*.
        If *msg* is valid JSON it is merged as a dict; otherwise it is stored
        under the ``'message'`` key.

        Args:
            msg: Log message string or JSON-serialisable object.
        """
        logobj = self.jobinfo.copy()
        logobj['PilotTimeStamp'] = time.time()
        try:
            msg = json.loads(msg)
            logobj.update(msg)
        except Exception:
            logobj["message"] = msg

        self.info(logobj)

    def add_logfiles(self, job_or_filenames: Any, reset: bool = True) -> None:
        """Register log files to be streamed by the real-time logger.

        Closes any currently open files before updating the list. When
        *job_or_filenames* is a list those paths are added directly. When it is
        a job object the paths are resolved from ``self.logfiles_default``
        relative to the job's work directory, falling back to the payload stdout
        file when no defaults are configured.

        Args:
            job_or_filenames: A list of log file paths, or a job object whose
                ``workdir`` attribute is used to resolve relative paths.
            reset: If True, the current log file list is cleared before adding.
        """
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
        """Close files."""
        for openfile in self.openfiles.values():
            if openfile is not None:
                openfile.close()
        self.openfiles = {}
        self.logfiles = []

    def send_loginfiles(self):
        """Send log files."""
        for openfile in self.openfiles.values():
            if openfile is not None:
                lines = openfile.readlines()
                for line in lines:
                    self.send_with_jobinfo(line.strip())

    def sending_logs(self, args: Any, job: Any) -> None:
        """Stream log files to the real-time logging backend until the job finishes.

        Opens log files as they appear, reads new lines every 5 seconds, and
        sends them via :meth:`send_with_jobinfo`. Exits when the job completes,
        a final send is performed for pilot-log files, or ``args.graceful_stop``
        is set.

        Args:
            args: Pilot arguments object. The loop exits when
                ``args.graceful_stop`` is set.
            job: Running job object providing ``state``, ``jobid``, ``workdir``,
                and ``completed`` attributes.
        """
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
            if job.state in {'', 'starting', 'running'}:
                if len(self.logfiles) > len(self.openfiles):
                    for logfile in self.logfiles:
                        if logfile not in self.openfiles:
                            if os.path.exists(logfile):
                                openfile = open(logfile, encoding='utf-8')
                                if openfile:
                                    openfile.seek(0)
                                    self.openfiles[logfile] = openfile
                                    logger.debug(f'opened logfile: {logfile}')

                # logger.debug(f'real-time logging: sending logs for state={job.state} [1]')
                self.send_loginfiles()
            elif job.state in {'stagein', 'stageout'}:
                logger.debug('no real-time logging during stage-in/out')
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

    def get_rtlogging_ssl(self) -> tuple:
        """Return SSL configuration for real-time logging.

        Attempts to retrieve ``ssl_enable`` and ``ssl_verify`` from the
        experiment-specific plugin. Falls back to ``config.Pilot.ssl_enable``
        and ``config.Pilot.ssl_verify`` if the plugin raises an exception.

        Returns:
            Tuple of ``(ssl_enable, ssl_verify)`` booleans.
        """

        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        try:
            user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)
            ssl_enable, ssl_verify = user.get_rtlogging_ssl()
        except Exception:
            ssl_enable = config.Pilot.ssl_enable
            ssl_verify = config.Pilot.ssl_verify
            logger.warning(f'found no experiment specific ssl_enable, ssl_verify, using config values ({ssl_enable}, {ssl_verify})')

        return ssl_enable, ssl_verify
