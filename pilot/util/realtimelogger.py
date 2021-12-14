#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Shuwei Ye, yesw@bnl.gov, 2021
# - Paul Nilsson, paul.nilsson@cern.ch, 2021

import os
import time
import json
from pilot.util.config import config
from logging import Logger, INFO
import logging

logger = logging.getLogger(__name__)
## logServer = "google-cloud-logging"


def get_realtime_logger(args=None, info_dic=None):
    """
    Helper function for real-time logger.

    The info_dic dictionary has the format: {'logging_type': .., 'protocol': .., 'url': .., 'port': .., 'logname': ..}

    :param args: pilot arguments object.
    :param info_dic: info dictionary.
    :return: RealTimeLogger instance (self).
    """

    if RealTimeLogger.glogger is None:
        RealTimeLogger(args, info_dic)
    return RealTimeLogger.glogger


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

    def __init__(self, args, info_dic, level=INFO):
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
        :param level: logging level (constant).
        :return:
        """

        super(RealTimeLogger, self).__init__(name="realTimeLogger", level=level)
        RealTimeLogger.glogger = self

        if not info_dic:
            logger.warning('info dictionary not set - add \'logging=type:protocol://host:port\' to PQ.catchall)')
            RealTimeLogger.glogger = None
            return

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
            logger.warning('not enough information for setting up logging')
            RealTimeLogger.glogger = None
            return

        _handler = None

        try:
            server = 'aipanda020.cern.ch'
            port = 8443
            if logtype == "google-cloud-logging":
                import google.cloud.logging
                from google.cloud.logging_v2.handlers import CloudLoggingHandler
                client = google.cloud.logging.Client()
                _handler = CloudLoggingHandler(client, name=name)
            elif logtype == "fluent":
                from fluent import handler
                _handler = handler.FluentHandler(name, host=server, port=port)
            elif logtype == "logstash":
                from logstash_async.transport import HttpTransport
                from logstash_async.handler import AsynchronousLogstashHandler
                # from logstash_async.handler import LogstashFormatter
                transport = HttpTransport(
                    server,
                    port,
                    timeout=5.0,
                    ssl_enable=False,
                    ssl_verify=False,
                    user='pilot',
                    password='XXX'
                )
                # Create the handler
                _handler = AsynchronousLogstashHandler(
                    host=server,
                    port=port,
                    transport=transport,
                    ssl_enable=False,
                    ssl_verify=False,
                    database_path='logstash_test.db')
            else:
                logger.warning(f'unknown logtype: {logtype}')
                _handler = None
        except (ModuleNotFoundError, ImportError) as exc:
            logger.warning(f'exception caught while setting up log handlers: {exc}')
            _handler = None

        if _handler is not None:
            self.addHandler(_handler)
        else:
            RealTimeLogger.glogger = None
            del self

    def set_jobinfo(self, job):
        self.jobinfo = {"TaskID": job.taskid, "PandaJobID": job.jobid}
        if 'HARVESTER_WORKER_ID' in os.environ:
            self.jobinfo["Harvester_WorkerID"] = os.environ.get('HARVESTER_WORKER_ID')
        logger.debug('set_jobinfo with PandaJobID=%s', self.jobinfo["PandaJobID"])

    # prepend some panda job info
    # check if the msg is a dict-based object via isinstance(msg,dict),
    # then decide how to insert the PandaJobInf
    def send_with_jobinfo(self, msg):
        logobj = self.jobinfo.copy()
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
        while not args.graceful_stop.is_set():
            i += 1
            if i % 10 == 0:
                logger.debug(f'RealTimeLogger iteration #{i} (job state={job.state})')
            if job.state == '' or job.state == 'starting' or job.state == 'running':
                if len(self.logfiles) > len(self.openfiles):
                    for logfile in self.logfiles:
                        if logfile not in self.openfiles:
                            if os.path.exists(logfile):
                                openfile = open(logfile)
                                openfile.seek(0)
                                self.openfiles[logfile] = openfile
                                logger.debug(f'opened logfile: {logfile}')
                self.send_loginfiles()
            elif job.state == 'stagein' or job.state == 'stageout':
                pass
            else:
                self.send_loginfiles()  # send the remaining logs after the job completion
                self.close_files()
                break
            time.sleep(5)
        else:
            self.send_loginfiles()  # send the remaining logs after the job completion
            self.close_files()

# in pilot/control/payload.py
# define a new function run_realTimeLog(queues, traces, args)
# add add it into the thread list "targets"
