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
import re
from pilot.util.config import config
from logging import Logger, INFO
import logging

logger = logging.getLogger(__name__)
## logServer = "google-cloud-logging"


def get_realtime_logger(args=None):
    if RealTimeLogger.glogger is None:
        RealTimeLogger(args)
    return RealTimeLogger.glogger


# RealTimeLogger is called if args.realtimelogger is on
class RealTimeLogger(Logger):
    """
    RealTimeLogger class definition.
    """

    glogger = None

    def __init__(self, args):
        super(RealTimeLogger, self).__init__(name="realTimeLogger", level=INFO)
        RealTimeLogger.glogger = self

        self.jobInfo = {}
        self.logFiles = []
        self.logFiles_default = []
        self.openFiles = {}

        if args.use_realtime_logging:
            name = args.realtime_logname
            if name:
                logserver = args.realtime_logging_server
            else:
                logserver = ""
            logfiles = os.environ.get('REALTIME_LOGFILES')
            if logfiles is not None:
                self.logFiles_default = re.split('[:,]', logfiles)

        items = logserver.split(':')
        logtype = items[0].lower()
        h = None

        try:
            if logtype == "google-cloud-logging":
                import google.cloud.logging
                from google.cloud.logging_v2.handlers import CloudLoggingHandler
                client = google.cloud.logging.Client()
                h = CloudLoggingHandler(client, name=name)
                self.addHandler(h)

            elif logtype == "fluent":
                if len(items) < 3:
                    RealTimeLogger.glogger = None
                fluentserver = items[1]
                fluentport = items[2]
                from fluent import handler
                h = handler.FluentHandler(name, host=fluentserver, port=fluentport)
            elif logtype == "logstash":
                pass
            else:
                pass
        except Exception as exc:
            logger.warning(f'exception caught while setting up log handlers: {exc}')
            h = None

        if h is not None:
            self.addHandler(h)
        else:
            RealTimeLogger.glogger = None
            del self

    def set_jobinfo(self, job):
        self.jobInfo = {"TaskID": job.taskid, "PandaJobID": job.jobid}
        if 'HARVESTER_WORKER_ID' in os.environ:
            self.jobInfo["Harvester_WorkerID"] = os.environ.get('HARVESTER_WORKER_ID')
        logger.debug('set_jobinfo with PandaJobID=%s', self.jobInfo["PandaJobID"])

    # prepend some panda job info
    # check if the msg is a dict-based object via isinstance(msg,dict),
    # then decide how to insert the PandaJobInf
    def send_with_jobinfo(self, msg):
        logobj = self.jobInfo.copy()
        try:
            msg = json.loads(msg)
            logobj.update(msg)
        except Exception:
            logobj["message"] = msg
        self.info(logobj)

    def add_logfiles(self, job_or_filenames, reset=True):
        self.close_files()
        if reset:
            self.logFiles = []
        if type(job_or_filenames) == list:
            logfiles = job_or_filenames
            for logfile in logfiles:
                self.logfiles += [logfile]
        else:
            job = job_or_filenames
            for logfile in self.logFiles_default:
                if not logfile.startswith('/'):
                    logfile = os.path.join(job.workdir, logfile)
                self.logFiles += [logfile]
            if len(self.logFiles_default) == 0:
                stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
                self.logFiles += [stdout]
                # stderr = os.path.join(job.workdir, config.Payload.payloadstderr)
                # self.logFiles += [stderr]
        if len(self.logFiles) > 0:
            logger.info('Added log files:%s', self.logFiles)

    def close_files(self):
        for openfile in self.openFiles.values():
            if openfile is not None:
                openfile.close()
        self.openFiles = {}
        self.logFiles = []

    def send_loginfiles(self):
        for openfile in self.openFiles.values():
            if openfile is not None:
                lines = openfile.readlines()
                for line in lines:
                    self.send_with_jobinfo(line.strip())

    def sending_logs(self, args, job):
        logger.info('Starting RealTimeLogger.sending_logs')
        self.set_jobinfo(job)
        self.add_logfiles(job)
        while not args.graceful_stop.is_set():
            if job.state == '' or job.state == 'starting' or job.state == 'running':
                if len(self.logFiles) > len(self.openFiles):
                    for logfile in self.logFiles:
                        if logfile not in self.openFiles:
                            if os.path.exists(logfile):
                                openfile = open(logfile)
                                openfile.seek(0)
                                self.openFiles[logfile] = openfile
                                logger.debug('opened logfile:%s', logfile)
                self.send_loginfiles()
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
