#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2023

# from pilot.util.filehandling import read_file

import logging
import os

from pilot.util.filehandling import read_file

logger = logging.getLogger(__name__)


class Features(object):

    def __init__(self):
        """

        """

        pass

    def get_data_members(self):
        """
        Return all data members.

        :return: list of data members.
        """

        return [attr for attr in dir(self) if not callable(getattr(self, attr)) and not attr.startswith("__")]

    def get(self):
        """
        Convert class to dictionary.
        """

        from json import dumps
        return dumps(self, default=lambda par: par.__dict__)


class MachineFeatures(Features):

    def __init__(self):

        # treat float and int values as strings

        # machine features
        self.hs06 = ""
        self.shutdowntime = ""
        self.total_cpu = ""
        self.grace_secs = ""

        logger.info('collecting machine features')
        path = os.environ.get('MACHINEFEATURES', '')
        if path and os.path.exists(path):
            data_members = self.get_data_members()
            for member in data_members:
                value = read_file(os.path.join(path, member))
                if value:
                    setattr(self, member, value)
        else:
            logger.info('machine features path does not exist (path=\"{path}\")')


class JobFeatures(Features):

    def __init__(self):

        # treat float and int values as strings

        # job features
        self.allocated_cpu = ""
        self.hs06_job = ""
        self.shutdowntime_job = ""
        self.grace_secs_job = ""
        self.jobstart_secs = ""
        self.job_id = ""
        self.wall_limit_secs = ""
        self.cpu_limit_secs = ""
        self.max_rss_bytes = ""
        self.max_swap_bytes = ""
        self.scratch_limit_bytes = ""

        logger.info('collecting job features')
        path = os.environ.get('JOBFEATURES', '')
        if path and os.path.exists(path):
            data_members = self.get_data_members()
            for member in data_members:
                value = read_file(os.path.join(path, member))
                if value:
                    setattr(self, member, value)
        else:
            logger.info('job features path does not exist (path=\"{path}\")')
