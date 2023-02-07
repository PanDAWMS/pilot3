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
from pilot.common.exception import FileHandlingFailure

logger = logging.getLogger(__name__)


class Features(object):

    def __init__(self):
        """
        Default init.
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

        :return: class dictionary.
        """

        from json import dumps, loads
        # convert class data members to a dictionary string (dumps), then to a dictionary (loads)
        # note that all data members will remain as strings
        return loads(dumps(self, default=lambda par: par.__dict__))

    def set(self, path, label):
        """
        Set all values.

        :param path: path to job or machine features directory.
        :param label: machine or job string.
        :return:
        """

        if path and os.path.exists(path):
            data_members = self.get_data_members()
            for member in data_members:
                try:
                    value = read_file(os.path.join(path, member))
                except FileHandlingFailure as exc:
                    logger.warning(f'failed to process {member}: {exc}')
                    value = None

                if value:
                    value = value[:-1] if value.endswith('\n') else value
                    setattr(self, member, value)
        else:
            logger.info(f'{label} features path does not exist (path=\"{path}\")')


class MachineFeatures(Features):

    def __init__(self):
        """
        Default init.
        """

        super().__init__()

        # machine features
        self.hs06 = ""
        self.shutdowntime = ""
        self.total_cpu = ""
        self.grace_secs = ""

        logger.info('collecting machine features')
        self.set(os.environ.get('MACHINEFEATURES', ''), 'machine')


class JobFeatures(Features):

    def __init__(self):
        """
        Default init.
        """

        super().__init__()

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
        self.set(os.environ.get('JOBFEATURES', ''), 'job')
