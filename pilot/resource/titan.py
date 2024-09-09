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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-24
# - Danila Oleynik danila.oleynik@cern.ch, 2018

"""Functions for Titan."""

import logging
import os
import shutil
import sys
import time
from typing import Any

from pilot.common.exception import FileHandlingFailure
from pilot.util.config import config
from pilot.util.constants import (
    PILOT_PRE_STAGEIN,
    PILOT_POST_STAGEIN
)
from pilot.util.filehandling import (
    read_json,
    write_json,
    remove
)
from pilot.util.timing import add_to_pilot_timing
from .jobdescription import JobDescription

logger = logging.getLogger(__name__)


def get_job(harvesterpath: str) -> (JobDescription, int):
    """
    Return job description in dictionary form and MPI rank (if applicable).

    :param harvesterpath: path to config.Harvester.jobs_list_file (string).
    :return: job object (JobDescription), rank (int).
    """
    rank = 0
    job = None
    logger.info("going to read job definition from file")

    pandaids_list_filename = os.path.join(harvesterpath, config.Harvester.jobs_list_file)
    if not os.path.isfile(pandaids_list_filename):
        logger.info("file with PanDA IDs are missing. nothing to execute.")
        return job, rank

    harvesterpath = os.path.abspath(harvesterpath)
    #rank, max_ranks = get_ranks_info()

    pandaids = read_json(pandaids_list_filename)
    logger.info(f'Got {len(pandaids)} job ids')
    pandaid = pandaids[rank]
    job_workdir = os.path.join(harvesterpath, str(pandaid))

    logger.info(f'rank: {rank} with job {pandaid} will have work directory {job_workdir}')

    job_def_filename = os.path.join(job_workdir, config.Harvester.pandajob_file)
    jobs_dict = read_json(job_def_filename)
    job_dict = jobs_dict[str(pandaid)]
    job = JobDescription()
    job.load(job_dict)

    return job, rank


def get_setup(job: Any = None) -> list:
    """
    Return the resource specific setup.

    :param job: optional job object (Any)
    :return: setup commands (list).
    """
    if not job:
        logger.warning('job object not sent to get_setup')
    setup_commands = ['source /ccs/proj/csc108/athena_grid_env/setup.sh',
                      'source $MODULESHOME/init/bash',
                      'tmp_dirname=/tmp/scratch',
                      'tmp_dirname+="/tmp"',
                      'export TEMP=$tmp_dirname',
                      'export TMPDIR=$TEMP',
                      'export TMP=$TEMP',
                      'export LD_LIBRARY_PATH=/ccs/proj/csc108/AtlasReleases/ldpatch:$LD_LIBRARY_PATH',
                      'export ATHENA_PROC_NUMBER=16',
                      'export G4ATLAS_SKIPFILEPEEK=1',
                      'export PANDA_RESOURCE=\"ORNL_Titan_MCORE\"',
                      'export ROOT_TTREECACHE_SIZE=1',
                      'export RUCIO_APPID=\"simul\"',
                      'export RUCIO_ACCOUNT=\"pilot\"',
                      'export CORAL_DBLOOKUP_PATH=/ccs/proj/csc108/AtlasReleases/21.0.15/nfs_db_files',
                      'export CORAL_AUTH_PATH=$SW_INSTALL_AREA/DBRelease/current/XMLConfig',
                      'export DATAPATH=$SW_INSTALL_AREA/DBRelease/current:$DATAPATH',
                      'unset FRONTIER_SERVER',
                      ' ']

    return setup_commands


def set_job_workdir(job: Any, path: str) -> str:
    """
    Point pilot to job working directory (job id).

    :param job: job object (Any)
    :param path: local path to Harvester access point (str)
    :return: job working directory (str).
    """
    work_dir = os.path.join(path, str(job.jobid))
    os.chdir(work_dir)

    return work_dir


def set_scratch_workdir(job: Any, work_dir: str, args: dict) -> str:
    """
    Copy input files and some db files to RAM disk.

    :param job: job object (Any)
    :param work_dir: job working directory (permanent FS) (str)
    :param args: args dictionary to collect timing metrics (dict)
    :return: job working directory in scratch (str)
    :raises FileHandlingFailure: in case of IOError.
    """
    scratch_path = config.HPC.scratch
    job_scratch_dir = os.path.join(scratch_path, str(job.jobid))
    for inp_file in job.input_files:
        job.input_files[inp_file]["scratch_path"] = job_scratch_dir
    logger.debug(f"Job scratch path: {job_scratch_dir}")
    # special data, that should be preplaced in RAM disk
    dst_db_path = 'sqlite200/'
    dst_db_filename = 'ALLP200.db'
    dst_db_path_2 = 'geomDB/'
    dst_db_filename_2 = 'geomDB_sqlite'
    tmp_path = 'tmp/'
    src_file = '/ccs/proj/csc108/AtlasReleases/21.0.15/DBRelease/current/sqlite200/ALLP200.db'
    src_file_2 = '/ccs/proj/csc108/AtlasReleases/21.0.15/DBRelease/current/geomDB/geomDB_sqlite'

    if os.path.exists(scratch_path):
        try:
            add_to_pilot_timing(job.jobid, PILOT_PRE_STAGEIN, time.time(), args)
            logger.debug("Prepare \'tmp\' dir in scratch ")
            if not os.path.exists(scratch_path + tmp_path):
                os.makedirs(scratch_path + tmp_path)
            logger.debug("Prepare dst and copy sqlite db files")
            t0 = time.time()
            if not os.path.exists(scratch_path + dst_db_path):
                os.makedirs(scratch_path + dst_db_path)
            shutil.copyfile(src_file, scratch_path + dst_db_path + dst_db_filename)
            logger.debug("")
            sql_cp_time = time.time() - t0
            logger.debug(f"Copy of sqlite files took: {sql_cp_time}")
            logger.debug("Prepare dst and copy geomDB files")
            t0 = time.time()
            if not os.path.exists(scratch_path + dst_db_path_2):
                os.makedirs(scratch_path + dst_db_path_2)
            shutil.copyfile(src_file_2, scratch_path + dst_db_path_2 + dst_db_filename_2)
            geomdb_cp_time = time.time() - t0
            logger.debug(f"copy of geomDB files took: {geomdb_cp_time} s")
            logger.debug("prepare job scratch dir")
            t0 = time.time()
            if not os.path.exists(job_scratch_dir):
                os.makedirs(job_scratch_dir)
            logger.debug("copy input file")
            for inp_file in job.input_files:
                shutil.copyfile(os.path.join(work_dir, inp_file),
                                os.path.join(job.input_files[inp_file]["scratch_path"], inp_file))
            input_cp_time = time.time() - t0
            logger.debug(f"copy of input files took: {input_cp_time} s")
        except IOError as exc:
            logger.error(f"i/o error({exc.errno}): {exc.strerror}")
            logger.error(f"copy to scratch failed, execution terminated': \n {sys.exc_info()[1]} ")
            raise FileHandlingFailure("Copy to RAM disk failed") from exc
        finally:
            add_to_pilot_timing(job.jobid, PILOT_POST_STAGEIN, time.time(), args)
    else:
        logger.info(f'Scratch directory ({scratch_path}) dos not exist')
        return work_dir

    os.chdir(job_scratch_dir)
    logger.debug(f"Current directory: {os.getcwd()}")
    true_dir = '/ccs/proj/csc108/AtlasReleases/21.0.15/nfs_db_files'
    pseudo_dir = "./poolcond"
    os.symlink(true_dir, pseudo_dir)

    return job_scratch_dir


def process_jobreport(payload_report_file: str, job_scratch_path: str, job_communication_point: str):
    """
    Copy job report file to make it accessible by Harvester. Shrink job report file.

    :param payload_report_file: name of job report (str)
    :param job_scratch_path: path to scratch directory (str)
    :param job_communication_point: path to updated job report accessible by Harvester (str)
    :raises FileHandlingFailure: in case of IOError.
    """
    src_file = os.path.join(job_scratch_path, payload_report_file)
    dst_file = os.path.join(job_communication_point, payload_report_file)

    try:
        logger.info(
            f"copy of payload report [{payload_report_file}] to access point: {job_communication_point}")
        # shrink jobReport
        job_report = read_json(src_file)
        if 'executor' in job_report:
            for executor in job_report['executor']:
                if 'logfileReport' in executor:
                    executor['logfileReport'] = {}

        write_json(dst_file, job_report)

    except IOError as exc:
        logger.error(f"job report copy failed, execution terminated':  \n {sys.exc_info()[1]} ")
        raise FileHandlingFailure("job report copy from RAM failed") from exc


def postprocess_workdir(workdir: str):
    """
    Post-processing of working directory. Unlink paths.

    :param workdir: path to directory to be processed (str)
    :raises FileHandlingFailure: in case of IOError.
    """
    pseudo_dir = "poolcond"
    try:
        if os.path.exists(pseudo_dir):
            remove(os.path.join(workdir, pseudo_dir))
    except IOError as exc:
        raise FileHandlingFailure("Post processing of working directory failed") from exc


def command_fix(command: str, job_scratch_dir: str) -> str:
    """
    Modification of payload parameters, to be executed on Titan on RAM disk. Some cleanup.

    :param command: payload command (str)
    :param job_scratch_dir: local path to input files (str)
    :return: updated/fixed payload command (str).
    """
    subs_a = command.split()
    for i, sub in enumerate(subs_a):
        if i > 0:
            if '(' in sub and not sub[0] == '"':
                subs_a[i] = '"' + sub + '"'
            if sub.startswith("--inputEVNTFile"):
                filename = sub.split("=")[1]
                subs_a[i] = sub.replace(filename, os.path.join(job_scratch_dir, filename))

    fixed_command = ' '.join(subs_a)
    fixed_command = fixed_command.strip()
    fixed_command = fixed_command.replace('--DBRelease="all:current"', '')  # avoid Frontier reading

    return fixed_command
