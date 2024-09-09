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
# - Mario Lassnig, mario.lassnig@cern.ch, 2016
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-24
# - Danila Oleynik danila.oleynik@cern.ch, 2018

import functools
import logging
import os
import signal
import time
from collections import namedtuple
from datetime import datetime
from functools import reduce
from types import FrameType

from pilot.common.exception import FileHandlingFailure
from pilot.util.auxiliary import set_pilot_state
from pilot.util.config import config
from pilot.util.constants import (
    FAILURE,
    PILOT_POST_FINAL_UPDATE,
    PILOT_POST_GETJOB,
    PILOT_POST_PAYLOAD,
    PILOT_POST_SETUP,
    PILOT_POST_STAGEOUT,
    PILOT_PRE_FINAL_UPDATE,
    PILOT_PRE_GETJOB,
    PILOT_PRE_SETUP,
    PILOT_PRE_PAYLOAD,
    PILOT_PRE_STAGEOUT,
    SUCCESS,
)
from pilot.util.container import execute
from pilot.util.filehandling import (
    tar_files,
    write_json,
    read_json,
    copy
)
from pilot.util.harvester import (
    get_initial_work_report,
    publish_work_report
)
from pilot.util.timing import add_to_pilot_timing

logger = logging.getLogger(__name__)
Traces = namedtuple("Traces", ["pilot"])


def interrupt(args: object, signum: int, frame: FrameType):
    """
    Interrupt function on the receiving end of kill signals.
    This function is forwarded any incoming signals (SIGINT, SIGTERM, etc) and will set abort_job which instructs
    the threads to abort the job.

    :param args: pilot arguments.
    :param signum: signal.
    :param frame: stack/execution frame pointing to the frame that was interrupted by the signal.
    """
    if frame:  # to bypass pylint score 0
        pass
    logger.info(
        "caught signal: %s",
        [v for v, k in list(signal.__dict__.items()) if k == signum][0],
    )
    args.graceful_stop.set()


def run(args: object) -> Traces or None:
    """
    Main execution function for the generic HPC workflow.

    :param args: pilot arguments (object)
    :returns: traces object (Traces or None).
    """
    # set communication point. Worker report should be placed there, matched with working directory of Harvester
    if args.harvester_workdir:
        communication_point = args.harvester_workdir
    else:
        communication_point = os.getcwd()
    work_report = get_initial_work_report()
    worker_attributes_file = config.Harvester.workerattributes_file
    worker_stageout_declaration = config.Harvester.stageoutn_file
    payload_report_file = config.Payload.jobreport
    payload_stdout_file = config.Payload.payloadstdout
    payload_stderr_file = config.Payload.payloadstderr

    try:
        logger.info("setting up signal handling")
        signal.signal(signal.SIGINT, functools.partial(interrupt, args))

        logger.info("setting up tracing")
        # Initialize traces with default values
        traces = Traces(pilot={"state": SUCCESS, "nr_jobs": 0, "error_code": 0, "command": None})

        if args.hpc_resource == "":
            logger.critical("hpc resource not specified, cannot continue")
            # Update traces using _replace for immutable update
            traces = traces._replace(pilot={"state": FAILURE,
                                            "nr_jobs": traces.pilot["nr_jobs"],
                                            "error_code": 0})
            return traces

        # get the resource reference
        resource = __import__(
            f"pilot.resource.{args.hpc_resource}",
            globals(),
            locals(),
            [args.hpc_resource],
            0,
        )

        # get the user reference
        user = __import__(
            f"pilot.user.{args.pilot_user.lower()}.common",
            globals(),
            locals(),
            [args.pilot_user.lower()],
            0,
        )

        # get job (and rank)
        add_to_pilot_timing("0", PILOT_PRE_GETJOB, time.time(), args)
        job, _ = resource.get_job(communication_point)  # replaced rank with _ since it is not used
        add_to_pilot_timing(job.jobid, PILOT_POST_GETJOB, time.time(), args)
        # cd to job working directory

        add_to_pilot_timing(job.jobid, PILOT_PRE_SETUP, time.time(), args)
        work_dir = resource.set_job_workdir(job, communication_point)
        work_report["workdir"] = work_dir
        worker_attributes_file = os.path.join(work_dir, worker_attributes_file)
        logger.debug(f"Worker attributes will be publeshied in: {worker_attributes_file}")

        set_pilot_state(job=job, state="starting")
        work_report["jobStatus"] = job.state
        publish_work_report(work_report, worker_attributes_file)

        # Get HPC specific setup commands
        logger.info(f"setup for resource {args.hpc_resource}: {resource.get_setup()}")
        setup_str = "; ".join(resource.get_setup())

        # Prepare job scratch directory (RAM disk etc.)
        job_scratch_dir = resource.set_scratch_workdir(job, work_dir, args)

        my_command = " ".join([job.script, job.script_parameters])
        my_command = resource.command_fix(my_command, job_scratch_dir)
        my_command = setup_str + my_command
        add_to_pilot_timing(job.jobid, PILOT_POST_SETUP, time.time(), args)

        # Basic execution. Should be replaced with something like 'run_payload'
        logger.debug(f"Going to launch: {my_command}")
        logger.debug(f"Current work directory: {job_scratch_dir}")
        with open(payload_stdout_file, "w", encoding="utf-8") as payloadstdout, \
                open(payload_stderr_file, "w", encoding="utf-8") as payloadstderr:

            add_to_pilot_timing(job.jobid, PILOT_PRE_PAYLOAD, time.time(), args)
            set_pilot_state(job=job, state="running")
            work_report["jobStatus"] = job.state
            work_report["startTime"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            start_time = time.asctime(time.localtime(time.time()))
            job.startTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            publish_work_report(work_report, worker_attributes_file)

            stime = time.time()
            t0 = os.times()
            exit_code, _, _ = execute(
                my_command, stdout=payloadstdout, stderr=payloadstderr, shell=True
            )
            logger.debug(f"Payload exit code: {exit_code}")
            t1 = os.times()
            exetime = time.time() - stime
            end_time = time.asctime(time.localtime(time.time()))
            t = [x - y for x, y in zip(t1, t0)]
            t_tot = reduce(lambda x, y: x + y, t[2:3])
            job.endTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        add_to_pilot_timing(job.jobid, PILOT_POST_PAYLOAD, time.time(), args)

        state = "finished" if exit_code == 0 else "failed"
        set_pilot_state(job=job, state=state)
        job.exitcode = exit_code

        work_report["startTime"] = job.startTime
        work_report["endTime"] = job.endTime
        work_report["jobStatus"] = job.state
        work_report["cpuConsumptionTime"] = t_tot
        work_report["transExitCode"] = job.exitcode

        log_jobreport = f"\nPayload exit code: {exit_code} JobID: {job.jobid} \n"
        log_jobreport += f"CPU comsumption time: {t_tot}  JobID: {job.jobid} \n"
        log_jobreport += f"Start time: {start_time}  JobID: {job.jobid} \n"
        log_jobreport += f"End time: {end_time}  JobID: {job.jobid} \n"
        log_jobreport += f"Execution time: {exetime} sec.  JobID: {job.jobid} \n"
        logger.info(log_jobreport)
        log_jobreport = f"\nJob report start time: {job.startTime}\nJob report end time: {job.endTime}"
        logger.debug(log_jobreport)

        # Parse job report file and update of work report
        if os.path.exists(payload_report_file):
            payload_report = user.parse_jobreport_data(read_json(payload_report_file))
            work_report.update(payload_report)
            resource.process_jobreport(payload_report_file, job_scratch_dir, work_dir)

        resource.postprocess_workdir(job_scratch_dir)

        # output files should not be packed with logs
        protectedfiles = list(job.output_files.keys())

        # log file not produced (yet), so should be excluded
        if job.log_file in protectedfiles:
            protectedfiles.remove(job.log_file)
        else:
            logger.info("Log files was not declared")

        logger.info("Cleanup of working directory")

        protectedfiles.extend([worker_attributes_file, worker_stageout_declaration])
        user.remove_redundant_files(job_scratch_dir, protectedfiles)
        res = tar_files(job_scratch_dir, protectedfiles, job.log_file)
        if res > 0:
            raise FileHandlingFailure("Log file tar failed")

        add_to_pilot_timing(job.jobid, PILOT_PRE_STAGEOUT, time.time(), args)
        # Copy of output to shared FS for stageout
        if not job_scratch_dir == work_dir:
            copy_output(job, job_scratch_dir, work_dir)
        add_to_pilot_timing(job.jobid, PILOT_POST_STAGEOUT, time.time(), args)

        logger.info("Declare stage-out")
        add_to_pilot_timing(job.jobid, PILOT_PRE_FINAL_UPDATE, time.time(), args)
        declare_output(job, work_report, worker_stageout_declaration)

        logger.info("All done")
        publish_work_report(work_report, worker_attributes_file)
        logger.debug(f"Final report: {work_report}")
        add_to_pilot_timing(job.jobid, PILOT_POST_FINAL_UPDATE, time.time(), args)

    except Exception as error:
        work_report["jobStatus"] = "failed"
        work_report["exitMsg"] = str(error)
        publish_work_report(work_report, worker_attributes_file)
        logging.exception(f"exception caught: {error}")
        # Update traces using _replace for immutable update
        traces = traces._replace(pilot={"state": FAILURE,
                                        "nr_jobs": traces.pilot["nr_jobs"],
                                        "error_code": 0})

    return traces


def copy_output(job: object, job_scratch_dir: str, work_dir: str) -> int:
    """
    Copy output files from scratch directory to access point.

    :param job: job object (object)
    :param job_scratch_dir: job scratch directory (str)
    :param work_dir: work directory (str)
    :return: 0 if successful (int).
    """
    cp_start = time.time()
    try:
        for outfile in list(job.output_files.keys()):
            if os.path.exists(outfile):
                copy(
                    os.path.join(job_scratch_dir, outfile),
                    os.path.join(work_dir, outfile),
                )
        os.chdir(work_dir)
    except IOError as e:
        raise FileHandlingFailure("Copy from scratch dir to access point failed") from e
    finally:
        cp_time = time.time() - cp_start
        logger.info(f"Copy of outputs took: {cp_time} sec")

    return 0


def declare_output(job: object, work_report: dict, worker_stageout_declaration: str):
    """
    Declare output files for stage-out.

    :param job: job object (object)
    :param work_report: work report (dict)
    :param worker_stageout_declaration: worker stageout declaration (str).
    """
    out_file_report = {}
    out_file_report[job.jobid] = []
    for outfile in list(job.output_files.keys()):
        logger.debug(f"File {outfile} will be checked and declared for stage out")
        if os.path.exists(outfile):
            file_desc = {}
            if outfile == job.log_file:
                file_desc["filetype"] = "log"
            else:
                file_desc["filetype"] = "output"
            file_desc["path"] = os.path.abspath(outfile)
            file_desc["fsize"] = os.path.getsize(outfile)
            if "guid" in list(job.output_files[outfile].keys()):
                file_desc["guid"] = job.output_files[outfile]["guid"]
            elif work_report["outputfiles"] and work_report["outputfiles"][outfile]:
                file_desc["guid"] = work_report["outputfiles"][outfile]["guid"]
            out_file_report[job.jobid].append(file_desc)
        else:
            logger.info(f"Expected output file {outfile} missed. Job {job.jobid} will be failed")
            set_pilot_state(job=job, state="failed")

    if out_file_report[job.jobid]:
        write_json(worker_stageout_declaration, out_file_report)
        logger.debug(f"Stagout declared in: {worker_stageout_declaration}")
        logger.debug(f"Report for stageout: {out_file_report}")
