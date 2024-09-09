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

import functools
import logging
import signal

from collections import namedtuple
from os import environ
from types import FrameType

from pilot.util.constants import SUCCESS, FAILURE

logger = logging.getLogger(__name__)
# Define Traces namedtuple at the module level
Traces = namedtuple("Traces", ["pilot"])


def interrupt(args: object, signum: int, frame: FrameType):
    """
    Handle signals for graceful exit.

    :param args: pilot arguments (object)
    :param signum: signal number (int)
    :param frame: signal frame (object)
    """
    if frame:  # to bypass pylint score 0
        pass

    tmp = [v for v, k in list(signal.__dict__.items()) if k == signum]
    logger.info(
        f"caught signal: {tmp[0]}"
    )
    args.graceful_stop.set()


def run(args: object) -> Traces or None:
    """
    Run the event service workflow on HPCs (Yoda-Droid).

    :param args: pilot arguments (object)
    :returns: traces object (Traces namedtuple)
    """
    traces = None
    try:
        logger.info("setting up signal handling")
        signal.signal(signal.SIGINT, functools.partial(interrupt, args))

        logger.info("setting up tracing")

        # Initialize traces with default values
        traces = Traces(pilot={"state": SUCCESS, "nr_jobs": 0})

        if args.hpc_resource == "":
            logger.critical("hpc resource not specified, cannot continue")
            # properly update the traces object (to prevent pylint error)
            traces = traces._replace(pilot={"state": FAILURE, "nr_jobs": traces.pilot["nr_jobs"]})
            return traces

        # get the resource reference
        resource = __import__(
            f"pilot.resource.{args.hpc_resource}",
            globals(),
            locals(),
            [args.hpc_resource],
            0,
        )

        # example usage:
        logger.info(f"setup for resource {args.hpc_resource}: {resource.get_setup()}")

        # are we Yoda or Droid?
        if environ.get("SOME_ENV_VARIABLE", "") == "YODA":
            yodadroid = __import__("pilot.eventservice.yoda")
        else:
            yodadroid = __import__("pilot.eventservice.droid")
        yodadroid.run()

    except Exception as e:
        logger.fatal(f"exception caught: {e}")

    return traces
