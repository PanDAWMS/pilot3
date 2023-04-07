#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2023

import os
from signal import SIGTERM, SIGKILL
from time import sleep

import logging
logger = logging.getLogger(__name__)


def kill_process_group(pgrp):
    """
    Kill the process group.
    DO NOT MOVE TO PROCESSES.PY - will lead to circular import since execute() needs it as well.
    :param pgrp: process group id (int).
    :return: boolean (True if SIGTERM followed by SIGKILL signalling was successful)
    """

    status = False
    _sleep = True

    # kill the process gracefully
    logger.info(f"killing group process {pgrp}")
    try:
        os.killpg(pgrp, SIGTERM)
    except Exception as error:
        logger.warning(f"exception thrown when killing child group process under SIGTERM: {error}")
        _sleep = False
    else:
        logger.info(f"SIGTERM sent to process group {pgrp}")

    if _sleep:
        nap = 30
        logger.info(f"sleeping {nap} s to allow processes to exit")
        sleep(nap)

    try:
        os.killpg(pgrp, SIGKILL)
    except Exception as error:
        logger.warning(f"exception thrown when killing child group process with SIGKILL: {error}")
    else:
        logger.info(f"SIGKILL sent to process group {pgrp}")
        status = True

    return status
