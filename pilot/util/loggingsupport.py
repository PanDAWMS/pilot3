#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2023

# This module contains functions related to logging.

import logging

logger = logging.getLogger(__name__)


def flush_handler(name=""):
    """
    Flush the stdout buffer for the given handler.
    Useful e.g. in case of time-out exceptions.

    :param name: name of handler (string)
    :return:
    """

    if not name:
        return
    for handler in logging.getLogger().handlers:
        if handler.name == name:
            handler.flush()  # make sure that stdout buffer gets flushed
