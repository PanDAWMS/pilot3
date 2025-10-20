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
# - Paul Nilsson, paul.nilsson@cern.ch, 2025

"""Utility functions for HTCondor interaction."""

import logging
import os
import re
import socket
import subprocess
from shutil import which

from pilot.util.container import execute

logger = logging.getLogger(__name__)


def find_condor_chirp() -> str:
    """
    Find the full path to condor_chirp using condor_config_val.

    Returns:
        str: Full path to condor_chirp if found, otherwise an error message.
    """
    path = which("condor_chirp")
    if path:
        return path
    logger.warning(f'condor_chirp not found in standard $PATH={os.environ["PATH"]}')
    path = os.path.join('/usr/bin', 'condor_chirp')
    if os.path.isfile(path):
        return path
    logger.warning('condor_chirp not found in /usr/bin - trying condor_config_val to locate it}')
    path = os.path.join('/usr/bin', 'condor_config_val')
    if not os.path.isfile(path):
        logger.warning(f'condor_config_val not found in {path} - cannot locate condor_chirp')
        return "Error: condor_chirp not found"

    try:
        # Run condor_config_val to get the LIBEXEC path
        result = subprocess.run(
            ["condor_config_val", "-quiet", "LIBEXEC"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        libexec_path = result.stdout.strip()

        # Construct full path to condor_chirp
        chirp_path = os.path.join(libexec_path, "condor_chirp")

        # Verify it actually exists
        if os.path.isfile(chirp_path):
            return chirp_path
        else:
            return f"Error: condor_chirp not found in {libexec_path}"
    except subprocess.CalledProcessError:
        return "Error: condor_config_val command failed or HTCondor not installed."
    except FileNotFoundError:
        return "Error: condor_config_val not found in PATH."


def update_condor_classad(pandaid: int = 0, state: str = '') -> bool:
    """
    Update the Condor ClassAd with PanDA information using condor_chirp.

    Params:
        pandaid: PanDA job id (int).
        state: current job state (string).

    Returns:
        bool: True if condor_chirp is available and was used, False otherwise.
    """
    logger.debug('updating condor ClassAd with PanDA job id')
    path = find_condor_chirp()
    if not path.startswith("/"):
        logger.warning(path)
        return False

    # update the ClassAd
    if pandaid:
        cmd = f'{path} set_job_attr PandaID "{pandaid}"'
        ec, stdout, stderr = execute(cmd)
        if ec:
            logger.warning(f'failed to set attribute PandaID={pandaid} for job ClassAd')
            logger.debug(stdout)
            logger.debug(stderr)
            return False
    if state:
        cmd = f'{path} set_job_attr PandaJobState "{state}"'
        ec, stdout, stderr = execute(cmd)
        if ec:
            logger.warning(f'failed to set attribute PandaJobState={state} for job ClassAd')
            logger.debug(stdout)
            logger.debug(stderr)
            return False

    logger.debug('successfully updated job ClassAd')
    return True


def get_globaljobid() -> str:
    """
    Return the GlobalJobId value from the Condor ClassAd.

    :return: GlobalJobId value (str).
    """
    ret = ""
    with open(os.environ.get("_CONDOR_JOB_AD"), 'r', encoding='utf-8') as _fp:
        for line in _fp:
            res = re.search(r'^GlobalJobId\s*=\s*"(.*)"', line)
            if res is None:
                continue
            try:
                ret = res.group(1)
            except IndexError as exc:
                logger.warning(f'failed to interpret GlobalJobId: {exc}')
            break

    return ret


def encode_globaljobid(jobid: str, maxsize: int = 31) -> str:
    """
    Encode the global job id on HTCondor.

    To be used as an environmental variable on HTCondor nodes to facilitate debugging.

    Format: <PanDA id>:<Processing type>:<cluster ID>.<process ID>_<schedd name code>

    NEW FORMAT: WN hostname, process and user id

    Note: due to batch system restrictions, this string is limited to 31 (maxsize) characters, using the least significant
    characters (i.e. the left part of the string might get cut). Also, the cluster ID and process IDs are converted to hex
    to limit the sizes. The schedd host name is further encoded using the last digit in the host name (spce03.sdcc.bnl.gov -> spce03 -> 3).

    :param jobid: panda job id (str)
    :param maxsize: max length allowed (int)
    :return: encoded global job id (str).
    """
    def get_host_name():
        # spool1462.sdcc.bnl.gov -> spool1462
        if 'PANDA_HOSTNAME' in os.environ:
            host = os.environ.get('PANDA_HOSTNAME')
        elif hasattr(os, 'uname'):
            host = os.uname()[1]
        else:
            try:
                host = socket.gethostname()
            except socket.herror as e:
                logger.warning(f'failed to get host name: {e}')
                host = 'localhost'
        return host.split('.')[0]

    globaljobid = get_globaljobid()
    if not globaljobid:
        return ""

    try:
        _globaljobid = globaljobid.split('#')
        # host = _globaljobid[0]
        tmp = _globaljobid[1].split('.')
        # timestamp = _globaljobid[2] - ignore this one
        # clusterid = tmp[0]
        processid = tmp[1]
    except IndexError as exc:
        logger.warning(exc)
        return ""

    host_name = get_host_name()
    if processid and host_name:
        global_name = f'{host_name}_{processid}_{jobid}'
    else:
        global_name = ''

    if len(global_name) > maxsize:
        logger.warning(f'HTCondor: global name is exceeding maxsize({maxsize}), will be truncated: {global_name}')
        global_name = global_name[-maxsize:]
        logger.debug(f'HTCondor: final global name={global_name}')
    else:
        logger.debug(f'HTCondor: global name is within limits: {global_name} (length={len(global_name)}, max size={maxsize})')

    return global_name


def get_condor_node_name(nodename):
    """
    On a condor system, add the SlotID to the nodename

    :param nodename:
    :return:
    """

    if "_CONDOR_SLOT" in os.environ:
        nodename = "%s@%s" % (os.environ.get("_CONDOR_SLOT"), nodename)

    return nodename
