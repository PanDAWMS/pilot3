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
from pathlib import Path
from shutil import which
from typing import Dict, Any

from pilot.util.container import execute

logger = logging.getLogger(__name__)

COMMON_CONDOR_BINS = [
    "/usr/bin/condor_config_val",
    "/usr/sbin/condor_config_val",
    "/opt/condor/bin/condor_config_val",
    "/opt/condor/sbin/condor_config_val",
]
COMMON_LIBEXEC_DIRS = [
    "/usr/libexec/condor",
    "/usr/lib/condor",
    "/opt/condor/libexec",
]


def update_condor_classad(pandaid: int = 0, pilotid: str = '') -> bool:
    """
    Update the Condor ClassAd with PanDA information using condor_chirp.

    Params:
        pandaid: PanDA job id (int).
        pilotid: Pilot id (str).

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
    if pilotid:
        cmd = f'{path} set_job_attr PandaPilotId "{pilotid}"'
        ec, stdout, stderr = execute(cmd)
        if ec:
            logger.warning(f'failed to set attribute PandaPilotId={pilotid} for job ClassAd')
            logger.debug(stdout)
            logger.debug(stderr)
            return False

    logger.debug('successfully updated job ClassAd')
    return True


def update_condor_classad_bulk(attrs: Dict[str, Any]) -> bool:
    """
    Write multiple key/value pairs to the Condor ClassAd using `condor_chirp`.

    Each key in *attrs* is written as a job attribute via:
    `condor_chirp set_job_attr <key> "<value>"`. Values are converted to strings
    and internal double quotes are escaped.

    Args:
        attrs (Dict[str, Any]): Mapping of attribute names to values to set.

    Returns:
        bool: True if `condor_chirp` was found and all attributes were set
            successfully; False otherwise.
    """
    logger.debug('updating condor ClassAd with multiple attributes')
    path = find_condor_chirp()
    if not path.startswith("/"):
        logger.warning(path)
        return False

    if not attrs:
        logger.debug('no attributes provided to update_condor_classad_bulk')
        return False

    for key, value in attrs.items():
        if not key:
            logger.debug('skipping empty attribute name')
            continue
        # Convert value to string and escape double quotes
        val_str = str(value).replace('"', '\\"')
        cmd = f'{path} set_job_attr {key} "{val_str}"'
        ec, stdout, stderr = execute(cmd)
        if ec:
            logger.warning(f'failed to set attribute {key}={value} for job ClassAd')
            logger.debug(stdout)
            logger.debug(stderr)
            return False

    logger.debug('successfully updated job ClassAd with provided attributes')
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


def _expand_macros(v, defs):
    """
    Expand macros of the form $(KEY) in the string v using the definitions in defs.

    Args:
        v (str): The string containing macros to expand.
        defs (Dict[str, str]): A dictionary of key-value pairs for macro expansion.

    Returns:
        str: The string with macros expanded.
    """
    return re.sub(r"\$\(([^)]+)\)", lambda m: defs.get(m.group(1), m.group(0)), v)


def _parse_condor_config(path):
    """
    Parse a Condor configuration file and return a dictionary of key-value pairs.

    Args:
        path (str): Path to the Condor configuration file.

    Returns:
        Dict[str, str]: A dictionary of configuration key-value pairs.
    """
    defs = {}
    try:
        for raw in Path(path).read_text().splitlines():
            line = raw.split("#", 1)[0].strip()
            if not line or "=" not in line:
                continue
            k, v = [x.strip() for x in line.split("=", 1)]
            v = _expand_macros(v, defs)
            defs[k] = v
    except Exception:
        pass
    return defs


def find_condor_chirp():
    """
    Locate the condor_chirp executable.

    Returns:
        str: The path to condor_chirp if found, otherwise an informative error message.
    """
    # 0) If caller already hints the libexec dir, use it.
    if "_CONDOR_LIBEXEC" in os.environ:
        p = Path(os.environ["_CONDOR_LIBEXEC"]) / "condor_chirp"
        if p.is_file() and os.access(p, os.X_OK):
            return str(p)

    # 1) Try condor_config_val (PATH or common locations)
    exe = which("condor_config_val")
    if not exe:
        for p in COMMON_CONDOR_BINS:
            if Path(p).is_file() and os.access(p, os.X_OK):
                exe = p
                break

    condor_cfg_err = None
    if exe:
        # Helpful diagnostic: where configs are expected
        _ = subprocess.run([exe, "-config"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        proc = subprocess.run([exe, "-quiet", "LIBEXEC"],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if proc.returncode == 0:
            libexec = proc.stdout.strip()
            p = Path(libexec) / "condor_chirp"
            if p.is_file() and os.access(p, os.X_OK):
                return str(p)
            else:
                condor_cfg_err = f"LIBEXEC reported as '{libexec}', but condor_chirp not found there."
        else:
            condor_cfg_err = proc.stderr.strip() or "condor_config_val exited with code 1 without stderr."

    # 2) If a specific config is pointed to, parse it for LIBEXEC
    cfg = os.environ.get("CONDOR_CONFIG")
    if cfg and Path(cfg).is_file():
        defs = _parse_condor_config(cfg)
        libexec = defs.get("LIBEXEC")
        if libexec:
            p = Path(libexec) / "condor_chirp"
            if p.is_file() and os.access(p, os.X_OK):
                return str(p)

    # 3) Directly in PATH?
    cp = which("condor_chirp")
    if cp:
        return cp

    # 4) Common install dirs
    for d in COMMON_LIBEXEC_DIRS:
        p = Path(d) / "condor_chirp"
        if p.is_file() and os.access(p, os.X_OK):
            return str(p)

    # Final, informative error
    tips = [
        "If you're on a worker node/container, HTCondor may not expose configs to your env.",
        "Try: export CONDOR_CONFIG=/etc/condor/condor_config  (or your site's path).",
        "Or set: export _CONDOR_LIBEXEC=/usr/libexec/condor   (or /opt/condor/libexec).",
        "Verify tool: which condor_config_val; condor_config_val -config",
    ]
    detail = f"condor_config_val diagnostics: {condor_cfg_err}" if condor_cfg_err else "condor_config_val not found."
    return "condor_chirp not found. " + detail + " | " + " ".join(tips)
