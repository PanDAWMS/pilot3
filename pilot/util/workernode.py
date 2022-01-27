#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2022

import os
import re
import logging

from subprocess import getoutput

from pilot.common.exception import PilotException, ErrorCodes
from pilot.info import infosys
from pilot.util.disk import disk_usage

logger = logging.getLogger(__name__)


def get_local_disk_space(path):
    """
    Return remaining disk space for the disk in the given path.
    Unit is MB.

    :param path: path to disk (string). Can be None, if call to collect_workernode_info() doesn't specify it.
    :return: disk space (float).
    :raises: PilotException in case of failure to convert df output to float, or if getoutput() returns an empty string.
    """

    # -mP = blocks of 1024*1024 (MB) and POSIX format
    cmd = f"df -mP {path}"
    disks = getoutput(cmd)
    if disks:
        logger.debug(f'disks={disks}')
        try:
            disk = float(disks.splitlines()[1].split()[3])
        except (IndexError, ValueError, TypeError, AttributeError) as error:
            msg = f'exception caught while trying to convert disk info: {error}'
            logger.warning(msg)
            raise PilotException(msg, code=ErrorCodes.UNKNOWNEXCEPTION)
    else:
        msg = f'no stdout+stderr from command: {cmd}'
        logger.warning(msg)
        raise PilotException(msg, code=ErrorCodes.UNKNOWNEXCEPTION)

    return disk


def get_meminfo():
    """
    Return the total memory (in MB).

    :return: memory (float).
    """

    mem = 0.0
    with open("/proc/meminfo", "r") as _fd:
        mems = _fd.readline()
        while mems:
            if mems.upper().find("MEMTOTAL") != -1:
                try:
                    mem = float(mems.split()[1]) / 1024  # value listed by command as kB, convert to MB
                except ValueError as error:
                    logger.warning(f'exception caught while trying to convert meminfo: {error}')
                break
            mems = _fd.readline()

    return mem


def get_cpuinfo():
    """
    Return the CPU frequency (in MHz).

    :return: cpu (float).
    """

    cpu = 0.0
    with open("/proc/cpuinfo", "r") as _fd:
        lines = _fd.readlines()
        for line in lines:
            if line.find("cpu MHz") != -1:  # Python 2/3
                try:
                    cpu = float(line.split(":")[1])
                except ValueError as error:
                    logger.warning(f'exception caught while trying to convert cpuinfo: {error}')
                break  # command info is the same for all cores, so break here

    return cpu


def collect_workernode_info(path=None):
    """
    Collect node information (cpu, memory and disk space).
    The disk space (in MB) is return for the disk in the given path.

    :param path: path to disk (string).
    :return: memory (float), cpu (float), disk space (float).
    """

    mem = get_meminfo()
    cpu = get_cpuinfo()
    try:
        disk = get_local_disk_space(path)
    except PilotException as exc:
        diagnostics = exc.get_detail()
        logger.warning(f'exception caught while executing df: {diagnostics} (ignoring)')
        disk = None

    return mem, cpu, disk


def get_disk_space(queuedata):
    """
    Get the disk space from the queuedata that should be available for running the job;
    either what is actually locally available or the allowed size determined by the site (value from queuedata). This
    value is only to be used internally by the job dispatcher.

    :param queuedata: infosys object.
    :return: disk space that should be available for running the job (int).
    """

    # --- non Job related queue data
    # jobinfo provider is required to consider overwriteAGIS data coming from Job
    _maxinputsize = infosys.queuedata.maxwdir
    logger.debug(f'resolved value from global infosys.queuedata instance: infosys.queuedata.maxwdir={_maxinputsize} B')
    _maxinputsize = queuedata.maxwdir
    logger.debug(f'resolved value: queuedata.maxwdir={_maxinputsize} B')

    try:
        _du = disk_usage(os.path.abspath("."))
        _diskspace = int(_du[2] / (1024 * 1024))  # need to convert from B to MB
    except ValueError as error:
        logger.warning(f"failed to extract disk space: {error} (will use schedconfig default)")
        _diskspace = _maxinputsize
    else:
        logger.info(f'available WN disk space: {_diskspace} MB')

    _diskspace = min(_diskspace, _maxinputsize)
    logger.info(f'sending disk space {_diskspace} MB to dispatcher')

    return _diskspace


def get_node_name():
    """
    Return the local node name.

    :return: node name (string)
    """
    if 'PANDA_HOSTNAME' in os.environ:
        host = os.environ.get('PANDA_HOSTNAME')
    elif hasattr(os, 'uname'):
        host = os.uname()[1]
    else:
        import socket
        host = socket.gethostname()

    return get_condor_node_name(host)


def get_condor_node_name(nodename):
    """
    On a condor system, add the SlotID to the nodename

    :param nodename:
    :return:
    """

    if "_CONDOR_SLOT" in os.environ:
        nodename = "%s@%s" % (os.environ.get("_CONDOR_SLOT"), nodename)

    return nodename


def get_cpu_model():
    """
    Get cpu model and cache size from /proc/cpuinfo.

    Example.
      model name      : Intel(R) Xeon(TM) CPU 2.40GHz
      cache size      : 512 KB

    gives the return string "Intel(R) Xeon(TM) CPU 2.40GHz 512 KB".

    :return: cpu model (string).
    """

    cpumodel = ""
    cpucache = ""
    modelstring = ""

    re_model = re.compile(r'^model name\s+:\s+(\w.+)')  # Python 3 (added r)
    re_cache = re.compile(r'^cache size\s+:\s+(\d+ KB)')  # Python 3 (added r)

    with open("/proc/cpuinfo", "r") as _fp:

        # loop over all lines in cpuinfo
        for line in _fp.readlines():
            # try to grab cpumodel from current line
            model = re_model.search(line)
            if model:
                # found cpu model
                cpumodel = model.group(1)

            # try to grab cache size from current line
            cache = re_cache.search(line)
            if cache:
                # found cache size
                cpucache = cache.group(1)

            # stop after 1st pair found - can be multiple cpus
            if cpumodel and cpucache:
                # create return string
                modelstring = cpumodel + " " + cpucache
                break

    # default return string if no info was found
    if not modelstring:
        modelstring = "UNKNOWN"

    return modelstring


def check_hz():
    """
    Try to read the SC_CLK_TCK and write it to the log.

    :return:
    """

    try:
        _ = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
    except Exception:
        import traceback
        logger.fatal('failed to read SC_CLK_TCK - will not be able to perform CPU consumption calculation')
        logger.warning(traceback.format_exc())
