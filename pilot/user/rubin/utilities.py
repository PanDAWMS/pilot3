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
# - Paul Nilsson, paul.nilsson@cern.ch, 2020-25

import logging
import os
import time

from pilot.common.exception import (
    NoSuchFile,
    FileHandlingFailure
)
from pilot.info import JobData
from pilot.util.filehandling import (
    read_json,
    copy, write_json
)
from pilot.util.parameters import convert_to_int

logger = logging.getLogger(__name__)


def get_memory_monitor_summary_filename(selector: bool = None) -> str:
    """
    Return the name for the memory monitor summary file.

    :param selector: special conditions flag (bool)
    :return: File name (str).
    """
    name = "memory_monitor_summary.json"
    if selector:
        name += '_snapshot'

    return name


def get_memory_monitor_output_filename(suffix: str = 'txt') -> str:
    """
    Return the filename of the memory monitor text output file.

    :return: File name (str).
    """
    return f"memory_monitor_output.{suffix}"


def get_memory_monitor_info_path(workdir: str, allowtxtfile: bool = False) -> str:
    """
    Find the proper path to the utility info file.

    Priority order:
       1. JSON summary file from workdir
       2. JSON summary file from pilot initdir
       3. Text output file from workdir (if allowtxtfile is True)

    :param workdir: relevant work directory (str)
    :param allowtxtfile: boolean attribute to allow for reading the raw memory monitor output (bool)
    :return: path (str).
    """
    pilot_initdir = os.environ.get('PILOT_HOME', '')
    path = os.path.join(workdir, get_memory_monitor_summary_filename())
    init_path = os.path.join(pilot_initdir, get_memory_monitor_summary_filename())

    if not os.path.exists(path):
        if os.path.exists(init_path):
            path = init_path
        else:
            logger.info(f"neither {path}, nor {init_path} exist")
            path = ""

        if path == "" and allowtxtfile:
            path = os.path.join(workdir, get_memory_monitor_output_filename())
            if not os.path.exists(path):
                logger.warning(f"file does not exist either: {path}")

    return path


def get_memory_monitor_info(workdir: str, allowtxtfile: bool = False, name: str = "") -> dict:  # noqa: C901
    """
    Add the utility info to the node structure if available.

    :param workdir: relevant work directory (str)
    :param allowtxtfile: boolean attribute to allow for reading the raw memory monitor output (bool)
    :param name: name of memory monitor (str)
    :return: node structure (dict).
    """
    if not allowtxtfile:  # to bypass pylint complaint
        pass
    node = {}

    # Get the values from the memory monitor file (json if it exists, otherwise the preliminary txt file)
    # Note that only the final json file will contain the totRBYTES, etc
    try:
        summary_dictionary = get_memory_values(workdir, name=name)
    except Exception as exc:
        logger.warning(f'failed to get memory values from memory monitor tool: {exc}')
        summary_dictionary = {}
    else:
        logger.debug(f"summary_dictionary={str(summary_dictionary)}")

    # Fill the node dictionary
    if summary_dictionary and summary_dictionary != {}:
        # first determine which memory monitor version was running (MemoryMonitor or prmon)
        if 'maxRSS' in summary_dictionary['Max']:
            version = 'MemoryMonitor'
        elif 'rss' in summary_dictionary['Max']:
            version = 'prmon'
        else:
            version = 'unknown'
        if version == 'MemoryMonitor':
            try:
                node['maxRSS'] = summary_dictionary['Max']['maxRSS']
                node['maxVMEM'] = summary_dictionary['Max']['maxVMEM']
                node['maxSWAP'] = summary_dictionary['Max']['maxSwap']
                node['maxPSS'] = summary_dictionary['Max']['maxPSS']
                node['avgRSS'] = summary_dictionary['Avg']['avgRSS']
                node['avgVMEM'] = summary_dictionary['Avg']['avgVMEM']
                node['avgSWAP'] = summary_dictionary['Avg']['avgSwap']
                node['avgPSS'] = summary_dictionary['Avg']['avgPSS']
            except KeyError as exc:
                logger.warning(f"exception caught while parsing memory monitor file: {exc}")
                logger.warning("will add -1 values for the memory info")
                node['maxRSS'] = -1
                node['maxVMEM'] = -1
                node['maxSWAP'] = -1
                node['maxPSS'] = -1
                node['avgRSS'] = -1
                node['avgVMEM'] = -1
                node['avgSWAP'] = -1
                node['avgPSS'] = -1
            else:
                logger.info("extracted standard info from memory monitor json")
            try:
                node['totRCHAR'] = summary_dictionary['Max']['totRCHAR']
                node['totWCHAR'] = summary_dictionary['Max']['totWCHAR']
                node['totRBYTES'] = summary_dictionary['Max']['totRBYTES']
                node['totWBYTES'] = summary_dictionary['Max']['totWBYTES']
                node['rateRCHAR'] = summary_dictionary['Avg']['rateRCHAR']
                node['rateWCHAR'] = summary_dictionary['Avg']['rateWCHAR']
                node['rateRBYTES'] = summary_dictionary['Avg']['rateRBYTES']
                node['rateWBYTES'] = summary_dictionary['Avg']['rateWBYTES']
            except KeyError:
                logger.warning("standard memory fields were not found in memory monitor json (or json doesn't exist yet)")
            else:
                logger.info("extracted standard memory fields from memory monitor json")
        elif version == 'prmon':
            try:
                node['maxRSS'] = int(summary_dictionary['Max']['rss'])
                node['maxVMEM'] = int(summary_dictionary['Max']['vmem'])
                node['maxSWAP'] = int(summary_dictionary['Max']['swap'])
                node['maxPSS'] = int(summary_dictionary['Max']['pss'])
                node['avgRSS'] = summary_dictionary['Avg']['rss']
                node['avgVMEM'] = summary_dictionary['Avg']['vmem']
                node['avgSWAP'] = summary_dictionary['Avg']['swap']
                node['avgPSS'] = summary_dictionary['Avg']['pss']
            except KeyError as exc:
                logger.warning(f"exception caught while parsing prmon file: {exc}")
                logger.warning("will add -1 values for the memory info")
                node['maxRSS'] = -1
                node['maxVMEM'] = -1
                node['maxSWAP'] = -1
                node['maxPSS'] = -1
                node['avgRSS'] = -1
                node['avgVMEM'] = -1
                node['avgSWAP'] = -1
                node['avgPSS'] = -1
            else:
                logger.info("extracted standard info from prmon json")
            try:
                node['totRCHAR'] = int(summary_dictionary['Max']['rchar'])
                node['totWCHAR'] = int(summary_dictionary['Max']['wchar'])
                node['totRBYTES'] = int(summary_dictionary['Max']['read_bytes'])
                node['totWBYTES'] = int(summary_dictionary['Max']['write_bytes'])
                node['rateRCHAR'] = summary_dictionary['Avg']['rchar']
                node['rateWCHAR'] = summary_dictionary['Avg']['wchar']
                node['rateRBYTES'] = summary_dictionary['Avg']['read_bytes']
                node['rateWBYTES'] = summary_dictionary['Avg']['write_bytes']
            except KeyError:
                logger.warning("standard memory fields were not found in prmon json (or json doesn't exist yet)")
            else:
                logger.info("extracted standard memory fields from prmon json")
        else:
            logger.warning('unknown memory monitor version')
    else:
        logger.info("memory summary dictionary not yet available")

    return node


def get_max_memory_monitor_value(value: int, maxvalue: int, totalvalue: int) -> tuple[int, int, int]:  # noqa: C90
    """
    Return the max and total value (used by memory monitoring).

    Return an error code, 1, in case of value error.

    :param value: value to be tested (int)
    :param maxvalue: current maximum value (int)
    :param totalvalue: total value (int)
    :return: exit code, maximum and total value (tuple).
    """
    ec = 0
    try:
        value_int = int(value)
    except (ValueError, TypeError) as exc:
        logger.warning(f"exception caught: {exc}")
        ec = 1
    else:
        totalvalue += value_int
        maxvalue = max(maxvalue, value_int)

    return ec, maxvalue, totalvalue


def convert_unicode_string(unicode_string: str) -> str or None:
    """
    Convert a unicode string into str.

    :param unicode_string: unicode string (str)
    :return: string or None (str or None).
    """
    if unicode_string is not None:
        return str(unicode_string)

    return None


def get_average_summary_dictionary_prmon(path: str) -> dict:
    """
    Loop over the memory monitor output file and create the averaged summary dictionary.

    prmon keys:
    'Time', 'nprocs', 'nthreads', 'pss', 'rchar', 'read_bytes', 'rss', 'rx_bytes',
    'rx_packets', 'stime', 'swap', 'tx_bytes', 'tx_packets', 'utime', 'vmem', 'wchar',
    'write_bytes', 'wtime'

    The function uses the first line in the output file to define the dictionary keys used
    later in the function. This means that any change in the format such as new columns
    will be handled automatically.

    :param path: path to memory monitor txt output file (str)
    :return: summary dictionary (dict).
    """
    summary_dictionary = {}

    # get the raw memory monitor output, convert to dictionary
    dictionary = convert_text_file_to_dictionary(path)

    if dictionary:
        # Calculate averages and store all values
        summary_dictionary = {"Max": {}, "Avg": {}, "Other": {}, "Time": {}}

        def filter_value(value: str or None) -> bool:
            """ Inline function used to remove any string or None values from data. """
            if isinstance(value, str) or value is None:
                return False

            return True

        keys = ['vmem', 'pss', 'rss', 'swap']
        values = {}
        for key in keys:
            value_list = list(filter(filter_value, dictionary.get(key, 0)))
            n = len(value_list)
            average = int(float(sum(value_list)) / float(n)) if n > 0 else 0
            maximum = max(value_list)
            values[key] = {'avg': average, 'max': maximum}

        summary_dictionary["Max"] = {"maxVMEM": values['vmem'].get('max'), "maxPSS": values['pss'].get('max'),
                                     "maxRSS": values['rss'].get('max'), "maxSwap": values['swap'].get('max')}
        summary_dictionary["Avg"] = {"avgVMEM": values['vmem'].get('avg'), "avgPSS": values['pss'].get('avg'),
                                     "avgRSS": values['rss'].get('avg'), "avgSwap": values['swap'].get('avg')}

        # add the last of the rchar, .., values
        keys = ['rchar', 'wchar', 'read_bytes', 'write_bytes', 'nprocs']
        time_keys = ['stime', 'utime']
        keys = keys + time_keys
        # warning: should read_bytes/write_bytes be reported as rbytes/wbytes?
        for key in keys:
            value = get_last_value(dictionary.get(key, None))
            if value:
                if key in time_keys:
                    summary_dictionary["Time"][key] = value
                else:
                    summary_dictionary["Other"][key] = value

    return summary_dictionary


def get_metadata_dict_from_txt(path: str, storejson: bool = False, jobid: str = None) -> dict:
    """
    Convert memory monitor text output to json, store it, and return a selection as a dictionary.

    :param path: path to memory monitor txt output file (str)
    :param storejson: store dictionary on disk if True (bool)
    :param jobid: job id (str)
    :return: prmon metadata (dict).
    """
    # get the raw memory monitor output, convert to dictionary
    dictionary = convert_text_file_to_dictionary(path)

    if dictionary and storejson:
        # add metadata
        dictionary['type'] = 'MemoryMonitorData'
        dictionary['pandaid'] = jobid

        path = os.path.join(os.path.dirname(path), get_memory_monitor_output_filename(suffix='json'))
        logger.debug(f'writing prmon dictionary to: {path}')
        write_json(path, dictionary)
    else:
        logger.debug('nothing to write (no prmon dictionary)')

    # filter dictionary?
    # ..

    return dictionary


def convert_text_file_to_dictionary(path: str) -> dict:
    """
    Convert row-column text file to dictionary.

    User first row identifiers as dictionary keys.
    Note: file must follow the convention:
        NAME1   NAME2   ..
        value1  value2  ..
        ..      ..      ..

    :param path: path to file (str)
    :return: dictionary (dict).
    """
    summary_keys = []  # to keep track of content
    header_locked = False
    dictionary = {}

    with open(path, encoding="utf-8") as f:
        for line in f:
            line = convert_unicode_string(line)
            if line != "":
                try:
                    # Remove empty entries from list (caused by multiple \t)
                    _l = line.replace('\n', '')
                    _l = [_f for _f in _l.split('\t') if _f]

                    # define dictionary keys
                    if isinstance(_l[0], str) and not header_locked:
                        summary_keys = _l
                        for key in _l:
                            dictionary[key] = []
                        header_locked = True
                    else:  # sort the memory measurements in the correct columns
                        for i, key in enumerate(_l):
                            # for key in _l:
                            key_entry = summary_keys[i]  # e.g. Time
                            value = convert_to_int(key)
                            dictionary[key_entry].append(value)
                except Exception:
                    logger.warning(f"unexpected format of utility output: {line}")

    return dictionary


def get_last_value(value_list: list) -> int or None:
    """
    Return the last value in the list.

    :param value_list: list of values (list)
    :return: value (int or None).
    """
    value = None
    if value_list:
        value = value_list[-1]

    return value


def get_average_summary_dictionary(path: str) -> dict:
    """
    Loop over the memory monitor output file and create the averaged summary dictionary.

    :param path: path to memory monitor txt output file (str)
    :return: summary dictionary (dict).
    """
    maxvmem = -1
    maxrss = -1
    maxpss = -1
    maxswap = -1
    avgvmem = 0
    avgrss = 0
    avgpss = 0
    avgswap = 0
    totalvmem = 0
    totalrss = 0
    totalpss = 0
    totalswap = 0
    n = 0
    rchar = None
    wchar = None
    rbytes = None
    wbytes = None

    first = True
    with open(path, encoding="utf-8") as f:
        for line in f:
            # Skip the first line
            if first:
                first = False
                continue
            line = convert_unicode_string(line)
            if line != "":
                try:
                    # Remove empty entries from list (caused by multiple \t)
                    _l = [_f for _f in line.split('\t') if _f]
                    # _time = _l[0]  # 'Time' not user
                    vmem = _l[1]
                    pss = _l[2]
                    rss = _l[3]
                    swap = _l[4]
                    # note: the last rchar etc values will be reported
                    if len(_l) == 9:
                        rchar = int(_l[5])
                        wchar = int(_l[6])
                        rbytes = int(_l[7])
                        wbytes = int(_l[8])
                    else:
                        rchar = None
                        wchar = None
                        rbytes = None
                        wbytes = None
                except (ValueError, TypeError, IndexError) as exc:
                    logger.warning(f"unexpected format of utility output: {line} (expected format: Time, VMEM, PSS, "
                                   f"RSS, Swap [, RCHAR, WCHAR, RBYTES, WBYTES]): {exc}")
                else:
                    # Convert to int
                    ec1, maxvmem, totalvmem = get_max_memory_monitor_value(vmem, maxvmem, totalvmem)
                    ec2, maxpss, totalpss = get_max_memory_monitor_value(pss, maxpss, totalpss)
                    ec3, maxrss, totalrss = get_max_memory_monitor_value(rss, maxrss, totalrss)
                    ec4, maxswap, totalswap = get_max_memory_monitor_value(swap, maxswap, totalswap)
                    if ec1 or ec2 or ec3 or ec4:
                        logger.warning(f"will skip this row of numbers due to value exception: {line}")
                    else:
                        n += 1

        # Calculate averages and store all values
        summary_dictionary = {"Max": {}, "Avg": {}, "Other": {}}
        summary_dictionary["Max"] = {"maxVMEM": maxvmem, "maxPSS": maxpss, "maxRSS": maxrss, "maxSwap": maxswap}
        if rchar:
            summary_dictionary["Other"]["rchar"] = rchar
        if wchar:
            summary_dictionary["Other"]["wchar"] = wchar
        if rbytes:
            summary_dictionary["Other"]["rbytes"] = rbytes
        if wbytes:
            summary_dictionary["Other"]["wbytes"] = wbytes
        if n > 0:
            avgvmem = int(float(totalvmem) / float(n))
            avgpss = int(float(totalpss) / float(n))
            avgrss = int(float(totalrss) / float(n))
            avgswap = int(float(totalswap) / float(n))
        summary_dictionary["Avg"] = {"avgVMEM": avgvmem, "avgPSS": avgpss, "avgRSS": avgrss, "avgSwap": avgswap}

    return summary_dictionary


def get_memory_values(workdir: str, name: str = "") -> dict:
    """
    Find the values in the memory monitor output file.

    In case the summary JSON file has not yet been produced, create a summary dictionary with the same format
    using the output text file (produced by the memory monitor and which is updated once per minute).

    FORMAT:
       {"Max":{"maxVMEM":40058624,"maxPSS":10340177,"maxRSS":16342012,"maxSwap":16235568},
        "Avg":{"avgVMEM":19384236,"avgPSS":5023500,"avgRSS":6501489,"avgSwap":5964997},
        "Other":{"rchar":NN,"wchar":NN,"rbytes":NN,"wbytes":NN}}

    :param workdir: relevant work directory (str)
    :param name: name of memory monitor (str)
    :return: memory values dictionary (dict).
    """
    summary_dictionary = {}

    # Get the path to the proper memory info file (priority ordered)
    path = get_memory_monitor_info_path(workdir, allowtxtfile=True)
    if os.path.exists(path):
        logger.info(f"using path: {path} (trf name={name})")

        # Does a JSON summary file exist? If so, there's no need to calculate maximums and averages in the pilot
        if path.lower().endswith('json'):
            # Read the dictionary from the JSON file
            summary_dictionary = read_json(path)
        else:
            # Loop over the output file, line by line, and look for the maximum PSS value
            if name == "prmon":
                summary_dictionary = get_average_summary_dictionary_prmon(path)
            else:
                summary_dictionary = get_average_summary_dictionary(path)
            logger.debug(f'summary_dictionary={str(summary_dictionary)} (trf name={name})')
    elif path == "":
        logger.warning("filename not set for memory monitor output")
    else:
        # Normally this means that the memory output file has not been produced yet
        pass

    return summary_dictionary


def post_memory_monitor_action(job: JobData):
    """
    Perform post action items for memory monitor.

    :param job: job object (JobData).
    """
    nap = 3
    path1 = os.path.join(job.workdir, get_memory_monitor_summary_filename())
    path2 = os.environ.get('PILOT_HOME')
    counter = 0
    maxretry = 20
    while counter <= maxretry:
        if os.path.exists(path1):
            break
        logger.info(f"taking a short nap ({nap} s) to allow the memory monitor to finish writing to the "
                    f"summary file (#{counter}/#{maxretry})")
        time.sleep(nap)
        counter += 1

    try:
        copy(path1, path2)
    except (NoSuchFile, FileHandlingFailure) as exc:
        logger.warning(f'failed to copy memory monitor output: {exc}')


def precleanup():
    """Pre-cleanup at the beginning of the job to remove any pre-existing files from previous jobs in the main work dir."""
    pass


def get_cpu_arch() -> str:
    """
    Return the CPU architecture string.

    If not returned by this function, the pilot will resort to use the internal scripts/cpu_arch.py.

    :return: CPU arch (str).
    """
    return ""
