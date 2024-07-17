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
# - Danila Oleynik, 2018-2021
# - Paul Nilsson, paul.nilsson@cern.ch, 2020-24

"""Function library for Titan."""

import re
import json
import logging
import numbers
import sys
import traceback
import threading
from typing import Any

logger = logging.getLogger(__name__)


def camel_to_snake(name: str) -> str:
    """
    Change CamelCase to snake_case.

    Used by Python.

    :param name: name to change (str)
    :return: name in snake_case (str).
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def snake_to_camel(snake_str: str) -> str:
    """
    Change snake_case to firstLowCamelCase.

    Used by server.

    :param snake_str: name to change (str)
    :return: name in camelCase (str).
    """
    components = snake_str.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + "".join(x.title() for x in components[1:])


def split(val: str, separator: str = ",", min_len: int = 0, fill_last: bool = False) -> list:
    """
    Split comma separated values and parse them.

    :param val: values to split (str)
    :param separator: comma or whatever (str)
    :param min_len: minimum needed length of array, array is filled up to this value (int)
    :param fill_last: flag stating the array filler, if min_value is greater then extracted array length.
                        If true, array is filled with last value, else, with Nones (bool)
    :return: parsed array (list).
    """
    if val is None:
        return [None for _ in range(min_len)]

    v_arr = val.split(separator)

    for i, v in enumerate(v_arr):
        v_arr[i] = parse_value(v)

    if min_len > len(v_arr):
        filler = None if not fill_last or len(v_arr) < 1 else v_arr[0]
        v_arr.extend([filler for _ in range(min_len - len(v_arr))])

    return v_arr


def get_nulls(val: str) -> str or None:
    """
    Convert every "NULL" string to None.

    :param val: string or whatever
    :return: val or None if val is "NULL"
    """
    return val if val != "NULL" else None


def is_float(val: Any) -> bool:
    """
    Test floatliness of the string value.

    :param val: string or whatever (Any)
    :return: True if the value may be converted to Float (bool).
    """
    try:
        float(val)
    except ValueError:
        return False

    return True


def is_int(val: Any) -> bool:
    """
    Test if the given string is an integer.

    :param val: string or whatever (Any)
    :return: True if the value may be converted to int (bool).
    """
    try:
        int(val)
    except ValueError:
        return False

    return True


def parse_value(value: Any) -> Any:
    """
    Try to parse value as number or None.

    If some of this can be done, parsed value is returned. Otherwise returns
    value unparsed.

    :param value: value to be tested (Any)
    :return: mixed (Any).
    """
    if not isinstance(value, str):
        return value

    if is_int(value):
        return int(value)

    if is_float(value):
        return float(value)

    return get_nulls(value)


def stringify_weird(arg: Any) -> str:
    """
    Convert None to "NULL".

    :param arg: value to stringify (Any)
    :return: arg or "NULL" if arg is None (str).
    """
    if arg is None:
        return "NULL"
    if isinstance(arg, numbers.Number):
        return arg

    return str(arg)


def join(arr: list) -> str:
    """
    Join arrays, converting contents to strings.

    :param arr: array (list)
    :return: joined array (str).
    """
    return ",".join(str(stringify_weird(x)) for x in arr)


def get_input_files(description: dict) -> dict:
    """
    Extract input files from the description.

    :param description: job decsription (dict)
    :return: file dictionary (dict).
    """
    logger.info("extracting input files from job description")
    files = {}
    if description['inFiles'] and description['inFiles'] != "NULL":
        in_files = split(description["inFiles"])
        length = len(in_files)
        ddm_endpoint = split(description.get("ddmEndPointIn"), min_len=length)
        destination_se = split(description.get("destinationSE"), min_len=length)
        dispatch_dblock = split(description.get("dispatchDblock"), min_len=length)
        dispatch_dblock_token = split(description.get("dispatchDBlockToken"), min_len=length)
        datasets = split(description.get("realDatasetsIn"), min_len=length, fill_last=True)
        dblocks = split(description.get("prodDBlocks"), min_len=length)
        dblock_tokens = split(description.get("prodDBlockToken"), min_len=length)
        size = split(description.get("fsize"), min_len=length)
        c_sum = split(description.get("checksum"), min_len=length)
        scope = split(description.get("scopeIn"), min_len=length, fill_last=True)
        guids = split(description.get("GUID"), min_len=length, fill_last=True)

        for counter, _file in enumerate(in_files):
            if _file is not None:
                files[_file] = {
                    "ddm_endpoint": ddm_endpoint[counter],
                    "storage_element": destination_se[counter],
                    "dispatch_dblock": dispatch_dblock[counter],
                    "dispatch_dblock_token": dispatch_dblock_token[counter],
                    "dataset": datasets[counter],
                    "dblock": dblocks[counter],
                    "dblock_token": dblock_tokens[counter],
                    "size": size[counter],
                    "checksum": c_sum[counter],
                    'scope': scope[counter],
                    "guid": guids[counter]
                }

    return files


def fix_log(description: dict, files: dict) -> dict:
    """
    Fix log file description in output files (change GUID and scope).

    :param description: job description (dict)
    :param files: output files (dict)
    :return: fixed output files (dict).
    """
    logger.info("modifying log-specific values in a log file description")
    if description["logFile"] and description["logFile"] != "NULL":
        if description["logGUID"] and description["logGUID"] != "NULL" and description["logFile"] in \
                files:
            files[description["logFile"]]["guid"] = description["logGUID"]
            files[description["logFile"]]["scope"] = description["scopeLog"]

    return files


def get_output_files(description: dict) -> dict:
    """
    Extract output files from the description.

    :param description: job description (dict)
    :return: output files (dict).
    """
    logger.info("extracting output files in description")
    files = {}
    if description['outFiles'] and description['outFiles'] != "NULL":
        out_files = split(description["outFiles"])
        length = len(out_files)
        ddm_endpoint = split(description.get("ddmEndPointOut"), min_len=length)
        destination_se = split(description.get("fileDestinationSE"), min_len=length)
        dblock_token = split(description.get("dispatchDBlockTokenForOut"), min_len=length)
        dblock_tokens = split(description.get("prodDBlockTokenForOut"), min_len=length)
        datasets = split(description.get("realDatasets"), min_len=length)
        dblocks = split(description.get("destinationDblock"), min_len=length)
        destination_dblock_token = split(description.get("destinationDBlockToken"), min_len=length)
        scope = split(description.get("scopeOut"), min_len=length, fill_last=True)

        for counter, _file in enumerate(out_files):
            if _file is not None:
                files[_file] = {
                    "ddm_endpoint": ddm_endpoint[counter],
                    "storage_element": destination_se[counter],
                    "dispatch_dblock_token": dblock_token[counter],
                    "destination_dblock_token": destination_dblock_token[counter],
                    "dblock_token": dblock_tokens[counter],
                    "dataset": datasets[counter],
                    "dblock": dblocks[counter],
                    "scope": scope[counter]
                }

    return fix_log(description, files)


def one_or_set(array: list) -> str:
    """
    Return the only element of array if it's the only one.

    :param array: array (list)
    :return: array[0] or array (str).
    """
    if len(array) < 1:
        return join(array)

    zero = array[0]

    for i in array:
        if i != zero:
            return join(array)

    return stringify_weird(zero)


class JobDescription():
    """Job description class."""

    __holder = None
    __key_aliases = {
        'PandaID': 'jobid',  # it is job id, not PanDA
        'transformation': 'script',  # making it more convenient
        'jobPars': 'script_parameters',  # -.-
        'coreCount': 'number_of_cores',
        'prodUserID': 'user_dn',
        'prodSourceLabel': 'label',  # We don't have any other labels in there. And this is The Label, or just label
        'homepackage': 'home_package',  # lowercase, all of a sudden, splitting words
        "nSent": 'throttle',  # as it's usage says
        'minRamCount': 'minimum_ram',  # reads better
        'maxDiskCount': 'maximum_input_file_size',
        'maxCpuCount': 'maximum_cpu_usage_time',
        'attemptNr': 'attempt_number',  # bad practice to strip words API needs to be readable
    }
    __key_back_aliases = {
        'task_id': 'taskID',  # all ID's are to be placed here, because snake case lacks of all-caps abbrev info
        'jobset_id': 'jobsetID',
        'job_definition_id': 'jobDefinitionID',
        'status_code': 'StatusCode',  # uppercase starting names also should be here
    }
    __soft_key_aliases = {
        'id': 'jobid',
        'command': 'script',
        'command_parameters': 'script_parameters'
    }

    __input_file_keys = {   # corresponding fields in input_files
        'inFiles': '',
        "ddmEndPointIn": 'ddm_endpoint',
        "destinationSE": 'storage_element',
        "dispatchDBlockToken": 'dispatch_dblock_token',
        "realDatasetsIn": 'dataset',
        "prodDBlocks": 'dblock',
        "fsize": 'size',
        "dispatchDblock": 'dispatch_dblock',
        'prodDBlockToken': 'dblock_token',
        "GUID": 'guid',
        "checksum": 'checksum',
        "scopeIn": 'scope'
    }
    __may_be_united = ['guid', 'scope', 'dataset']  # can be sent as one for all files, if is the same

    __output_file_keys = {   # corresponding fields in output_files
        'outFiles': '',
        'ddmEndPointOut': 'ddm_endpoint',
        'fileDestinationSE': 'storage_element',
        'dispatchDBlockTokenForOut': 'dispatch_dblock_token',
        'prodDBlockTokenForOut': 'dblock_token',
        'realDatasets': 'dataset',
        'destinationDblock': 'dblock',
        'destinationDBlockToken': 'destination_dblock_token',
        'scopeOut': 'scope',
        'logGUID': 'guid',
        'scopeLog': 'scope'
    }

    __key_back_aliases_from_forward = None
    __key_reverse_aliases = None
    __key_aliases_snake = None
    input_files = None
    output_files = None

    def __init__(self):
        """Job description constructor."""
        super().__init__()

        self.__key_back_aliases_from_forward = self.__key_back_aliases.copy()
        self.__key_reverse_aliases = {}
        self.__key_aliases_snake = {}
        self.input_files = {}
        self.output_files = {}

        for key, alias in self.__key_aliases.items():
            self.__key_back_aliases_from_forward[alias] = key
            self.__key_aliases_snake[camel_to_snake(key)] = alias

    def get_input_file_prop(self, key: str) -> str:
        """
        Get input file property.

        :param key: property name (str)
        :return: property value (str).
        """
        corresponding_key = self.__input_file_keys[key]
        ret = []

        for _file in self.input_files:
            ret.append(_file if corresponding_key == '' else self.input_files[_file][corresponding_key])

        if corresponding_key in self.__may_be_united:
            return one_or_set(ret)

        return join(ret)

    def get_output_file_prop(self, key: str) -> str:
        """
        Get output file property.

        :param key: property name (str)
        :return: property value (str).
        """
        log_file = self.log_file

        if key == 'logGUID':
            return stringify_weird(self.output_files[log_file]['guid'])
        if key == 'scopeLog':
            return stringify_weird(self.output_files[log_file]['scope'])

        corresponding_key = self.__output_file_keys[key]
        ret = []

        for _file in self.output_files:
            if key != 'scopeOut' or _file != log_file:
                ret.append(_file if corresponding_key == '' else self.output_files[_file][corresponding_key])

        if corresponding_key in self.__may_be_united:
            return one_or_set(ret)

        return join(ret)

    def load(self, new_desc: Any):
        """
        Load job description.

        :param new_desc: job description (Any).
        """
        if isinstance(new_desc, str):
            new_desc = json.loads(new_desc)

        if "PandaID" in new_desc:
            logger.info("Parsing description to be of readable, easy to use format")

            fixed = {}

            self.input_files = get_input_files(new_desc)
            self.output_files = get_output_files(new_desc)

            for key in new_desc:
                value = new_desc[key]

                if key not in self.__input_file_keys and key not in self.__output_file_keys:
                    old_key = key
                    if key in self.__key_aliases:
                        key = self.__key_aliases[key]
                    else:
                        key = camel_to_snake(key)

                    if key != old_key:
                        self.__key_back_aliases_from_forward[key] = old_key

                    self.__key_reverse_aliases[old_key] = key

                    fixed[key] = parse_value(value)

            new_desc = fixed
        else:
            self.input_files = new_desc['input_files']
            self.output_files = new_desc['output_files']

        self.__holder = new_desc

    def to_json(self, decompose: bool = False, **kwargs: dict) -> str:
        """
        Convert description to JSON.

        :param decompose: flag stating if the description should be decomposed (bool)
        :param kwargs: additional arguments for json.dumps (dict)
        :return: JSON representation of the description (str).
        """
        if decompose:
            prep = {}

            for k in self.__holder:
                if k not in ['input_files', 'output_files']:
                    if k in self.__key_back_aliases_from_forward:
                        rev = self.__key_back_aliases_from_forward[k]
                    else:
                        rev = snake_to_camel(k)
                    prep[rev] = stringify_weird(self.__holder[k])

            for k in self.__output_file_keys:
                prep[k] = self.get_output_file_prop(k)
            for k in self.__input_file_keys:
                prep[k] = self.get_input_file_prop(k)

        else:
            prep = self.__holder.copy()
            prep['input_files'] = self.input_files
            prep['output_files'] = self.output_files

        return json.dumps(prep, **kwargs)

    def get_description_parameter(self, key: str) -> str:
        """
        Get description parameter.

        :param key: parameter name (str)
        :return: parameter value (str)
        :raises: AttributeError if parameter not found.
        """
        if self.__holder is not None:
            if key in self.__holder:
                return self.__holder[key]

            if key in self.__input_file_keys:
                logger.warning((f"Old key JobDescription.{key} is used. "
                                f"Better to use JobDescription.input_files[][{self.__input_file_keys[key]}] to "
                                "access and manipulate this value.\n") + self.get_traceback())
                return self.get_input_file_prop(key)
            if key in self.__output_file_keys:
                logger.warning((f"Old key JobDescription.{key} is used. "
                                f"Better to use JobDescription.output_files[][{self.__output_file_keys[key]}] to "
                                "access and manipulate this value.\n") + self.get_traceback())
                return self.get_output_file_prop(key)

            snake_key = camel_to_snake(key)
            if snake_key in self.__key_aliases_snake:
                logger.warning((f"Old key JobDescription.{key} is used. "
                                f"Better to use JobDescription.{self.__key_aliases_snake[snake_key]} to access and "
                                "manipulate this value.\n") + self.get_traceback())
                return stringify_weird(self.__holder[self.__key_aliases_snake[snake_key]])

            if key in self.__soft_key_aliases:
                return self.get_description_parameter(self.__soft_key_aliases[key])

        raise AttributeError("Description parameter not found")

    def set_description_parameter(self, key: str, value: Any) -> bool:
        """
        Set description parameter.

        :param key: parameter name (str)
        :param value: parameter value (Any)
        :return: True if parameter was set, False otherwise (bool)
        :raises: AttributeError if parameter is read-only.
        """
        if self.__holder is not None:
            if key in self.__holder:
                self.__holder[key] = value
                return True

            if key in self.__input_file_keys:
                err = f"Key JobDescription.{key} is read-only\n"
                if key == 'inFiles':
                    err += "Use JobDescription.input_files to manipulate input files"
                else:
                    err += f"Use JobDescription.input_files[][{self.__input_file_keys[key]}] to " \
                           f"set up this parameter in files description"
                raise AttributeError(err)

            if key in self.__output_file_keys:
                err = f"Key JobDescription.{key} is read-only\n"
                if key == 'outFiles':
                    err += "Use JobDescription.output_files to manipulate output files"
                else:
                    err += f"Use JobDescription.output_files[][{self.__output_file_keys[key]}] to " \
                           f"set up this parameter in files description"
                raise AttributeError(err)

            snake_key = camel_to_snake(key)
            if snake_key in self.__key_aliases_snake:
                logger.warning((f"Old key JobDescription.{key} is used. Better to use "
                                f"JobDescription.{self.__key_aliases_snake[snake_key]} to access and "
                                "manipulate this value.\n") + self.get_traceback())
                self.__holder[self.__key_aliases_snake[snake_key]] = parse_value(value)

            if key in self.__soft_key_aliases:
                return self.set_description_parameter(self.__soft_key_aliases[key], value)

        return False

    def get_traceback(self) -> str:
        """
        Get traceback.

        :return: traceback (str).
        """
        tb = list(reversed(traceback.extract_stack()))

        tb_str = '\n'
        for ii in enumerate(tb):
            if ii[0] < 3:
                continue  # we don't need inner scopes of this and subsequent calls
            i = ii[1]
            tb_str += f'{i[0]}:{i[1]} (in {i[2]}): {i[3]}\n'
        thread = threading.current_thread()

        return 'Traceback: (latest call first)' + tb_str + f'Thread: {thread.name}({thread.ident})'

    def __getattr__(self, key: str) -> str:
        """
        Return attribute value.

        :param key: attribute name (str)
        :return: attribute value (str).
        """
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            return self.get_description_parameter(key)

    def __setattr__(self, key: str, value: Any) -> Any:
        """
        Set attribute value.

        :param key: attribute name (str)
        :param value: attribute value (Any).
        :return: attribute value (Any).
        """
        try:
            object.__getattribute__(self, key)
        except AttributeError:
            if not self.set_description_parameter(key, value):
                return object.__setattr__(self, key, value)
            return None

        return object.__setattr__(self, key, value)


if __name__ == "__main__":
    logging.basicConfig()
    logger.setLevel(logging.DEBUG)

    jd = JobDescription()
    with open(sys.argv[1], "r", encoding='utf-8') as _fil:
        contents = _fil.read()

    jd.load(contents)

    logger.debug(jd.id)
    logger.debug(jd.command)
    logger.debug(jd.PandaID)
    logger.debug(jd.scopeOut)
    logger.debug(jd.scopeLog)
    logger.debug(jd.fileDestinationSE)
    logger.debug(jd.inFiles)
    logger.debug(json.dumps(jd.output_files, indent=4, sort_keys=True))

    logger.debug(jd.to_json(True, indent=4, sort_keys=True))
