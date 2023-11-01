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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-23

import fnmatch
import hashlib
import io
import logging
import os
import re
import subprocess
import tarfile
import time
import uuid
from collections.abc import Mapping as MappingABC
from collections.abc import Iterable as IterableABC
from functools import partial, reduce
from glob import glob
from json import load, JSONDecodeError
from json import dump as dumpjson
from mmap import mmap
from pathlib import Path
from shutil import copy2, rmtree
from typing import Any, IO, Union, Mapping, Iterable
from zipfile import ZipFile, ZIP_DEFLATED
from zlib import adler32

from pilot.common.exception import ConversionFailure, FileHandlingFailure, MKDirFailure, NoSuchFile
from .container import execute
from .math import diff_lists

logger = logging.getLogger(__name__)


def get_pilot_work_dir(workdir: str) -> str:
    """
    Return the full path to the main PanDA Pilot work directory. Called once at the beginning of the batch job.

    :param workdir: The full path to where the main work directory should be created (str)
    :return: The name of main work directory
    """

    return os.path.join(workdir, f"PanDA_Pilot3_{os.getpid()}_{int(time.time())}")


def mkdirs(workdir: str, chmod: int = 0o770) -> None:
    """
    Create a directory.
    Perform a chmod if set.

    :param workdir: Full path to the directory to be created (str)
    :param chmod: chmod code (default 0770) (octal int)
    :raises PilotException: MKDirFailure.
    """

    try:
        os.makedirs(workdir)
        if chmod:
            os.chmod(workdir, chmod)
    except Exception as exc:
        raise MKDirFailure(exc) from exc


def rmdirs(path: str) -> bool:
    """
    Remove directory in path.

    :param path: path to directory to be removed (str)
    :return: True if success, otherwise False (bool).
    """

    status = False

    try:
        rmtree(path)
    except OSError as exc:
        logger.warning(f"failed to remove directories {path}: {exc}")
    else:
        status = True

    return status


def read_file(filename: str, mode: str = 'r') -> str:
    """
    Open, read and close a file.
    :param filename: file name (str)
    :param mode: file mode (str)
    :return: file contents (str).
    """

    out = ""
    _file = open_file(filename, mode)
    if _file:
        out = _file.read()
        _file.close()

    return out


def write_file(path: str, contents: Any, mute: bool = True, mode: str = 'w', unique: bool = False) -> bool:
    """
    Write the given contents to a file.
    If unique=True, then if the file already exists, an index will be added (e.g. 'out.txt' -> 'out-1.txt')

    :param path: full path for file (str)
    :param contents: file contents (Any)
    :param mute: boolean to control stdout info message (bool)
    :param mode: file mode (e.g. 'w', 'r', 'a', 'wb', 'rb') (str)
    :param unique: file must be unique (bool)
    :raises PilotException: FileHandlingFailure
    :return: True if successful, otherwise False (bool).
    """

    status = False

    # add an incremental file name (add -%d if path already exists) if necessary
    if unique:
        path = get_nonexistant_path(path)

    _file = open_file(path, mode)
    if _file:
        try:
            _file.write(contents)
        except IOError as exc:
            raise FileHandlingFailure(exc) from exc
        else:
            status = True
        _file.close()

    if not mute:
        if 'w' in mode:
            logger.info(f'created file: {path}')
        if 'a' in mode:
            logger.info(f'appended file: {path}')

    return status


def open_file(filename: str, mode: str) -> IO:
    """
    Open and return a file pointer for the given mode.
    Note: the caller needs to close the file.

    :param filename: file name (str)
    :param mode: file mode (str)
    :raises PilotException: FileHandlingFailure
    :return: file pointer (IO).
    """

    _file = None
    try:
        _file = open(filename, mode, encoding='utf-8')
    except IOError as exc:
        raise FileHandlingFailure(exc)

    return _file


def find_text_files() -> list:
    """
    Find all non-binary files.

    :return: list of files (list).
    """

    files = []
    # -I = ignore binary files
    cmd = r"find . -type f -exec grep -Iq . {} \; -print"

    _, stdout, _ = execute(cmd)
    if stdout:
        # remove last \n if present
        if stdout.endswith('\n'):
            stdout = stdout[:-1]
        files = stdout.split('\n')

    return files


def get_files(pattern: str = "*.log") -> list:
    """
    Find all files whose names follow the given pattern.

    :param pattern: file name pattern (str)
    :return: list of files (list).
    """

    files = []
    cmd = f"find . -name {pattern}"

    _, stdout, _ = execute(cmd)
    if stdout:
        # remove last \n if present
        if stdout.endswith('\n'):
            stdout = stdout[:-1]
        files = stdout.split('\n')

    return files


def tail(filename: str, nlines: int = 10) -> str:
    """
    Return the last n lines of a file.
    Note: the function uses the posix tail function.

    :param filename: name of file to do the tail on (str)
    :param nlines: number of lines (int)
    :return: file tail (str).
    """

    _, stdout, _ = execute(f'tail -n {nlines} {filename}')
    # protection
    if not isinstance(stdout, str):
        stdout = ""
    return stdout


def head(filename: str, count: int = 20) -> list:
    """
    Return the first several line from the given file.

    :param filename: file name (str)
    :param count: number of lines (int)
    :return: head lines (list).
    """

    ret = None
    with open(filename, 'r', encoding='utf-8') as _file:
        lines = [_file.readline() for line in range(1, count + 1)]
        ret = filter(len, lines)

    return ret


def grep(patterns: list, file_name: str) -> list:
    """
    Search for the patterns in the given list in a file.

    Example:
      grep(["St9bad_alloc", "FATAL"], "athena_stdout.txt")
      -> [list containing the lines below]
        CaloTrkMuIdAlg2.sysExecute()             ERROR St9bad_alloc
        AthAlgSeq.sysExecute()                   FATAL  Standard std::exception is caught

    :param patterns: list of regexp patterns (list)
    :param file_name: file name (str)
    :return: list of matched lines in file (list).
    """

    matched_lines = []
    _pats = []
    for pattern in patterns:
        _pats.append(re.compile(pattern))

    _file = open_file(file_name, 'r')
    if _file:
        while True:
            # get the next line in the file
            line = _file.readline()
            if not line:
                break

            # can the search pattern be found
            for _cp in _pats:
                if re.search(_cp, line):
                    matched_lines.append(line)
        _file.close()

    return matched_lines


def convert(data: Union[str, Mapping, Iterable]) -> Union[str, dict, list]:

    """
    Convert unicode data to utf-8.

    Usage examples:
    1. Dictionary:
      data = {u'Max': {u'maxRSS': 3664, u'maxSwap': 0, u'maxVMEM': 142260, u'maxPSS': 1288}, u'Avg':
             {u'avgVMEM': 94840, u'avgPSS': 850, u'avgRSS': 2430, u'avgSwap': 0}}
    convert(data)
      {'Max': {'maxRSS': 3664, 'maxSwap': 0, 'maxVMEM': 142260, 'maxPSS': 1288}, 'Avg': {'avgVMEM': 94840,
       'avgPSS': 850, 'avgRSS': 2430, 'avgSwap': 0}}
    2. String:
      data = u'hello'
    convert(data)
      'hello'
    3. List:
      data = [u'1',u'2','3']
    convert(data)
      ['1', '2', '3']

    :param data: unicode object to be converted to utf-8
    :return: converted data to utf-8
    """

    if isinstance(data, str):
        ret = str(data)
    elif isinstance(data, MappingABC):
        ret = dict(list(map(convert, iter(list(data.items())))))
    elif isinstance(data, IterableABC):
        ret = type(data)(list(map(convert, data)))
    else:
        ret = data
    return ret


def is_json(input_file: str) -> bool:
    """
    Check if the file is in JSON format.

    This function reads the first few characters of the input file and checks if they match the JSON format.
    It returns True if the file appears to be in JSON format based on the initial characters, and False otherwise.

    :param input_file: The name of the file to be checked (str)
    :return: True if the file appears to be in JSON format, False otherwise (bool).
    """
    try:
        with open(input_file, 'r', encoding='utf-8') as file:
            first_chars = file.read(4)  # Read the first 4 characters
            return first_chars.strip().startswith("{")
    except FileNotFoundError:
        logger.warning(f'no such file: {input_file}')
        return False  # File not found
    except Exception as exc:
        logger.warning(f"exception caught: {exc}")
        return False  # Return False in case of other exceptions


def read_list(filename: str) -> list:
    """
    Read the contents of a JSON file into a list.

    :param filename: file name (str)
    :return: file content (list).
    """

    _list = []

    # open output file for reading
    try:
        with open(filename, 'r', encoding='utf-8') as filehandle:
            _list = load(filehandle)
    except IOError as exc:
        logger.warning(f'failed to read {filename}: {exc}')

    return convert(_list)


def read_json(filename: str) -> dict:
    """
    Read a dictionary with unicode to utf-8 conversion

    :param filename: file name (str)
    :raises PilotException: FileHandlingFailure, ConversionFailure
    :return: json dictionary (dict).
    """

    dictionary = None
    _file = open_file(filename, 'r')
    if _file:
        try:
            dictionary = load(_file)
        except JSONDecodeError as exc:
            logger.warning(f'exception caught: {exc}')
            #raise FileHandlingFailure(str(error))
        else:
            _file.close()

            # Try to convert the dictionary from unicode to utf-8
            if dictionary != {}:
                try:
                    dictionary = convert(dictionary)
                except Exception as exc:
                    raise ConversionFailure(exc) from exc

    return dictionary


def write_json(filename: str, data: Union[dict, list], sort_keys: bool = True, indent: int = 4,
               separators: tuple = (',', ': ')) -> bool:

    """
    Write the dictionary to a JSON file.

    :param filename: file name (string).
    :param data: object to be written to file (dictionary or list).
    :param sort_keys: should entries be sorted? (boolean).
    :param indent: indentation level, default 4 (int).
    :param separators: field separators (default (',', ': ') for dictionaries, use e.g. (',\n') for lists) (tuple)
    :raises PilotException: FileHandlingFailure.
    :return: status (boolean).
    """

    status = False

    try:
        with open(filename, 'w', encoding='utf-8') as _fh:
            dumpjson(data, _fh, sort_keys=sort_keys, indent=indent, separators=separators)
    except IOError as exc:
        logger.warning(f'exception caught in write_json: {exc}')
    else:
        status = True

    return status


def touch(path):
    """
    Touch a file and update mtime in case the file exists.
    Default to use execute() if case of python problem with appending to non-existant path.

    :param path: full path to file to be touched (string).
    """

    try:
        with open(path, 'a', encoding='utf-8'):
            os.utime(path, None)
    except OSError:
        execute(f'touch {path}')


def remove_empty_directories(src_dir):
    """
    Removal of empty directories in the given src_dir tree.
    Only empty directories will be removed.

    :param src_dir: directory to be purged of empty directories.
    """

    for dirpath, _, _ in os.walk(src_dir, topdown=False):
        if dirpath == src_dir:
            break
        try:
            os.rmdir(dirpath)
        except OSError:
            pass


def remove(path):
    """
    Remove file.
    :param path: path to file (string).
    :return: 0 if successful, -1 if failed (int)
    """

    ret = -1
    try:
        os.remove(path)
    except OSError as exc:
        logger.warning(f"failed to remove file: {path} ({exc.errno}, {exc.strerror})")
    else:
        logger.debug(f'removed {path}')
        ret = 0

    return ret


def remove_dir_tree(path):
    """
    Remove directory tree.
    :param path: path to directory (string).
    :return: 0 if successful, -1 if failed (int)
    """

    try:
        rmtree(path)
    except OSError as exc:
        logger.warning(f"failed to remove directory: {path} ({exc.errno}, {exc.strerror})")
        return -1
    return 0


def remove_files(files, workdir=None):
    """
    Remove all given files from workdir.
    If workdir is set, it will be used as base path.

    :param files: file list
    :param workdir: optional working directory (string)
    :return: exit code (0 if all went well, -1 otherwise)
    """

    exitcode = 0
    if not isinstance(files, list):
        logger.warning(f'files parameter not a list: {type(files)}')
        exitcode = -1
    else:
        for _file in files:
            path = os.path.join(workdir, _file) if workdir else _file
            _ec = remove(path)
            if _ec != 0 and exitcode == 0:
                exitcode = _ec

    return exitcode


def tar_files(wkdir, excludedfiles, logfile_name, attempt=0):
    """
    Tarring of files in given directory.

    :param wkdir: work directory (string)
    :param excludedfiles: list of files to be excluded from tar operation (list)
    :param logfile_name: file name (string)
    :param attempt: attempt number (integer)
    :return: 0 if successful, 1 in case of error (int)
    """

    to_pack = []
    pack_start = time.time()
    for path, _, files in os.walk(wkdir):
        for _file in files:
            if _file not in excludedfiles:
                file_rel_path = os.path.join(os.path.relpath(path, wkdir), _file)
                file_path = os.path.join(path, _file)
                to_pack.append((file_path, file_rel_path))
    if to_pack:
        try:
            logfile_name = os.path.join(wkdir, logfile_name)
            log_pack = tarfile.open(logfile_name, 'w:gz')
            for _file in to_pack:
                log_pack.add(_file[0], arcname=_file[1])
            log_pack.close()
        except IOError:
            if attempt == 0:
                safe_delay = 15
                logger.warning(f'i/o error - will retry in {safe_delay} seconds')
                time.sleep(safe_delay)
                tar_files(wkdir, excludedfiles, logfile_name, attempt=1)
            else:
                logger.warning("continues i/o errors during packing of logs - job will fail")
                return 1

    for _file in to_pack:
        remove(_file[0])

    remove_empty_directories(wkdir)
    logger.debug(f"packing of logs took {time.time() - pack_start} seconds")

    return 0


def move(path1, path2):
    """
    Move a file from path1 to path2.

    :param path1: source path (string).
    :param path2: destination path2 (string).
    """

    if not os.path.exists(path1):
        diagnostic = f'file copy failure: path does not exist: {path1}'
        logger.warning(diagnostic)
        raise NoSuchFile(diagnostic)

    try:
        import shutil
        shutil.move(path1, path2)
    except IOError as exc:
        logger.warning(f"exception caught during file move: {exc}")
        raise FileHandlingFailure(exc)
    else:
        logger.info(f"moved {path1} to {path2}")


def copy(path1, path2):
    """
    Copy path1 to path2.

    :param path1: file path (string).
    :param path2: file path (string).
    :raises PilotException: FileHandlingFailure, NoSuchFile
    """

    if not os.path.exists(path1):
        diagnostics = f'file copy failure: path does not exist: {path1}'
        logger.warning(diagnostics)
        raise NoSuchFile(diagnostics)

    try:
        copy2(path1, path2)
    except IOError as exc:
        logger.warning(f"exception caught during file copy: {exc}")
        raise FileHandlingFailure(exc)
    else:
        logger.info(f"copied {path1} to {path2}")


def add_to_total_size(path, total_size):
    """
    Add the size of file in the given path to the total size of all in/output files.

    :param path: path to file (string).
    :param total_size: prior total size of all input/output files (long).
    :return: total size of all input/output files (long).
    """

    if os.path.exists(path):
        # Get the file size
        fsize = get_local_file_size(path)
        if fsize:
            logger.info(f"size of file {path}: {fsize} B")
            total_size += int(fsize)
    else:
        logger.warning(f"skipping file {path} since it is not present")

    return total_size


def get_local_file_size(filename):
    """
    Get the file size of a local file.

    :param filename: file name (string).
    :return: file size (int).
    """

    file_size = None

    if os.path.exists(filename):
        try:
            file_size = os.path.getsize(filename)
        except OSError as exc:
            logger.warning(f"failed to get file size: {exc}")
    else:
        logger.warning(f"local file does not exist: {filename}")

    return file_size


def get_guid():
    """
    Generate a GUID using the uuid library.
    E.g. guid = '92008FAF-BE4C-49CF-9C5C-E12BC74ACD19'

    :return: a random GUID (string)
    """

    return str(uuid.uuid4()).upper()


def get_table_from_file(filename, header=None, separator="\t", convert_to_float=True):
    """
    Extract a table of data from a txt file.
    E.g.
    header="Time VMEM PSS RSS Swap rchar wchar rbytes wbytes"
    or the first line in the file is
    Time VMEM PSS RSS Swap rchar wchar rbytes wbytes
    each of which will become keys in the dictionary, whose corresponding values are stored in lists, with the entries
    corresponding to the values in the rows of the input file.

    The output dictionary will have the format
    {'Time': [ .. data from first row .. ], 'VMEM': [.. data from second row], ..}

    :param filename: name of input text file, full path (string).
    :param header: header string.
    :param separator: separator character (char).
    :param convert_to_float: boolean, if True, all values will be converted to floats.
    :return: dictionary.
    """

    tabledict = {}
    keylist = []  # ordered list of dictionary key names

    try:
        _file = open_file(filename, 'r')
    except FileHandlingFailure as exc:
        logger.warning(f"failed to open file: {filename}, {exc}")
    else:
        firstline = True
        for line in _file:
            fields = line.split(separator)
            if firstline:
                firstline = False
                tabledict, keylist = _define_tabledict_keys(header, fields, separator)
                if not header:
                    continue

            # from now on, fill the dictionary fields with the input data
            i = 0
            for field in fields:
                # get the corresponding dictionary key from the keylist
                key = keylist[i]
                # store the field value in the correct list
                if convert_to_float:
                    try:
                        field = float(field)
                    except (TypeError, ValueError) as exc:
                        logger.warning(f"failed to convert {field} to float: {exc} (aborting)")
                        return None
                tabledict[key].append(field)
                i += 1
        _file.close()

    return tabledict


def _define_tabledict_keys(header, fields, separator):
    """
    Define the keys for the tabledict dictionary.
    Note: this function is only used by parse_table_from_file().

    :param header: header string.
    :param fields: header content string.
    :param separator: separator character (char).
    :return: tabledict (dictionary), keylist (ordered list with dictionary key names).
    """

    tabledict = {}
    keylist = []

    if not header:
        # get the dictionary keys from the header of the file
        for key in fields:
            # first line defines the header, whose elements will be used as dictionary keys
            if key == '':
                continue
            if key.endswith('\n'):
                key = key[:-1]
            tabledict[key] = []
            keylist.append(key)
    else:
        # get the dictionary keys from the provided header
        keys = header.split(separator)
        for key in keys:
            if key == '':
                continue
            if key.endswith('\n'):
                key = key[:-1]
            tabledict[key] = []
            keylist.append(key)

    return tabledict, keylist


def calculate_checksum(filename, algorithm='adler32'):
    """
    Calculate the checksum value for the given file.
    The default algorithm is adler32. Md5 is also be supported.
    Valid algorithms are 1) adler32/adler/ad32/ad, 2) md5/md5sum/md.

    :param filename: file name (string).
    :param algorithm: optional algorithm string.
    :raises FileHandlingFailure, NotImplementedError, Exception.
    :return: checksum value (string).
    """

    if not os.path.exists(filename):
        raise FileHandlingFailure(f'file does not exist: {filename}')

    if algorithm == 'adler32' or algorithm == 'adler' or algorithm == 'ad' or algorithm == 'ad32':
        try:
            checksum = calculate_adler32_checksum(filename)
        except Exception as exc:
            raise exc
        return checksum
    elif algorithm == 'md5' or algorithm == 'md5sum' or algorithm == 'md':
        return calculate_md5_checksum(filename)
    else:
        msg = f'unknown checksum algorithm: {algorithm}'
        logger.warning(msg)
        raise NotImplementedError()


def calculate_adler32_checksum(filename):
    """
    An Adler-32 checksum is obtained by calculating two 16-bit checksums A and B and concatenating their bits
    into a 32-bit integer. A is the sum of all bytes in the stream plus one, and B is the sum of the individual values
    of A from each step.

    :param filename: file name (string).
    :raises: Exception.
    :returns: hexadecimal string, padded to 8 values (string).
    """

    # adler starting value is _not_ 0
    adler = 1

    try:
        with open(filename, 'r+b') as _file:
            _mm = mmap(_file.fileno(), 0)
            for block in iter(partial(_mm.read, io.DEFAULT_BUFFER_SIZE), b''):
                adler = adler32(block, adler)
    except Exception as exc:
        logger.warning(f'failed to get adler32 checksum for file {filename} - {exc} (attempting alternative)')
        try:
            adler = 1  # default adler32 starting value
            blocksize = 64 * 1024 * 1024  # read buffer size, 64 Mb

            with open(filename, 'rb') as _file:
                while True:
                    data = _file.read(blocksize)
                    if not data:
                        break
                    adler = adler32(data, adler)
        except Exception as exc:
            raise Exception(f'failed to get adler32 checksum for file {filename} - {exc} (tried alternative)')

    # backflip on 32bit
    if adler < 0:
        adler = adler + 2 ** 32

    # convert to hex
    return "{0:08x}".format(adler)


def calculate_md5_checksum(filename):
    """
    Calculate the md5 checksum for the given file.
    The file is assumed to exist.

    :param filename: file name (string).
    :return: checksum value (string).
    """

    length = io.DEFAULT_BUFFER_SIZE
    md5 = hashlib.md5()

    with io.open(filename, mode="rb") as _fd:
        for chunk in iter(lambda: _fd.read(length), b''):
            md5.update(chunk)

    return md5.hexdigest()


def get_checksum_value(checksum):
    """
    Return the checksum value.
    The given checksum might either be a standard ad32 or md5 string, or a dictionary with the format
    { checksum_type: value } as defined in the `FileSpec` class. This function extracts the checksum value from this
    dictionary (or immediately returns the checksum value if the given value is a string).

    :param checksum: checksum object (string or dictionary).
    :return: checksum. checksum string.
    """

    if isinstance(checksum, str):
        return checksum

    checksum_value = ''
    checksum_type = get_checksum_type(checksum)

    if isinstance(checksum, dict):
        checksum_value = checksum.get(checksum_type)

    return checksum_value


def get_checksum_type(checksum):
    """
    Return the checksum type (ad32 or md5).
    The given checksum can be either be a standard ad32 or md5 value, or a dictionary with the format
    { checksum_type: value } as defined in the `FileSpec` class.
    In case the checksum type cannot be identified, the function returns 'unknown'.

    :param checksum: checksum string or dictionary.
    :return: checksum type (string).
    """

    checksum_type = 'unknown'
    if isinstance(checksum, dict):
        for key in list(checksum.keys()):
            # the dictionary is assumed to only contain one key-value pair
            checksum_type = key
            break
    elif isinstance(checksum, str):
        if len(checksum) == 8:
            checksum_type = 'ad32'
        elif len(checksum) == 32:
            checksum_type = 'md5'

    return checksum_type


def scan_file(path, error_messages, warning_message=None):
    """
    Scan file for known error messages.

    :param path: path to file (string).
    :param error_messages: list of error messages.
    :param warning_message: optional warning message to be printed with any of the error_messages have been found (string).
    :return: Boolean. (note: True means the error was found)
    """

    found_problem = False

    matched_lines = grep(error_messages, path)
    if matched_lines:
        if warning_message:
            logger.warning(warning_message)
        for line in matched_lines:
            logger.info(line)
        found_problem = True

    return found_problem


def verify_file_list(list_of_files):
    """
    Make sure that the files in the given list exist, return the list of files that does exist.

    :param list_of_files: file list.
    :return: list of existing files.
    """

    # remove any non-existent files from the input file list
    filtered_list = [f for f in list_of_files if os.path.exists(f)]

    diff = diff_lists(list_of_files, filtered_list)
    if diff:
        logger.debug(f'found {len(diff)} file(s) that do not exist (e.g. {diff[0]})')

    return filtered_list


def find_latest_modified_file(list_of_files):
    """
    Find the most recently modified file among the list of given files.
    In case int conversion of getmtime() fails, int(time.time()) will be returned instead.

    :param list_of_files: list of files with full paths.
    :return: most recently updated file (string), modification time (int).
    """

    if not list_of_files:
        logger.warning('there were no files to check mod time for')
        return None, None

    try:
        latest_file = max(list_of_files, key=os.path.getmtime)
        mtime = int(os.path.getmtime(latest_file))
    except OSError as exc:
        logger.warning(f"int conversion failed for mod time: {exc}")
        latest_file = ""
        mtime = None

    return latest_file, mtime


def list_mod_files(file_list):
    """
    List file names along with the mod times.
    Called before looping killer is executed.

    :param file_list: list of files with full paths.
    """

    if file_list:
        logger.info('dumping info for recently modified files prior to looping job kill')
        for _file in file_list:
            try:
                size = int(os.path.getmtime(_file))
            except Exception as exc:
                size = f'unknown (exc={exc})'
            logger.info(f'file name={_file} : mod_time={size}')


def dump(path, cmd="cat"):
    """
    Dump the content of the file in the given path to the log.

    :param path: file path (string).
    :param cmd: optional command (string).
    :return: cat (string).
    """

    if os.path.exists(path) or cmd == "echo":
        _cmd = f"{cmd} {path}"
        _, stdout, stderr = execute(_cmd)
        logger.info(f"{_cmd}:\n{stdout + stderr}")
    else:
        logger.info(f"path {path} does not exist")


def remove_core_dumps(workdir, pid=None):
    """
    Remove any remaining core dumps so they do not end up in the log tarball

    A core dump from the payload process should not be deleted if in debug mode (checked by the called). Also,
    a found core dump from a non-payload process, should be removed but should result in function returning False.

    :param workdir: working directory for payload (string).
    :param pid: payload pid (integer).
    :return: Boolean (True if a payload core dump is found)
    """

    found = False

    coredumps = glob(f"{workdir}/core.*") + glob(f"{workdir}/core")
    if coredumps:
        for coredump in coredumps:
            if pid and os.path.basename(coredump) == f"core.{pid}":
                found = True
            logger.info(f"removing core dump: {coredump}")
            remove(coredump)

    return found


def get_nonexistant_path(fname_path):
    """
    Get the path to a filename which does not exist by incrementing path.

    :param fname_path: file name path (string).
    :return: file name path (string).
    """

    if not os.path.exists(fname_path):
        return fname_path
    filename, file_extension = os.path.splitext(fname_path)
    i = 1
    new_fname = "{}-{}{}".format(filename, i, file_extension)
    while os.path.exists(new_fname):
        i += 1
        new_fname = "{}-{}{}".format(filename, i, file_extension)
    return new_fname


def update_extension(path='', extension=''):
    """
    Update the file name extension to the given extension.

    :param path: file path (string).
    :param extension: new extension (string).
    :return: file path with new extension (string).
    """

    path, _ = os.path.splitext(path)
    if not extension.startswith('.'):
        extension = '.' + extension
    path += extension

    return path


def get_valid_path_from_list(paths):
    """
    Return the first valid path from the given list.

    :param paths: list of file paths.
    :return: first valid path from list (string).
    """

    valid_path = None
    for path in paths:
        if os.path.exists(path):
            valid_path = path
            break

    return valid_path


def copy_pilot_source(workdir, filename=None):
    """
    Copy the pilot source into the work directory.
    If a filename is specified, only that file will be copied.

    :param workdir: working directory (string).
    :param filename: specific filename (string).
    :return: diagnostics (string).
    """

    diagnostics = ""
    srcdir = os.path.join(os.environ.get('PILOT_SOURCE_DIR', '.'), 'pilot3')

    if filename:
        srcdir = os.path.join(srcdir, filename)

    try:
        logger.debug(f'copy {srcdir} to {workdir}')
        pat = '%s' if filename else '%s/*'
        cmd = f'cp -pr {pat} %s' % (srcdir, workdir)
        exit_code, stdout, _ = execute(cmd)
        if exit_code != 0:
            diagnostics = f'file copy failed: {exit_code}, {stdout}'
            logger.warning(diagnostics)
    except Exception as exc:
        diagnostics = f'exception caught when copying pilot3 source: {exc}'
        logger.warning(diagnostics)

    return diagnostics


def create_symlink(from_path='', to_path=''):
    """
    Create a symlink from/to the given paths.

    :param from_path: from path (string).
    :param to_path: to path (string).
    """

    try:
        os.symlink(from_path, to_path)
    except (OSError, FileNotFoundError) as exc:
        logger.warning(f'failed to create symlink from {from_path} to {to_path}: {exc}')
    else:
        logger.debug(f'created symlink from {from_path} to {to_path}')


def locate_file(pattern):
    """
    Locate a file defined by the pattern.

    Example:
        pattern = os.path.join(os.getcwd(), '**/core.123')
        -> /Users/Paul/Development/python/tt/core.123

    :param pattern: pattern name (string).
    :return: path (string).
    """

    path = None
    for fname in glob(pattern):
        if os.path.isfile(fname):
            path = fname

    return path


def find_last_line(filename):
    """
    Find the last line in a (not too large) file.

    :param filename: file name, full path (string).
    :return: last line (string).
    """

    last_line = ""
    with open(filename) as _file:
        line = ""
        for line in _file:
            pass
        if line:
            last_line = line

    return last_line


def get_disk_usage(start_path='.'):
    """
    Calculate the disk usage of the given directory (including any sub-directories).

    :param start_path: directory (string).
    :return: disk usage in bytes (int).
    """

    total_size = 0
    for dirpath, _, filenames in os.walk(start_path):
        for fname in filenames:
            _fp = os.path.join(dirpath, fname)
            # skip if it is symbolic link
            if os.path.exists(_fp) and not os.path.islink(_fp):
                try:
                    total_size += os.path.getsize(_fp)
                except FileNotFoundError as exc:
                    logger.warning(f'caught exception: {exc} (skipping this file)')
                    continue

    return total_size


def extract_lines_from_file(pattern, filename):
    """
    Extract all lines containing 'pattern' from given file.

    :param pattern: text (string).
    :param filename: file name (string).
    :return: text (string).
    """

    _lines = ''
    try:
        with open(filename, 'r') as _file:
            lines = _file.readlines()
            for line in lines:
                if pattern in line:
                    _lines += line
    except EnvironmentError as exc:
        logger.warning(f'exception caught opening file: {exc}')

    return _lines


def find_file(filename, startdir):
    """
    Locate a file in a subdirectory to the given start directory.

    :param filename: file name (string).
    :param startdir: start directory for search (string).
    :return: full path (string).
    """

    logger.debug(f'looking for {filename} in start dir {startdir}')
    _path = None
    for path in Path(startdir).rglob(filename):
        logger.debug(f'located file at: {path}')
        _path = path.as_posix()
        break

    return _path


def zip_files(archivename, files):
    """
    Zip a list of files with standard compression level.

    :param archivename: archive name (string).
    :param files: list of files.
    :return: status (Boolean)
    """

    status = False
    try:

        zipped = False
        with ZipFile(archivename, 'w', ZIP_DEFLATED) as _zip:
            for _file in files:
                if os.path.exists(_file):
                    _zip.write(_file)
                    zipped = True
        if not zipped:
            print('nothing was zipped')
        else:
            status = True

    except Exception as exc:
        print(f'failed to create archive {archivename}: {exc}')

    return status


def generate_test_file(filename, filesize=1024):
    """
    Generate a binary file with the given size in Bytes.

    :param filename: full path, file name (string)
    :param filesize: file size in Bytes (int)
    """

    with open(filename, 'wb') as fout:
        fout.write(os.urandom(filesize))  # replace 1024 with a size in kilobytes if it is not unreasonably large


def get_directory_size(directory: str) -> float:
    """
    Measure the size of the given directory with du -sh.
    The function will return None in case of failure.

    :param directory: full directory path (string).
    :return: size in MB (float).
    """

    size_mb = None
    command = ["du", "-sh", directory]
    output = subprocess.check_output(command)
    # E.g. '269M   /path'
    match = re.search(r"^([0-9.]+)\S+(.*)$", output.decode("utf-8"))
    if match:
        print(match.group(1))
        try:
            size_mb = float(match.group(1))
        except ValueError as exc:
            logger.warning(f'failed to convert {match.group(1)} to float: {exc}')
        # path = match.group(2)
    return size_mb


def get_total_input_size(files: Any, nolib: bool = True) -> int:
    """
    Calculate the total input file size, but do not include the lib file if present.

    :param files: files object (list of FileSpec)
    :param nolib: if True, do not include the lib file in the calculation
    :return: total input file size in bytes (int).
    """

    if not nolib:
        total_size = reduce(lambda x, y: x + y.filesize, files, 0)
    else:
        total_size = 0
        for _file in files:
            if nolib and '.lib.' not in _file.lfn:
                total_size += _file.filesize

    return total_size


def append_to_file(from_file: str, to_file: str) -> bool:
    """
    Appends the contents of one file to another.

    :param from_file: The path to the source file to read from (str)
    :param to_file: The path to the target file to append to (str)
    :return: True if the operation was successful, False otherwise (bool).
    """

    status = False
    try:
        # 1 kB chunk size
        chunk_size = 1024

        # Open the source file in read mode
        with open(from_file, 'r') as source_file:
            # Open the target file in append mode
            with open(to_file, 'a') as target_file:
                while True:
                    # Read a chunk from the source file
                    chunk = source_file.read(chunk_size)
                    if not chunk:
                        target_file.write('--------------------------------------\n')
                        break  # Reached the end of the source file

                    # Write the chunk to the target file
                    target_file.write(chunk)

        status = True

    except FileNotFoundError as exc:
        logger.warning(f"file not found: {exc}")

    except IOError as exc:
        logger.warning(f"an error occurred while processing the file: {exc}")

    return status


def find_files_with_pattern(directory, pattern):
    """
    Find files in a directory that match a specified pattern.

    :param directory: The directory to search for files (str)
    :param pattern: The pattern to match filenames (str)
    :return: a list of matching filenames found in the directory (list).
    """

    try:
        if not os.path.exists(directory):
            raise FileNotFoundError(f"directory '{directory}' does not exist")

        # return all matching files
        return [f for f in os.listdir(directory) if fnmatch.fnmatch(f, pattern)]
    except (FileNotFoundError, PermissionError) as exc:
        logger.warning(f"exception caught while finding files: {exc}")
        return []


def rename_xrdlog(name: str):
    """
    Rename xroot client logfile if it was created.

    :param name: local file name (str).
    """

    xrd_logfile = os.environ.get('XRD_LOGFILE', None)
    if xrd_logfile:
        # xrootd is then expected to have produced a corresponding log file
        pilot_home = os.environ.get('PILOT_HOME', None)
        if pilot_home:
            path = os.path.join(pilot_home, xrd_logfile)
            suffix = Path(xrd_logfile).suffix  # .txt
            stem = Path(xrd_logfile).stem  # xrdlog
            if os.path.exists(path):
                try:
                    os.rename(path, f'{stem}-{name}{suffix}')
                except (NoSuchFile, IOError) as exc:
                    logger.warning(f'exception caught while renaming file: {exc}')
            else:
                logger.warning(f'did not find the expected {xrd_logfile} in {pilot_home}')
        else:
            logger.warning(f'cannot look for {xrd_logfile} since PILOT_HOME was not set')
