#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2022

import hashlib
import io
import os
import re
import tarfile
import time
import uuid
import sys
import logging
from collections.abc import Iterable, Mapping
from glob import glob
from json import load, JSONDecodeError
from json import dump as dumpjson
from pathlib import Path
from shutil import copy2, rmtree
from zlib import adler32
from functools import partial
from mmap import mmap

from pilot.common.exception import ConversionFailure, FileHandlingFailure, MKDirFailure, NoSuchFile
from pilot.util.config import config
from .container import execute
from .math import diff_lists

logger = logging.getLogger(__name__)


def get_pilot_work_dir(workdir):
    """
    Return the full path to the main PanDA Pilot work directory. Called once at the beginning of the batch job.

    :param workdir: The full path to where the main work directory should be created
    :return: The name of main work directory
    """

    return os.path.join(workdir, "PanDA_Pilot3_%d_%s" % (os.getpid(), str(int(time.time()))))


def mkdirs(workdir, chmod=0o770):
    """
    Create a directory.
    Perform a chmod if set.

    :param workdir: Full path to the directory to be created
    :param chmod: chmod code (default 0770) (octal).
    :raises PilotException: MKDirFailure.
    :return:
    """

    try:
        os.makedirs(workdir)
        if chmod:
            os.chmod(workdir, chmod)
    except Exception as exc:
        raise MKDirFailure(exc)


def rmdirs(path):
    """
    Remove directory in path.

    :param path: path to directory to be removed (string).
    :return: Boolean (True if success).
    """

    status = False

    try:
        rmtree(path)
    except OSError as exc:
        logger.warning("failed to remove directories %s: %s", path, exc)
    else:
        status = True

    return status


def read_file(filename, mode='r'):
    """
    Open, read and close a file.
    :param filename: file name (string).
    :param mode:
    :return: file contents (string).
    """

    out = ""
    _file = open_file(filename, mode)
    if _file:
        out = _file.read()
        _file.close()

    return out


def write_file(path, contents, mute=True, mode='w', unique=False):
    """
    Write the given contents to a file.
    If unique=True, then if the file already exists, an index will be added (e.g. 'out.txt' -> 'out-1.txt')
    :param path: full path for file (string).
    :param contents: file contents (object).
    :param mute: boolean to control stdout info message.
    :param mode: file mode (e.g. 'w', 'r', 'a', 'wb', 'rb') (string).
    :param unique: file must be unique (Boolean).
    :raises PilotException: FileHandlingFailure.
    :return: True if successful, otherwise False.
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
            raise FileHandlingFailure(exc)
        else:
            status = True
        _file.close()

    if not mute:
        if 'w' in mode:
            logger.info('created file: %s', path)
        if 'a' in mode:
            logger.info('appended file: %s', path)

    return status


def open_file(filename, mode):
    """
    Open and return a file pointer for the given mode.
    Note: the caller needs to close the file.

    :param filename: file name (string).
    :param mode: file mode (character).
    :raises PilotException: FileHandlingFailure.
    :return: file pointer.
    """

    _file = None
    try:
        _file = open(filename, mode)
    except IOError as exc:
        raise FileHandlingFailure(exc)

    return _file


def find_text_files():
    """
    Find all non-binary files.

    :return: list of files.
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


def get_files(pattern="*.log"):
    """
    Find all files whose names follow the given pattern.

    :param pattern: file name pattern (string).
    :return: list of files.
    """

    files = []
    cmd = "find . -name %s" % pattern

    _, stdout, _ = execute(cmd)
    if stdout:
        # remove last \n if present
        if stdout.endswith('\n'):
            stdout = stdout[:-1]
        files = stdout.split('\n')

    return files


def tail(filename, nlines=10):
    """
    Return the last n lines of a file.
    Note: the function uses the posix tail function.

    :param filename: name of file to do the tail on (string).
    :param nlines: number of lines (int).
    :return: file tail (str).
    """

    _, stdout, _ = execute('tail -n %d %s' % (nlines, filename))
    # protection
    if not isinstance(stdout, str):
        stdout = ""
    return stdout


def grep(patterns, file_name):
    """
    Search for the patterns in the given list in a file.

    Example:
      grep(["St9bad_alloc", "FATAL"], "athena_stdout.txt")
      -> [list containing the lines below]
        CaloTrkMuIdAlg2.sysExecute()             ERROR St9bad_alloc
        AthAlgSeq.sysExecute()                   FATAL  Standard std::exception is caught

    :param patterns: list of regexp patterns.
    :param file_name: file name (string).
    :return: list of matched lines in file.
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


def convert(data):
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
    elif isinstance(data, Mapping):
        ret = dict(list(map(convert, iter(list(data.items())))))
    elif isinstance(data, Iterable):
        ret = type(data)(list(map(convert, data)))
    else:
        ret = data
    return ret


def is_json(input_file):
    """
    Check if the file is in JSON format.
    The function reads the first character of the file, and if it is "{" then returns True.

    :param input_file: file name (string)
    :return: Boolean.
    """

    with open(input_file) as unknown_file:
        first_char = unknown_file.read(1)
        if first_char == '{':
            return True
        return False


def read_list(filename):
    """
    Read a list from a JSON file.

    :param filename: file name (string).
    :return: list.
    """

    _list = []

    # open output file for reading
    try:
        with open(filename, 'r') as filehandle:
            _list = load(filehandle)
    except IOError as exc:
        logger.warning('failed to read %s: %s', filename, exc)

    return convert(_list)


def read_json(filename):
    """
    Read a dictionary with unicode to utf-8 conversion

    :param filename:
    :raises PilotException: FileHandlingFailure, ConversionFailure
    :return: json dictionary
    """

    dictionary = None
    _file = open_file(filename, 'r')
    if _file:
        try:
            dictionary = load(_file)
        except JSONDecodeError as exc:
            logger.warning('exception caught: %s', exc)
            #raise FileHandlingFailure(str(error))
        else:
            _file.close()

            # Try to convert the dictionary from unicode to utf-8
            if dictionary != {}:
                try:
                    dictionary = convert(dictionary)
                except Exception as exc:
                    raise ConversionFailure(exc)

    return dictionary


def write_json(filename, data, sort_keys=True, indent=4, separators=(',', ': ')):
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
        with open(filename, 'w') as _fh:
            dumpjson(data, _fh, sort_keys=sort_keys, indent=indent, separators=separators)
    except IOError as exc:
        raise FileHandlingFailure(exc)
    else:
        status = True

    return status


def touch(path):
    """
    Touch a file and update mtime in case the file exists.
    Default to use execute() if case of python problem with appending to non-existant path.

    :param path: full path to file to be touched (string).
    :return:
    """

    try:
        with open(path, 'a'):
            os.utime(path, None)
    except OSError:
        _ = execute('touch %s' % path)


def remove_empty_directories(src_dir):
    """
    Removal of empty directories in the given src_dir tree.
    Only empty directories will be removed.

    :param src_dir: directory to be purged of empty directories.
    :return:
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

    try:
        os.remove(path)
    except OSError as exc:
        logger.warning("failed to remove file: %s (%s, %s)", path, exc.errno, exc.strerror)
        return -1
    else:
        logger.debug('removed %s', path)

    return 0


def remove_dir_tree(path):
    """
    Remove directory tree.
    :param path: path to directory (string).
    :return: 0 if successful, -1 if failed (int)
    """

    try:
        rmtree(path)
    except OSError as exc:
        logger.warning("failed to remove directory: %s (%s, %s)", path, exc.errno, exc.strerror)
        return -1
    return 0


def remove_files(workdir, files):
    """
    Remove all given files from workdir.

    :param workdir: working directory (string).
    :param files: file list.
    :return: exit code (0 if all went well, -1 otherwise)
    """

    exitcode = 0
    if not isinstance(files, list):
        logger.warning('files parameter not a list: %s', str(type(list)))
        exitcode = -1
    else:
        for _file in files:
            _ec = remove(os.path.join(workdir, _file))
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
                logger.warning('i/o error - will retry in {0} seconds'.format(safe_delay))
                time.sleep(safe_delay)
                tar_files(wkdir, excludedfiles, logfile_name, attempt=1)
            else:
                logger.warning("continues i/o errors during packing of logs - job will fail")
                return 1

    for _file in to_pack:
        remove(_file[0])

    remove_empty_directories(wkdir)
    logger.debug("packing of logs took {0} seconds".format(time.time() - pack_start))

    return 0


def move(path1, path2):
    """
    Move a file from path1 to path2.

    :param path1: source path (string).
    :param path2: destination path2 (string).
    """

    if not os.path.exists(path1):
        logger.warning('file copy failure: path does not exist: %s', path1)
        raise NoSuchFile("File does not exist: %s" % path1)

    try:
        import shutil
        shutil.move(path1, path2)
    except IOError as exc:
        logger.warning("exception caught during file move: %s", exc)
        raise FileHandlingFailure(exc)
    else:
        logger.info("moved %s to %s", path1, path2)


def copy(path1, path2):
    """
    Copy path1 to path2.

    :param path1: file path (string).
    :param path2: file path (string).
    :raises PilotException: FileHandlingFailure, NoSuchFile
    :return:
    """

    if not os.path.exists(path1):
        logger.warning('file copy failure: path does not exist: %s', path1)
        raise NoSuchFile("File does not exist: %s" % path1)

    try:
        copy2(path1, path2)
    except IOError as exc:
        logger.warning("exception caught during file copy: %s", exc)
        raise FileHandlingFailure(exc)
    else:
        logger.info("copied %s to %s", path1, path2)


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
            logger.info("size of file %s: %d B", path, fsize)
            total_size += int(fsize)
    else:
        logger.warning("skipping file %s since it is not present", path)

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
            logger.warning("failed to get file size: %s", exc)
    else:
        logger.warning("local file does not exist: %s", filename)

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
        logger.warning("failed to open file: %s, %s", filename, exc)
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
                        logger.warning("failed to convert %s to float: %s (aborting)", field, exc)
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
        raise FileHandlingFailure('file does not exist: %s' % filename)

    if algorithm == 'adler32' or algorithm == 'adler' or algorithm == 'ad' or algorithm == 'ad32':
        try:
            checksum = calculate_adler32_checksum(filename)
        except Exception as exc:
            raise exc
        return checksum
    elif algorithm == 'md5' or algorithm == 'md5sum' or algorithm == 'md':
        return calculate_md5_checksum(filename)
    else:
        msg = 'unknown checksum algorithm: %s' % algorithm
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
    :param warning_message: optional warning message to printed with any of the error_messages have been found (string).
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
        logger.debug('found %d file(s) that do not exist (e.g. %s)', len(diff), diff[0])

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
        logger.warning("int conversion failed for mod time: %s", exc)
        latest_file = ""
        mtime = None

    return latest_file, mtime


def dump(path, cmd="cat"):
    """
    Dump the content of the file in the given path to the log.

    :param path: file path (string).
    :param cmd: optional command (string).
    :return: cat (string).
    """

    if os.path.exists(path) or cmd == "echo":
        _cmd = "%s %s" % (cmd, path)
        _, stdout, stderr = execute(_cmd)
        logger.info("%s:\n%s", _cmd, stdout + stderr)
    else:
        logger.info("path %s does not exist", path)


def establish_logging(debug=True, nopilotlog=False, filename=config.Pilot.pilotlog, loglevel=0):
    """
    Setup and establish logging.

    Option loglevel can be used to decide which (predetermined) logging format to use.
    Example:
      loglevel=0: '%(asctime)s | %(levelname)-8s | %(name)-32s | %(funcName)-25s | %(message)s'
      loglevel=1: 'ts=%(asctime)s level=%(levelname)-8s event=%(name)-32s.%(funcName)-25s msg="%(message)s"'

    :param debug: debug mode (Boolean),
    :param nopilotlog: True when pilot log is not known (Boolean).
    :param filename: name of log file (string).
    :param loglevel: selector for logging level (int).
    :return:
    """

    _logger = logging.getLogger('')
    _logger.handlers = []
    _logger.propagate = False

    console = logging.StreamHandler(sys.stdout)
    if debug:
        format_str = '%(asctime)s | %(levelname)-8s | %(name)-32s | %(funcName)-25s | %(message)s'
        level = logging.DEBUG
    else:
        format_str = '%(asctime)s | %(levelname)-8s | %(message)s'
        level = logging.INFO
    #rank, maxrank = get_ranks_info()
    #if rank is not None:
    #    format_str = 'Rank {0} |'.format(rank) + format_str
    if nopilotlog:
        logging.basicConfig(level=level, format=format_str, filemode='w')
    else:
        logging.basicConfig(filename=filename, level=level, format=format_str, filemode='w')
    console.setLevel(level)
    console.setFormatter(logging.Formatter(format_str))
    logging.Formatter.converter = time.gmtime
    #if not len(_logger.handlers):
    _logger.addHandler(console)


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

    coredumps = glob("%s/core.*" % workdir) + glob("%s/core" % workdir)
    if coredumps:
        for coredump in coredumps:
            if pid and os.path.basename(coredump) == "core.%d" % pid:
                found = True
            logger.info("removing core dump: %s", str(coredump))
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


def copy_pilot_source(workdir):
    """
    Copy the pilot source into the work directory.

    :param workdir: working directory (string).
    :return: diagnostics (string).
    """

    diagnostics = ""
    srcdir = os.path.join(os.environ.get('PILOT_SOURCE_DIR', '.'), 'pilot3')
    try:
        logger.debug('copy %s to %s', srcdir, workdir)
        cmd = 'cp -r %s/* %s' % (srcdir, workdir)
        exit_code, stdout, _ = execute(cmd)
        if exit_code != 0:
            diagnostics = 'file copy failed: %d, %s' % (exit_code, stdout)
            logger.warning(diagnostics)
    except Exception as exc:
        diagnostics = 'exception caught when copying pilot3 source: %s' % exc
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
        logger.warning('failed to create symlink from %s to %s: %s', from_path, to_path, exc)
    else:
        logger.debug('created symlink from %s to %s', from_path, to_path)


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
                    logger.warning('caught exception: %s (skipping this file)', exc)
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
