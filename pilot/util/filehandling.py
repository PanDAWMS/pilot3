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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-25

"""A collection of functions related to file handling."""

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
from mmap import mmap, ACCESS_READ
from pathlib import Path
from shutil import copy2, rmtree
from typing import Any, IO, Optional, Union, Mapping, Iterable
from zipfile import ZipFile, ZIP_DEFLATED
from zlib import adler32

from pilot.common.exception import ConversionFailure, FileHandlingFailure, MKDirFailure, NoSuchFile
from .container import execute
from .math import diff_lists

logger = logging.getLogger(__name__)


def get_pilot_work_dir(workdir: str) -> str:
    """Return the full path to the main PanDA Pilot work directory.

    Called once at the beginning of the batch job.

    Args:
        workdir: Full path to the location where the main work directory should
            be created.

    Returns:
        Full path to the newly named work directory.
    """
    return os.path.join(workdir, f"PanDA_Pilot3_{os.getpid()}_{int(time.time())}")


def mkdirs(workdir: str, chmod: int = 0o770) -> None:
    """Create a directory, performing a chmod if specified.

    Args:
        workdir: Full path to the directory to be created.
        chmod: Permission bits to apply after creation (default 0o770).

    Raises:
        MKDirFailure: If the directory cannot be created.
    """
    try:
        os.makedirs(workdir)
        if chmod:
            os.chmod(workdir, chmod)
    except Exception as exc:
        raise MKDirFailure(exc) from exc


def rmdirs(path: str) -> bool:
    """Remove the directory tree at the given path.

    Args:
        path: Path to the directory to be removed.

    Returns:
        True if removal was successful, False otherwise.
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
    """Open, read, and close a file.

    Args:
        filename: File name/path to read.
        mode: File open mode.

    Returns:
        File contents as a string.
    """
    out = ""
    _file = open_file(filename, mode)
    if _file:
        out = _file.read()
        _file.close()

    return out


def write_file(path: str, contents: Any, mute: bool = True, mode: str = 'w', unique: bool = False) -> bool:
    """Write the given contents to a file.

    If ``unique=True`` and the file already exists, an incrementing index is
    appended (e.g. ``'out.txt'`` → ``'out-1.txt'``).

    Args:
        path: Full path for the output file.
        contents: Data to write.
        mute: If False, log an info message after writing.
        mode: File open mode (e.g. ``'w'``, ``'a'``, ``'wb'``).
        unique: If True, ensure the file path does not already exist by
            appending an index.

    Raises:
        FileHandlingFailure: If the file cannot be written.

    Returns:
        True if successful, False otherwise.
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
    """Open and return a file pointer for the given mode.

    Note: the caller is responsible for closing the file.

    Args:
        filename: File name/path to open.
        mode: File open mode.

    Raises:
        FileHandlingFailure: If the file cannot be opened.

    Returns:
        An open file object, or None if opening failed.
    """
    _file = None
    try:
        _file = open(filename, mode, encoding='utf-8')
    except IOError as exc:
        raise FileHandlingFailure(exc)

    return _file


def find_text_files() -> list:
    """Find all non-binary files in the current directory tree.

    Returns:
        List of paths to text files found.
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
    """Find all files whose names follow the given pattern.

    Args:
        pattern: File name glob pattern.

    Returns:
        List of matching file paths.
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
    """Return the last n lines of a file using the posix ``tail`` command.

    Args:
        filename: Path to the file.
        nlines: Number of lines to return.

    Returns:
        The last ``nlines`` lines of the file as a string.
    """
    _, stdout, _ = execute(f'tail -n {nlines} {filename}')
    # protection
    if not isinstance(stdout, str):
        stdout = ""
    return stdout


def head(filename: str, count: int = 20) -> list:
    """Return the first several lines from the given file.

    Args:
        filename: Path to the file.
        count: Number of lines to return.

    Returns:
        A filter object containing the non-empty head lines.
    """
    ret = None
    with open(filename, 'r', encoding='utf-8') as _file:
        lines = [_file.readline() for line in range(1, count + 1)]
        ret = filter(len, lines)

    return ret


def grep(patterns: list, file_name: str) -> list:
    """Search for the patterns in the given file.

    Example::

        grep(["St9bad_alloc", "FATAL"], "athena_stdout.txt")
        # -> lines containing 'St9bad_alloc' or 'FATAL'

    Args:
        patterns: List of regexp patterns to search for.
        file_name: Path to the file to search.

    Returns:
        List of lines from the file that match any of the given patterns.
    """
    matched_lines = []
    compiled_patterns = [re.compile(pattern) for pattern in patterns]

    with open(file_name, 'r', encoding='utf-8') as _file:
        matched_lines = [
            line for line in _file
            if any(compiled_pattern.search(line) for compiled_pattern in compiled_patterns)
        ]

    return matched_lines


def grep_old(patterns: list, file_name: str) -> list:
    """Search for the patterns in the given file (legacy implementation).

    Example::

        grep_old(["St9bad_alloc", "FATAL"], "athena_stdout.txt")
        # -> lines containing 'St9bad_alloc' or 'FATAL'

    Args:
        patterns: List of regexp patterns to search for.
        file_name: Path to the file to search.

    Returns:
        List of lines from the file that match any of the given patterns.
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
    """Convert unicode data to utf-8.

    Examples::

        # Dictionary
        convert({u'Max': {u'maxRSS': 3664}})
        # -> {'Max': {'maxRSS': 3664}}

        # String
        convert(u'hello')
        # -> 'hello'

        # List
        convert([u'1', u'2', '3'])
        # -> ['1', '2', '3']

    Args:
        data: Unicode object to be converted to utf-8.

    Returns:
        Converted data in utf-8 encoding.
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
    """Check if the file is in JSON format.

    Reads the first few characters of the input file and checks whether they
    match the JSON format (i.e. the content starts with ``{``).

    Args:
        input_file: Path to the file to check.

    Returns:
        True if the file appears to be in JSON format, False otherwise.
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
    """Read the contents of a JSON file into a list.

    Args:
        filename: Path to the JSON file.

    Returns:
        File content as a list (empty list on failure).
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
    """Read a JSON file into a dictionary with unicode-to-utf-8 conversion.

    Args:
        filename: Path to the JSON file.

    Raises:
        FileHandlingFailure: If the file cannot be opened.
        ConversionFailure: If the unicode-to-utf-8 conversion fails.

    Returns:
        Parsed JSON dictionary, or None if parsing fails.
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
               separators: tuple[str, str] = (',', ': ')) -> bool:
    r"""Write the dictionary or list to a JSON file.

    Args:
        filename: Output file name/path.
        data: Object to be serialised (dictionary or list).
        sort_keys: If True, dictionary keys are sorted in the output.
        indent: Indentation level in the JSON output (default 4).
        separators: Field separators tuple (default ``(',', ': ')`` for
            dictionaries; use e.g. ``(',\n',)`` for lists).

    Returns:
        True if the file was written successfully, False otherwise.
    """
    status = False

    try:
        with open(filename, 'w', encoding='utf-8') as _fh:
            dumpjson(data, _fh, sort_keys=sort_keys, indent=indent, separators=separators)
    except (IOError, TypeError) as exc:
        logger.warning(f'exception caught (1) in write_json: {exc}')
    except Exception as exc:
        logger.warning(f'exception caught (2) in write_json: {exc}')
    else:
        status = True

    return status


def touch(path: str) -> None:
    """Touch a file and update mtime if the file already exists.

    Falls back to the shell ``touch`` command if the Python open call fails.

    Args:
        path: Full path to the file to be touched.
    """
    try:
        with open(path, 'a', encoding='utf-8'):
            os.utime(path, None)
    except OSError:
        execute(f'touch {path}')


def remove_empty_directories(src_dir: str) -> None:
    """Remove empty directories in the given directory tree.

    Only completely empty directories are removed; non-empty directories are
    left in place.

    Args:
        src_dir: Root directory whose empty sub-directories will be removed.
    """
    for dirpath, _, _ in os.walk(src_dir, topdown=False):
        if dirpath == src_dir:
            break
        try:
            os.rmdir(dirpath)
        except OSError:
            pass


def remove(path: str) -> int:
    """Remove the given file.

    Args:
        path: Path to the file to remove.

    Returns:
        0 if successful, -1 if removal failed.
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


def remove_dir_tree(path: str) -> int:
    """Remove the given directory tree.

    Args:
        path: Path to the directory to remove.

    Returns:
        0 if successful, -1 if removal failed.
    """
    try:
        rmtree(path)
    except OSError as exc:
        logger.warning(f"failed to remove directory: {path} ({exc.errno}, {exc.strerror})")
        return -1
    logger.debug(f'removed {path}')

    return 0


def remove_files(files: list, workdir: str = "") -> int:
    """Remove all given files from the given workdir.

    If ``workdir`` is set, it is prepended to each file path.

    Args:
        files: List of file names (or full paths if ``workdir`` is not given).
        workdir: Optional base directory prepended to each file name.

    Returns:
        0 if all removals succeeded, -1 if any removal failed.
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


def tar_files(wkdir: str, excludedfiles: list, logfile_name: str, attempt: int = 0) -> int:
    """Tar the files in the given directory into a compressed archive.

    Args:
        wkdir: Work directory containing the files to pack.
        excludedfiles: List of file names to exclude from the tar operation.
        logfile_name: Name of the output tar archive file.
        attempt: Internal retry counter; should not be set by callers.

    Returns:
        0 if successful, 1 if an I/O error persists after one retry.
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


def move(path1: str, path2: str) -> None:
    """Move a file from path1 to path2.

    Args:
        path1: Source path.
        path2: Destination path.

    Raises:
        NoSuchFile: If the source path does not exist.
        FileHandlingFailure: If the move operation fails.
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


def copy(path1: str, path2: str) -> None:
    """Copy path1 to path2.

    Args:
        path1: Source file path.
        path2: Destination file path.

    Raises:
        NoSuchFile: If the source path does not exist.
        FileHandlingFailure: If the copy operation fails.
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


def add_to_total_size(path: str, total_size: int) -> int:
    """Add the size of the file at the given path to a running total.

    Args:
        path: Path to the file whose size should be added.
        total_size: Current running total of all input/output file sizes in bytes.

    Returns:
        Updated total size in bytes.
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


def get_local_file_size(filename: str) -> Optional[int]:
    """Get the size of a local file in bytes.

    Args:
        filename: Path to the file.

    Returns:
        File size in bytes, or None if the file does not exist or the size
        cannot be determined.
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


def get_guid() -> str:
    """Generate a GUID using the uuid library.

    Example::

        get_guid()  # -> '92008FAF-BE4C-49CF-9C5C-E12BC74ACD19'

    Returns:
        A random GUID string in uppercase.
    """
    return str(uuid.uuid4()).upper()


def get_table_from_file(filename: str, header: str = "", separator: str = "\t", convert_to_float: bool = True) -> dict:
    """Extract a table of data from a text file into a dictionary.

    The header defines the column names (either provided explicitly or read
    from the first line of the file). Each column becomes a key in the returned
    dictionary whose value is a list of row entries.

    Example header::

        "Time VMEM PSS RSS Swap rchar wchar rbytes wbytes"

    Output format::

        {'Time': [...], 'VMEM': [...], ...}

    Args:
        filename: Full path to the input text file.
        header: Header string defining column names. If empty, the first line
            of the file is used as the header.
        separator: Column separator character.
        convert_to_float: If True, all data values are converted to floats.

    Returns:
        Dictionary mapping column names to lists of row values, or None if
        float conversion fails.
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


def _define_tabledict_keys(header: str, fields: str, separator: str) -> tuple[dict, list]:
    """Define the keys for the tabledict dictionary.

    Note: this function is only used by ``get_table_from_file()``.

    Args:
        header: Header string defining column names. If empty, column names
            are taken from ``fields``.
        fields: List of fields parsed from the file header line.
        separator: Separator character used between column names in ``header``.

    Returns:
        A tuple of (tabledict, keylist) where tabledict maps each column name
        to an empty list and keylist is the ordered list of column names.
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


def calculate_checksum(filename: str, algorithm: str = "adler32") -> str:
    """Calculate the checksum value for the given file.

    The default algorithm is adler32. Valid algorithm identifiers:

    - adler32: ``'adler32'``, ``'adler'``, ``'ad32'``, ``'ad'``
    - md5: ``'md5'``, ``'md5sum'``, ``'md'``

    Args:
        filename: Path to the file.
        algorithm: Checksum algorithm identifier string.

    Raises:
        FileHandlingFailure: If the file does not exist.
        NotImplementedError: If the algorithm is not recognised.

    Returns:
        Checksum value string.
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


def calculate_adler32_checksum(filename: str) -> str:
    """Calculate the adler32 checksum for the given file.

    An Adler-32 checksum is obtained by calculating two 16-bit checksums A and
    B and concatenating their bits into a 32-bit integer. A is the sum of all
    bytes in the stream plus one, and B is the sum of the individual values of
    A from each step.

    Args:
        filename: Path to the file.

    Raises:
        Exception: If the checksum cannot be computed even after a fallback
            attempt.

    Returns:
        Hexadecimal checksum string, zero-padded to 8 characters.
    """
    # adler starting value is _not_ 0
    adler = 1

    try:
        with open(filename, 'rb') as _file:
            _mm = mmap(_file.fileno(), 0, access=ACCESS_READ)
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
    return f"{adler:08x}"


def calculate_md5_checksum(filename: str) -> str:
    """Calculate the md5 checksum for the given file.

    The file is assumed to exist.

    Args:
        filename: Path to the file.

    Returns:
        Hexadecimal md5 checksum string.
    """
    length = io.DEFAULT_BUFFER_SIZE
    md5 = hashlib.md5()

    with io.open(filename, mode="rb") as _fd:
        for chunk in iter(lambda: _fd.read(length), b''):
            md5.update(chunk)

    return md5.hexdigest()


def get_checksum_value(checksum: Any) -> str:
    """Return the actual checksum value from the full checksum object.

    The given checksum may be a plain ad32/md5 string, or a dictionary with
    the format ``{checksum_type: value}`` as defined in the ``FileSpec`` class.
    This function extracts the checksum value from the dictionary, or returns
    the string directly if it is already a plain value.

    Args:
        checksum: Checksum string or ``{type: value}`` dictionary.

    Returns:
        Checksum value string.
    """
    if isinstance(checksum, str):
        return checksum

    checksum_value = ''
    checksum_type = get_checksum_type(checksum)

    if isinstance(checksum, dict):
        checksum_value = checksum.get(checksum_type)

    return checksum_value


def get_checksum_type(checksum: Any) -> str:
    """Return the checksum type (``'ad32'`` or ``'md5'``).

    The given checksum can be a plain ad32/md5 value string, or a dictionary
    with the format ``{checksum_type: value}`` as defined in the ``FileSpec``
    class. Returns ``'unknown'`` if the type cannot be identified.

    Args:
        checksum: Checksum string or ``{type: value}`` dictionary.

    Returns:
        Checksum type string: ``'ad32'``, ``'md5'``, or ``'unknown'``.
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


def scan_file(path: str, error_messages: list, warning_message: str = "") -> bool:
    """Scan the given file for known error messages.

    Args:
        path: Path to the file to scan.
        error_messages: List of error message patterns to search for.
        warning_message: Optional warning message to log if any pattern is found.

    Returns:
        True if any error message was found, False otherwise.
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


def verify_file_list(list_of_files: list) -> list:
    """Return only the files from the given list that actually exist on disk.

    Args:
        list_of_files: List of file paths to check.

    Returns:
        Filtered list containing only existing file paths.
    """
    # remove any non-existent files from the input file list
    filtered_list = [f for f in list_of_files if os.path.exists(f)]

    diff = diff_lists(list_of_files, filtered_list)
    if diff:
        logger.debug(f'found {len(diff)} file(s) that do not exist (e.g. {diff[0]})')

    return filtered_list


def find_latest_modified_file(list_of_files: list) -> tuple[Optional[str], Optional[int]]:
    """Find the most recently modified file among the given list.

    If the ``getmtime()`` int conversion fails, mtime is set to None.

    Args:
        list_of_files: List of file paths to check.

    Returns:
        A tuple of (most_recently_updated_file, modification_time). Both
        elements are None if the list is empty or an error occurs.
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


def get_modification_time(path: str) -> Optional[int]:
    """Get the modification time for the given file.

    Args:
        path: File path.

    Returns:
        Modification time as a Unix timestamp integer, or None if the file
        does not exist or the time cannot be read.
    """
    mtime = None
    if os.path.exists(path):
        try:
            mtime = int(os.path.getmtime(path))
        except OSError as exc:
            logger.warning(f"int conversion failed for mod time: {exc}")

    return mtime


def list_mod_files(file_list: list) -> None:
    """Log file names along with their modification times.

    Called before a looping killer is executed.

    Args:
        file_list: List of file paths with full paths.
    """
    if file_list:
        logger.info('dumping info for recently modified files prior to looping job kill')
        for _file in file_list:
            try:
                size = int(os.path.getmtime(_file))
            except Exception as exc:
                size = f'unknown (exc={exc})'
            logger.info(f'file name={_file} : mod_time={size}')


def dump(path: str, cmd: str = "cat") -> None:
    """Dump the content of the file at the given path to the log.

    Args:
        path: File path to dump.
        cmd: Shell command used to read the file (default ``'cat'``).
    """
    if os.path.exists(path) or cmd == "echo":
        _cmd = f"{cmd} {path}"
        _, stdout, stderr = execute(_cmd)
        logger.info(f"{_cmd}:\n{stdout + stderr}")
    else:
        logger.info(f"path {path} does not exist")


def remove_core_dumps(workdir: str, pid: int = 0) -> bool:
    """Remove any remaining core dumps so they do not end up in the log tarball.

    A core dump from the payload process should not be deleted if the pilot is
    in debug mode (checked by the caller). A core dump from a non-payload
    process will be removed but causes the function to return False.

    Args:
        workdir: Working directory for the payload.
        pid: Payload process id. If non-zero, the function checks for a core
            file named ``core.<pid>``.

    Returns:
        True if a payload core dump is found, False otherwise.
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


def get_nonexistant_path(fname_path: str) -> str:
    """Return a file path that does not yet exist by appending an index if necessary.

    Args:
        fname_path: Desired file path.

    Returns:
        The original path if it does not exist, otherwise a path with an
        incrementing index appended (e.g. ``'out-1.txt'``, ``'out-2.txt'``).
    """
    if not os.path.exists(fname_path):
        return fname_path
    filename, file_extension = os.path.splitext(fname_path)
    i = 1
    new_fname = f"{filename}-{i}{file_extension}"
    while os.path.exists(new_fname):
        i += 1
        new_fname = f"{filename}-{i}{file_extension}"
    return new_fname


def update_extension(path: str = "", extension: str = "") -> str:
    """Update the file name extension to the given extension.

    Args:
        path: File path whose extension will be replaced.
        extension: New extension string (with or without a leading dot).

    Returns:
        File path with the updated extension.
    """
    path, _ = os.path.splitext(path)
    if not extension.startswith('.'):
        extension = '.' + extension
    path += extension

    return path


def get_valid_path_from_list(paths: list) -> Optional[str]:
    """Return the first valid (existing) path from the given list.

    Args:
        paths: List of file paths to check.

    Returns:
        First path that exists on disk, or None if none exist.
    """
    valid_path = None
    for path in paths:
        if os.path.exists(path):
            valid_path = path
            break

    return valid_path


def copy_pilot_source(workdir: str, filename: str = "") -> str:
    """Copy the pilot source into the work directory.

    If a filename is specified, only that file is copied.

    Args:
        workdir: Destination working directory.
        filename: Specific filename to copy. If empty, the entire pilot3
            source directory is copied.

    Returns:
        Diagnostics string (empty if successful).
    """
    diagnostics = ""
    srcdir = os.path.join(os.environ.get('PILOT_SOURCE_DIR', '.'), 'pilot3')

    if filename:
        srcdir = os.path.join(srcdir, filename)

    try:
        logger.debug(f'copy {srcdir} to {workdir}')
        # replace with:
        # pat = f"{filename}" if filename else f"{filename}/*"
        # cmd = f"cp -pr {pat} {srcdir} {workdir}"
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


def create_symlink(from_path: str = "", to_path: str = "") -> None:
    """Create a symlink from ``from_path`` to ``to_path``.

    Args:
        from_path: Source path for the symlink.
        to_path: Destination path for the symlink.
    """
    try:
        os.symlink(from_path, to_path)
    except (OSError, FileNotFoundError) as exc:
        logger.warning(f'failed to create symlink from {from_path} to {to_path}: {exc}')
    else:
        logger.debug(f'created symlink from {from_path} to {to_path}')


def locate_file(pattern: str) -> Optional[str]:
    """Locate a file defined by the given glob pattern.

    Example::

        locate_file(os.path.join(os.getcwd(), '**/core.123'))
        # -> '/Users/Paul/Development/python/tt/core.123'

    Args:
        pattern: Glob pattern used to search for the file.

    Returns:
        Path to the located file, or None if no match was found.
    """
    path = None
    for fname in glob(pattern):
        if os.path.isfile(fname):
            path = fname

    return path


def find_last_line(filename: str) -> str:
    """Find the last line in a file.

    Note: the entire file is read into memory, so this is not suitable for
    very large files.

    Args:
        filename: Full path to the file.

    Returns:
        Last line of the file, or an empty string if the file is empty.
    """
    last_line = ""
    with open(filename) as _file:
        line = ""
        for line in _file:
            pass
        if line:
            last_line = line

    return last_line


def get_disk_usage(start_path: str = ".") -> int:
    """Calculate the disk usage of the given directory including sub-directories.

    Args:
        start_path: Root directory to measure.

    Returns:
        Total disk usage in bytes.
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


def extract_lines_from_file(pattern: str, filename: str) -> str:
    """Extract all lines containing the given pattern from a file.

    Args:
        pattern: Text substring to search for.
        filename: Path to the file to search.

    Returns:
        Concatenated matching lines as a single string.
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


def find_file(filename: str, startdir: str) -> Optional[str]:
    """Locate a file in a sub-directory of the given start directory.

    Args:
        filename: File name to search for.
        startdir: Root directory to start the recursive search from.

    Returns:
        Full POSIX path to the first matching file, or None if not found.
    """
    logger.debug(f'looking for {filename} in start dir {startdir}')
    _path = None
    for path in Path(startdir).rglob(filename):
        logger.debug(f'located file at: {path}')
        _path = path.as_posix()
        break

    return _path


def zip_files(archivename: str, files: list) -> bool:
    """Compress a list of files into a zip archive.

    Args:
        archivename: Path to the output zip archive.
        files: List of file paths to compress.

    Returns:
        True if at least one file was successfully added to the archive,
        False otherwise.
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


def generate_test_file(filename: str, filesize: int = 1024) -> None:
    """Generate a binary file filled with random data.

    Args:
        filename: Full path and name of the file to create.
        filesize: Size of the file in bytes (default 1024).
    """
    with open(filename, 'wb') as fout:
        fout.write(os.urandom(filesize))  # replace 1024 with a size in kilobytes if it is not unreasonably large


def get_directory_size(directory: str) -> Optional[float]:
    """Measure the size of the given directory.

    Args:
        directory: Full directory path.

    Returns:
        Directory size in MB, or None if measurement fails.
    """

    size_mb = None
    try:
        size_mb = get_disk_usage(directory) / 1024 / 1024
    except Exception as exc:
        logger.warning(f'failed to get directory size: {exc}')

    return size_mb


def old_get_directory_size(directory: str) -> Optional[float]:
    """Measure the size of the given directory using ``du -sh``.

    Returns None in case of failure.

    Args:
        directory: Full directory path.

    Returns:
        Directory size in MB, or None if measurement fails.
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
    """Calculate the total input file size.

    Args:
        files: Iterable of ``FileSpec`` objects.
        nolib: If True, exclude files whose LFN contains ``'.lib.'``
            (default True).

    Returns:
        Total input file size in bytes.
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
    """Append the contents of one file to another.

    Args:
        from_file: Path to the source file to read from.
        to_file: Path to the target file to append to.

    Returns:
        True if the operation was successful, False otherwise.
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


def rename_xrdlog(name: str) -> None:
    """Rename the xroot client logfile if it was created.

    Args:
        name: Local file name suffix used when renaming the log file.
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


def rename(from_name: str, to_name: str) -> bool:
    """Rename a file from one name to another.

    Args:
        from_name: The original file name/path.
        to_name: The new file name/path.

    Returns:
        True if the rename was successful, False otherwise.
    """
    status = False
    try:
        os.rename(from_name, to_name)
        status = True
    except FileNotFoundError as exc:
        logger.warning(f"file not found: {exc}")
    except IOError as exc:
        logger.warning(f"an error occurred while processing the file: {exc}")

    return status


def find_files_with_pattern(directory: str, pattern: str) -> list:
    """Find files in a directory that match a specified pattern.

    Args:
        directory: Directory to search for files.
        pattern: Glob pattern to match against file names.

    Returns:
        List of matching file names found in the directory (empty list on
        error).
    """
    try:
        if not os.path.exists(directory):
            raise FileNotFoundError(f"directory '{directory}' does not exist")

        # return all matching files
        return [f for f in os.listdir(directory) if fnmatch.fnmatch(f, pattern)]
    except (FileNotFoundError, PermissionError) as exc:
        logger.warning(f"exception caught while finding files: {exc}")
        return []
