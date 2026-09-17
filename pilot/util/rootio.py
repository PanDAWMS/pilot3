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
# - Paul Nilsson, paul.nilsson@cern.ch, 2026

"""Detection of local ROOT file write failures in payload stdout.

A payload can fail to write its output ROOT file because of a problem on the worker
node (failing local disk, full scratch area, stale NFS handle) rather than because of
anything the user code did. ROOT reports this on stdout but the transform frequently
does not translate it into a non-zero exit code, so the job can be reported as
successful while its output file is truncated.

The detection implemented here is shared by all experiment plugins, since nothing about
it is ATLAS specific.

Measured behaviour of ROOT (verified against the ROOT sources, not inferred)::

    io/io/src/TFile.cxx:1172   TFile::Flush()
                               SysSync(fD) < 0
                                   -> SetBit(kWriteError); SetWritable(kFALSE);
                                   -> SysError("Flush", "error flushing file %s", GetName())
    io/io/src/TFile.cxx:2562   SysError("WriteBuffer", "error writing to file %s (%ld)")
    io/io/src/TFile.cxx:2567   Error("WriteBuffer", "error writing all requested bytes "
                                                    "to file %s, wrote %ld of %d")
    tree/tree/src/TBasket.cxx:1218   TBasket::WriteBuffer()
                                     if (!file->IsWritable()) return -1;
    tree/tree/src/TBranch.cxx:3238   Error("WriteBasketImpl", "basket's WriteBuffer failed.")

``SysError`` appends ``(strerror(errno))`` to the message, which is where the
``(Input/output error)`` seen in real payload logs comes from.

This has two consequences for the pattern design:

1. ``TFile::Flush`` latches the file unwritable, so every subsequent basket write fails.
   The long cascade of ``basket's WriteBuffer failed.`` lines is therefore a *symptom*
   and not the cause, and it is not by itself proof of a node I/O problem: the same
   ``return -1`` is reached when the compressed buffer cannot be allocated, which is a
   memory problem that belongs to the out-of-memory classification path. Only the
   TFile-level line is used as the trigger; the cascade is counted and logged as
   corroboration.
2. The system error is written only once per file, so there is exactly one line carrying
   the errno text. That line is also the only one naming the file, which makes it the
   right choice for the error diagnostics.

Raw ROOT prints ``SysError in <TFile::Flush>: error flushing file ...``. Athena routes
the ROOT error handler through its message service, which reformats the same error as
``TFile::Flush   ERROR  error flushing file ...`` and truncates the originating method
name to 22 characters (this is why real logs show ``TBranchElement::WriteB...``). The
patterns below therefore anchor on the message text plus a ``TFile::`` prefix, which
survives both renderings, and never on the full method name.
"""

from __future__ import annotations

import logging
import os
import re

from pilot.common.errorcodes import ErrorCodes
from pilot.util.config import config
from pilot.util.filehandling import grep

logger = logging.getLogger(__name__)
errors = ErrorCodes()

# TFile-level write failures. These are the only patterns allowed to trigger the error;
# each one is emitted by ROOT at the point where a write or flush syscall actually failed.
# 'TFile::' is required in the same line so that the generic English message text cannot
# match unrelated payload output.
_ROOT_WRITE_PRIMARY_PATTERNS: list[str] = [
    r'TFile::.*error flushing file',
    r'TFile::.*error writing to file',
    r'TFile::.*error writing all requested bytes to file',
]

# Downstream symptoms of a file that has already been latched unwritable. Counted and
# logged for context only - never used to trigger the error on their own, since the same
# messages are produced by a compressed-buffer allocation failure.
_ROOT_WRITE_CASCADE_PATTERNS: list[str] = [
    r"basket's WriteBuffer failed",
    r'Failed to write out basket',
    r'Failed filling branch:',
]

# strerror() text appearing in the primary line, mapped to the pilot error code that
# describes the underlying condition. Anything not listed here is a generic write failure.
_ERRNO_TEXT_TO_ERROR_CODE: dict[str, int] = {
    'No space left on device': errors.NOLOCALSPACE,
    'Disk quota exceeded': errors.NOLOCALSPACE,
}

# Maximum length of the diagnostics string handed to add_error_code(). The PanDA monitor
# truncates at 256 characters including the standard error message, so the raw log line is
# capped well below that.
_MAX_DIAGNOSTICS_LENGTH = 200


def get_root_write_error(stdout_path: str) -> tuple[int, str]:
    """Scan a payload stdout file for a local ROOT file write failure.

    Only a TFile-level failure (flush or write) counts as a trigger. Cascading
    ``basket's WriteBuffer failed.`` messages are counted and logged but never cause
    an error on their own, because they are also produced by a compressed-buffer
    allocation failure, which is a memory problem rather than an I/O problem.

    When the errno text carried by the primary line indicates a full or over-quota
    file system, ``NOLOCALSPACE`` is returned instead of ``PAYLOADWRITEFAILURE`` so
    that the reported error names the actual condition.

    Args:
        stdout_path: Path to the payload stdout file.

    Returns:
        tuple: (error_code, diagnostics) where error_code is ``NOLOCALSPACE`` or
        ``PAYLOADWRITEFAILURE`` and diagnostics is the first TFile-level error line,
        stripped and truncated. (0, "") is returned when the file does not exist,
        cannot be read, or contains no TFile-level write failure.
    """
    if not os.path.exists(stdout_path):
        logger.warning(f'payload stdout does not exist, cannot scan for ROOT write errors: {stdout_path}')
        return 0, ""

    try:
        primary_lines = grep(_ROOT_WRITE_PRIMARY_PATTERNS, stdout_path)
    except OSError as exc:
        logger.warning(f'failed to read {stdout_path} while scanning for ROOT write errors: {exc}')
        return 0, ""

    if not primary_lines:
        return 0, ""

    diagnostics = primary_lines[0].strip()[:_MAX_DIAGNOSTICS_LENGTH]
    logger.warning('detected a local ROOT file write failure in payload stdout:')
    for line in primary_lines[:5]:  # cap output to avoid flooding the pilot log
        logger.warning(f'  {line.rstrip()}')

    # count the downstream symptoms purely to show the scale of the damage in the log
    try:
        cascade_lines = grep(_ROOT_WRITE_CASCADE_PATTERNS, stdout_path)
    except OSError as exc:
        logger.warning(f'failed to count ROOT write error cascade lines: {exc} (ignoring)')
    else:
        if cascade_lines:
            logger.warning(
                f'{len(cascade_lines)} subsequent basket/branch write failure(s) followed - '
                f'the output file is truncated'
            )

    error_code = _resolve_error_code(diagnostics)

    return error_code, diagnostics


def _resolve_error_code(diagnostics: str) -> int:
    """Map the errno text carried by a TFile error line to a pilot error code.

    Args:
        diagnostics: The TFile-level error line.

    Returns:
        int: ``NOLOCALSPACE`` for a full or over-quota file system, otherwise
        ``PAYLOADWRITEFAILURE``.
    """
    for errno_text, error_code in _ERRNO_TEXT_TO_ERROR_CODE.items():
        if re.search(re.escape(errno_text), diagnostics):
            logger.warning(f"ROOT write failure reports '{errno_text}' - "
                           f"mapping to {errors.get_error_name(error_code)}")
            return error_code

    return errors.PAYLOADWRITEFAILURE


def check_root_write_error(job: object) -> bool:
    """Set a pilot error code if the payload failed to write its output ROOT file.

    This check is deliberately not gated on the payload exit code. A ROOT write failure
    is not the result of a direct call from user code, so the transform regularly exits
    zero after the output file has already been truncated. Failing the job is the correct
    outcome: the file that would otherwise be staged out is incomplete.

    Args:
        job: Job object with ``workdir`` set.

    Returns:
        bool: True if a write failure was found and an error code was set.
    """
    stdout_path = os.path.join(job.workdir, config.Payload.payloadstdout)
    error_code, diagnostics = get_root_write_error(stdout_path)
    if not error_code:
        return False

    _log_affected_output_file(job, diagnostics)

    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(
        error_code, priority=True, msg=diagnostics
    )

    return True


def _log_affected_output_file(job: object, diagnostics: str) -> None:
    """Report which declared output file the failed write refers to, if any.

    The TFile error line names the file ROOT was writing. Matching it against the job's
    declared output makes the log state plainly that a file the pilot was about to stage
    out is the one that was lost.

    Args:
        job: Job object whose outdata may be inspected.
        diagnostics: The TFile-level error line.
    """
    outdata = getattr(job, 'outdata', None)
    if not outdata:
        return

    try:
        lfns = [fspec.lfn for fspec in outdata if getattr(fspec, 'lfn', None)]
    except TypeError:
        return

    for lfn in lfns:
        if lfn and lfn in diagnostics:
            logger.warning(f'the failed write concerns declared output file {lfn} - it will be incomplete')
            return
