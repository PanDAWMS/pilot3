#!/usr/bin/env python3
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
# - Alaettin Serhan Mete, alaettin.serhan.mete@cern.ch, 2023
# - Paul Nilsson, paul.nilsson@cern.ch, 2023-2026

"""CPU architecture detection utility for x86-64 and ARM platforms.

Classifies CPUs into architecture tiers by analysing flags from
/proc/cpuinfo or ATLAS pilot log files:

  - x86-64: v1 through v4 based on SIMD feature support.
  - ARM:    ARMv8 based on feature flags.

Typical usage::

    python3 cpu_arch.py
    python3 cpu_arch.py --logpath /path/to/pilotlog
    python3 cpu_arch.py --alg x86naive -d
"""

from __future__ import annotations

import argparse
import logging
import re
import subprocess
from dataclasses import dataclass, field


@dataclass
class FlagSpec:
    """Container for CPU architecture flag requirements.

    Attributes:
        must: Regex patterns that must match at least one CPU flag.
        must_not: Regex patterns that must not match any CPU flag.
    """

    must: list[str] = field(default_factory=list)
    must_not: list[str] = field(default_factory=list)


@dataclass
class X86FlagSpecs:
    """Flag requirements for all x86-64 architecture levels.

    Attributes:
        v4: Requirements for x86-64-v4.
        v3: Requirements for x86-64-v3.
        v2: Requirements for x86-64-v2.
    """

    v4: FlagSpec = field(default_factory=FlagSpec)
    v3: FlagSpec = field(default_factory=FlagSpec)
    v2: FlagSpec = field(default_factory=FlagSpec)


def get_osarch() -> str:
    """Return a normalised OS architecture string.

    Runs ``uname -m`` and maps the result to one of ``"x86-64"``,
    ``"arm"``, or ``"unknown"``.

    Returns:
        A string identifying the CPU family: ``"x86-64"``, ``"arm"``,
        or ``"unknown"`` if the value returned by ``uname`` is not
        recognised.

    References:
        https://stackoverflow.com/questions/45125516/possible-values-for-uname-m
        https://unix.stackexchange.com/questions/711432/uname-m-valid-values
    """
    result = subprocess.run(
        ["uname", "-m"],
        stdout=subprocess.PIPE,
        universal_newlines=True,
        check=False,
    )
    osarch = result.stdout.strip().lower()

    if any(re.match(p, osarch) for p in (r"x86_64.*",)):
        return "x86-64"
    if any(re.match(p, osarch) for p in (r"arm.*", r"aarch64.*")):
        return "arm"

    logging.error("No matching osarch for %s", osarch)
    return "unknown"


def check_flags(spec: FlagSpec, flags: list[str]) -> bool:
    """Check CPU flags against a :class:`FlagSpec`.

    Each pattern in ``spec.must`` and ``spec.must_not`` is treated as a
    case-insensitive regular expression matched against every element of
    *flags*.

    Args:
        spec: Required and prohibited flag patterns.
        flags: List of CPU feature flags to test.

    Returns:
        ``True`` if any required flag is absent or any prohibited flag is
        present; ``False`` if all checks pass.
    """
    failed = False

    for pattern in spec.must:
        if not any(re.match(pattern, f, re.IGNORECASE) for f in flags):
            logging.debug("Missing must-have: %s", pattern)
            failed = True

    for pattern in spec.must_not:
        if any(re.match(pattern, f, re.IGNORECASE) for f in flags):
            logging.debug("Present must-not-have: %s", pattern)
            failed = True

    return failed


def get_flags_x86() -> dict[str, str] | None:
    """Read CPU model, core count, and flags from ``/proc/cpuinfo``.

    Reads the first complete set of ``model name``, ``cpu cores``, and
    ``flags`` fields found in ``/proc/cpuinfo``.

    Returns:
        A dict with keys ``"cpu"``, ``"cpu_core"``, and ``"flags"``, or
        ``None`` if the required fields are not all present.
    """
    cpu: str | None = None
    cpu_core: str | None = None
    flags: str | None = None

    with open("/proc/cpuinfo", encoding="utf-8") as fh:
        for line in fh:
            if "model name" in line:
                cpu = line.split(":")[-1].strip()
            elif "cpu cores" in line:
                cpu_core = line.split(":")[-1].strip()
            elif "flags" in line:
                flags = line.split(":")[-1].strip()
            if cpu and cpu_core and flags:
                return {"cpu": cpu, "cpu_core": cpu_core, "flags": flags}

    return None


def get_flags_pilotlog(pilotlogname: str) -> dict[str, str] | None:
    """Read site, CPU model, core count, and flags from an ATLAS pilot log.

    Extracts the first occurrence of ``PANDA_RESOURCE``, ``model name``,
    ``coreCount``, and ``flags`` from the file at *pilotlogname*.

    Args:
        pilotlogname: Path to the pilot log file.

    Returns:
        A dict with keys ``"site"``, ``"cpu"``, ``"cpu_core"``, and
        ``"flags"``, or ``None`` if any field is missing.
    """
    site: str | None = None
    cpu: str | None = None
    cpu_core: str | None = None
    flags: str | None = None

    with open(pilotlogname, encoding="utf-8") as fh:
        for line in fh:
            if "PANDA_RESOURCE" in line:
                site = line.split("=")[-1].strip()
            elif "model name" in line:
                cpu = line.split(":")[-1].strip()
            elif "coreCount" in line:
                cpu_core = line.split(":")[-1].strip()
            elif "flags" in line:
                flags = line.split(":")[-1].strip()
            if site and cpu and cpu_core and flags:
                return {
                    "site": site,
                    "cpu": cpu,
                    "cpu_core": cpu_core,
                    "flags": flags,
                }

    return None


def build_x86_specs() -> X86FlagSpecs:
    """Return x86-64 flag specs derived from GCC compiler requirements.

    Builds :class:`X86FlagSpecs` using GCC test-suite flag lists, with
    the following adjustments relative to the raw GCC definitions:

      - ``LAHF_SAHF`` → ``LAHF_LM``
      - ``LZCNT`` → ``ABM``
      - ``SSE3`` removed

    Returns:
        An :class:`X86FlagSpecs` instance with ``v4``, ``v3``, and ``v2``
        populated; ``must_not`` lists are empty.

    References:
        https://gcc.gnu.org/git/?p=gcc.git;a=blob_plain;f=gcc/testsuite/gcc.target/i386/x86-64-v4.c
        https://gcc.gnu.org/git/?p=gcc.git;a=blob_plain;f=gcc/testsuite/gcc.target/i386/x86-64-v3.c
        https://gcc.gnu.org/git/?p=gcc.git;a=blob_plain;f=gcc/testsuite/gcc.target/i386/x86-64-v2.c
    """
    return X86FlagSpecs(
        v4=FlagSpec(must=[
            r"MMX", r"SSE", r"SSE2", r"LAHF_LM", r"POPCNT",
            r"SSE4_1", r"SSE4_2", r"SSSE3", r"AVX", r"AVX2",
            r"F16C", r"FMA", r"ABM", r"MOVBE", r"XSAVE",
            r"AVX512F", r"AVX512BW", r"AVX512CD", r"AVX512DQ", r"AVX512VL",
        ]),
        v3=FlagSpec(must=[
            r"MMX", r"SSE", r"SSE2", r"LAHF_LM", r"POPCNT",
            r"SSE4_1", r"SSE4_2", r"SSSE3", r"AVX", r"AVX2",
            r"F16C", r"FMA", r"ABM", r"MOVBE", r"XSAVE",
        ]),
        v2=FlagSpec(must=[
            r"MMX", r"SSE", r"SSE2", r"LAHF_LM", r"POPCNT",
            r"SSE4_1", r"SSE4_2", r"SSSE3",
        ]),
    )


def build_x86naive_specs() -> X86FlagSpecs:
    """Return simplified x86-64 flag specs using top-level instruction sets.

    Builds :class:`X86FlagSpecs` with minimal representative patterns
    (``AVX512.*``, ``AVX2.*``, ``SSE4_2.*``).  ``must_not`` lists are
    empty.

    Returns:
        An :class:`X86FlagSpecs` instance with ``v4``, ``v3``, and ``v2``
        populated.
    """
    return X86FlagSpecs(
        v4=FlagSpec(must=[r"AVX512.*"]),
        v3=FlagSpec(must=[r"AVX2.*"]),
        v2=FlagSpec(must=[r"SSE4_2.*"]),
    )


def x86_checks(specs: X86FlagSpecs, flag_string: str, name: str) -> str:
    """Classify an x86-64 CPU into an architecture level.

    Tests *flag_string* hierarchically against v4, v3, and v2 requirements
    from *specs*, falling back to ``"x86-64-v1"`` if none match.

    Args:
        specs: Flag requirements for each x86-64 level.
        flag_string: Space-separated CPU flags (e.g. from ``/proc/cpuinfo``).
        name: Human-readable CPU name used in debug log messages.

    Returns:
        One of ``"x86-64-v4"``, ``"x86-64-v3"``, ``"x86-64-v2"``, or
        ``"x86-64-v1"``.
    """
    flag_list = flag_string.split()

    logging.debug("--- Checking V4 for %s ---", name)
    if not check_flags(specs.v4, flag_list):
        return "x86-64-v4"

    logging.debug("--- Checking V3 for %s ---", name)
    if not check_flags(specs.v3, flag_list):
        return "x86-64-v3"

    logging.debug("--- Checking V2 for %s ---", name)
    if not check_flags(specs.v2, flag_list):
        return "x86-64-v2"

    logging.debug("--- Defaulting %s to V1 ---", name)
    return "x86-64-v1"


def get_flags_arm() -> dict[str, str] | None:
    """Read CPU architecture and feature flags from ``/proc/cpuinfo``.

    Extracts the first ``CPU architecture`` and ``Features`` fields found
    in ``/proc/cpuinfo``.

    Returns:
        A dict with keys ``"cpu_arch"`` and ``"flags"``, or ``None`` if
        either field is absent.
    """
    cpu_arch: str | None = None
    flags: str | None = None

    with open("/proc/cpuinfo", encoding="utf-8") as fh:
        for line in fh:
            if "CPU architecture" in line:
                cpu_arch = line.split(":")[-1].strip()
            elif "Features" in line:
                flags = line.split(":")[-1].strip()
            if cpu_arch and flags:
                return {"cpu_arch": cpu_arch, "flags": flags}

    return None


def build_arm_spec() -> FlagSpec:
    """Return a :class:`FlagSpec` for ARMv8 detection.

    Uses only the two flags mandated by the ARMv8-A baseline ISA:
    ``FP`` (scalar floating-point) and ``ASIMD`` (Advanced SIMD).  All
    other ARMv8 feature flags are optional or were introduced in later
    sub-revisions and may be absent on conforming hardware.

    Returns:
        A :class:`FlagSpec` with ``must`` populated and ``must_not`` empty.

    References:
        https://en.wikipedia.org/wiki/ARM_architecture_family#Cores
        https://unix.stackexchange.com/questions/43539/what-do-the-flags-in-proc-cpuinfo-mean
    """
    return FlagSpec(must=[r"FP", r"ASIMD"])


def arm_checks(spec: FlagSpec, flag_string: str, name: str) -> str:
    """Classify an ARM CPU into an architecture level.

    Tests *flag_string* against ARMv8 requirements from *spec*.

    Args:
        spec: Flag requirements for ARMv8.
        flag_string: Space-separated CPU feature flags from ``/proc/cpuinfo``.
        name: Human-readable CPU architecture string used in debug messages.

    Returns:
        ``"ARMv8"`` if all required flags are present, otherwise
        ``"UNKNOWN"``.
    """
    flag_list = flag_string.split()
    logging.debug("--- Checking ARMv8 for %s ---", name)

    if not check_flags(spec, flag_list):
        return "ARMv8"
    return "UNKNOWN"


def main() -> None:
    """Parse arguments, detect the CPU architecture, and print the result."""
    parser = argparse.ArgumentParser(
        description=(
            "Detect CPU architecture level from /proc/cpuinfo or a pilot log."
        )
    )
    parser.add_argument(
        "--logpath",
        default=None,
        type=str,
        help="Full path to an ATLAS pilot log file.",
    )
    parser.add_argument(
        "--alg",
        default="x86",
        choices=["x86", "x86naive"],
        help="Flag-matching algorithm for x86-64 classification.",
    )
    parser.add_argument(
        "-d", "--debug",
        action="store_true",
        help="Enable debug logging.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="CPUFLAGS-%(asctime)s-%(process)d-%(levelname)s-%(message)s",
    )

    osarch = get_osarch()
    x86_specs = (
        build_x86_specs() if args.alg == "x86" else build_x86naive_specs()
    )

    if args.logpath is not None:
        loginfo = get_flags_pilotlog(args.logpath)
        if loginfo is None:
            logging.error(
                "Could not extract required fields from %s", args.logpath
            )
            print("UNKNOWN")
            return
        print(x86_checks(x86_specs, loginfo["flags"], loginfo["cpu"]))

    elif osarch == "arm":
        arm_spec = build_arm_spec()
        arminfo = get_flags_arm()
        if arminfo is None:
            logging.error("Could not extract ARM flags from /proc/cpuinfo")
            print("UNKNOWN")
            return
        print(arm_checks(arm_spec, arminfo["flags"], arminfo["cpu_arch"]))

    elif osarch == "x86-64":
        x86info = get_flags_x86()
        if x86info is None:
            logging.error("Could not extract x86 flags from /proc/cpuinfo")
            print("UNKNOWN")
            return
        print(x86_checks(x86_specs, x86info["flags"], x86info["cpu"]))

    else:
        logging.error("Neither x86-64 nor ARM - osarch=%s", osarch)
        print("UNKNOWN")


if __name__ == "__main__":
    main()
