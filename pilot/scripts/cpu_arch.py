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
# - Paul Nilsson, paul.nilsson@cern.ch, 2023

import argparse
import logging
import re

must_v4 = []
must_not_v4 = []
must_v3 = []
must_not_v3 = []
must_v2 = []
must_not_v2 = []


def get_flags_cpuinfo():
    """
    Get the CPU (model) name, number of cores of the corresponding CPU and the CPU flags from the /proc/cpuinfo
    """
    cpu, cpu_core, flags = None, None, None
    with open('/proc/cpuinfo', 'r') as fiile:
        for line in fiile.readlines():
            if 'model name' in line:
                cpu = line.split(':')[-1].strip()
            if 'cpu cores' in line:
                cpu_core = line.split(':')[-1].strip()
            if 'flags' in line:
                flags = line.split(':')[-1].strip()
            if all([cpu, cpu_core, flags]):
                return {"cpu": cpu, "cpu_core": cpu_core, "flags": flags}


def get_flags_pilotlog(pilotlogname):
    """
    Get the site/queue name, the CPU (model) name, number of cores of the corresponding CPU and the CPU flags from the downloaded pilotlog
    """
    site, cpu, cpu_core, flags = None, None, None, None
    with open(pilotlogname, 'r') as fiile:
        for line in fiile.readlines():
            if 'PANDA_RESOURCE' in line:
                site = line.split('=')[-1].strip()
            if 'model name' in line:
                cpu = line.split(':')[-1].strip()
            if 'coreCount' in line:
                cpu_core = line.split(':')[-1].strip()
            if 'flags' in line:
                flags = line.split(':')[-1].strip()
            if all([site, cpu, cpu_core, flags]):
                return {"site": site, "cpu": cpu, "cpu_core": cpu_core, "flags": flags}


def set_naive():
    """
    Make a decision on the CPU architecture based on the simplified lists (must_'s) of flags
    The must_not_'s have been left blank, these could be filled if need be
    """
    global must_v4
    global must_not_v4
    global must_v3
    global must_not_v3
    global must_v2
    global must_not_v2

    must_v4 = [r'AVX512.*']
    must_not_v4 = []

    must_v3 = [r'AVX2.*']
    must_not_v3 = []

    must_v2 = [r'SSE4_2.*']
    must_not_v2 = []


def set_gcc():
    """
    Make a decision on the CPU architecture based on the modified lists (must_'s) of flags from gcc: LAHF_SAHF --> LAHF_LM; LZCNT --> ABM; removal of SSE3
    References:
        https://gcc.gnu.org/git/?p=gcc.git;a=blob_plain;f=gcc/testsuite/gcc.target/i386/x86-64-v4.c;hb=324bec558e95584e8c1997575ae9d75978af59f1
        https://gcc.gnu.org/git/?p=gcc.git;a=blob_plain;f=gcc/testsuite/gcc.target/i386/x86-64-v3.c;hb=324bec558e95584e8c1997575ae9d75978af59f1
        https://gcc.gnu.org/git/?p=gcc.git;a=blob_plain;f=gcc/testsuite/gcc.target/i386/x86-64-v2.c;hb=324bec558e95584e8c1997575ae9d75978af59f1

    The must_not_'s have been left blank, these could be filled if need be
    """
    global must_v4
    global must_not_v4
    global must_v3
    global must_not_v3
    global must_v2
    global must_not_v2

    must_v4 = [r'MMX', r'SSE', r'SSE2', r'LAHF_LM', r'POPCNT', r'SSE4_1', r'SSE4_2', r'SSSE3', r'AVX', r'AVX2', r'F16C',
               r'FMA', r'ABM', r'MOVBE', r'XSAVE', r'AVX512F', r'AVX512BW', r'AVX512CD', r'AVX512DQ', r'AVX512VL']
    must_not_v4 = []

    must_v3 = [r'MMX', r'SSE', r'SSE2', r'LAHF_LM', r'POPCNT', r'SSE4_1', r'SSE4_2', r'SSSE3', r'AVX', r'AVX2', r'F16C',
               r'FMA', r'ABM', r'MOVBE', r'XSAVE']
    must_not_v3 = []

    must_v2 = [r'MMX', r'SSE', r'SSE2', r'LAHF_LM', r'POPCNT', r'SSE4_1', r'SSE4_2', r'SSSE3']
    must_not_v2 = []


def check_flags(must, must_not, flags):
    """
    Matching of the actual CPU flags w.r.t. the lists of flags defined for deciding on architecture
    """
    failed = False
    for flag in must:
        if not any([re.match(flag, test_flag, re.IGNORECASE) for test_flag in flags]):
            logging.debug("Missing must-have: {0}".format(flag))
            failed = True
    for flag in must_not:
        if not any([re.match(flag, test_flag, re.IGNORECASE) for test_flag in flags]):
            logging.debug("Present must-not-have: {0}".format(flag))
            failed = True
    return failed


def all_version_checks(flag_string, name):
    """
    Architecture is assigned to the CPU based on the check_flags() function
    """
    flag_list = flag_string.split()
    logging.debug("-------Checking V4 for {0}--------".format(name))
    failed_v4 = check_flags(must_v4, must_not_v4, flag_list)
    if not failed_v4:
        return "x86-64-v4"
    else:
        pass
    logging.debug("-------Checking V3 for {0}--------".format(name))
    failed_v3 = check_flags(must_v3, must_not_v3, flag_list)
    if not failed_v3:
        return "x86-64-v3"
    else:
        pass
    logging.debug("-------Checking V2 for {0}--------".format(name))
    failed_v2 = check_flags(must_v2, must_not_v2, flag_list)
    if not failed_v2:
        return "x86-64-v2"
    else:
        pass
    logging.debug("-------Defaulting {0} to V1--------".format(name))
    if failed_v2 and failed_v3 and failed_v4:
        return "x86-64-v1"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--logpath", default=None, type=str, help="Enter the full path to pilotlog")
    parser.add_argument("--alg", default="naive", choices=["naive", "gcc"], help="algorithm type")
    parser.add_argument("-d", "--debug", help="Enable additional logging", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="CPUFLAGS-%(asctime)s-%(process)d-%(levelname)s-%(message)s",
                        )

    if args.alg == "naive":
        set_naive()
    elif args.alg == "gcc":
        set_gcc()
    else:
        raise RuntimeError("Invalid option specified")

    if args.logpath is not None:
        pilotlog = args.logpath
        loginfo = get_flags_pilotlog(pilotlog)
        arch_pilotlog = all_version_checks(loginfo["flags"], loginfo["cpu"])
        print(arch_pilotlog)
    else:
        cpuinfo = get_flags_cpuinfo()
        arch_cpuinfo = all_version_checks(cpuinfo["flags"], cpuinfo["cpu"])
        print(arch_cpuinfo)
