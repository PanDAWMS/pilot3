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
# - Fernando Barreiro Megino, fernando.harald.barreiro.megino@cern.ch, 2019
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-23

"""Setup module for pip installation."""

import sys

from setuptools import setup, find_packages

sys.path.insert(0, '.')

# get release version
with open('PILOTVERSION') as reader:
    release_version = reader.read()

setup(
    name="panda-pilot",
    version=release_version,
    description='PanDA Pilot 3',
    long_description='''This package contains the PanDA Pilot 3 source code''',
    license='Apache License 2.0',
    author='PanDA Team',
    author_email='atlas-adc-panda@cern.ch',
    url='https://github.com/PanDAWMS/pilot3/wiki',
    python_requires='>=3.6',
    packages=find_packages(),
    install_requires=[],
    data_files=[],
    package_data={'': ['PILOTVERSION']},
    include_package_data=True,
    scripts=[]
)
