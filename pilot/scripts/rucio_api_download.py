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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-23

# This script demonstrates how to download a file using the Rucio download client.
# Note: Rucio needs to be setup with 'lsetup rucio'.

try:
    from rucio.client.downloadclient import DownloadClient
except Exception:
    print("Rucio client has not been setup, please run \'lsetup rucio\' first")
else:
    f_ific = {'did_scope': 'mc16_13TeV', 'did': 'mc16_13TeV:EVNT.16337107._000147.pool.root.1',
              'rse': 'IFIC-LCG2_DATADISK',  # Python 2 - is unicode necessary for the 'rse' value? was u'IFIC-LCG2_DATADISK'
              'pfn': 'root://t2fax.ific.uv.es:1094//lustre/ific.uv.es/grid/atlas/atlasdatadisk/rucio/mc16_13TeV/59/29/EVNT.16337107._000147.pool.root.1',
              'did_name': 'EVNT.16337107._000147.pool.root.1', 'transfer_timeout': 3981, 'base_dir': '.'}

    download_client = DownloadClient()
    print(download_client.download_pfns([f_ific], 1))
