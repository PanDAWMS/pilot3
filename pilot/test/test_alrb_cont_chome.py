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

"""Regression test for ALRB_CONT_CHOME not being set for the payload container.

Background: ALRB_CONT_CHOME redirects ALRB's per-job container temp files
(startContainer.sh.XXXXXX and the .alrb/container tree) into the job workdir
instead of $HOME/.alrb/container/scripts/, which is shared by every pilot on a
worker node and can accumulate hundreds of thousands of files on shared/NFS
$HOME setups. The export was added to create_root_container_command() (used
when opening a remote file for direct I/O) and to
create_middleware_container_command() (used for stage-in/stage-out), but was
never added to alrb_wrapper(), which builds the actual payload execution
command for ALRB/apptainer sites. As a result, a site with a shared $HOME saw
the fix apply to the open-file container but not to the payload container,
which kept writing into the shared .alrb/container/scripts/ directory.
"""

import os
import tempfile
import shutil
import unittest

from pilot.info.jobdata import JobData
from pilot.user.atlas.container import alrb_wrapper


class _FakeQueueData:
    """Minimal stand-in for infosys.queuedata."""

    def __init__(self):
        """Set the small set of attributes alrb_wrapper() reads from queuedata."""
        self.container_type = {'pilot': 'apptainer'}
        self.container_options = ''
        self.is_cvmfs = True
        self.type = 'production'


class _FakeInfoSys:
    """Minimal stand-in for job.infosys."""

    def __init__(self):
        """Attach the fake queuedata."""
        self.queuedata = _FakeQueueData()


def _make_job(workdir: str) -> JobData:
    """Return a minimal JobData sufficient to drive alrb_wrapper() end-to-end.

    Args:
        workdir: job working directory (str).

    Returns:
        JobData: minimally populated job object.
    """
    job = JobData({}, use_kmap=False)
    job.workdir = workdir
    job.jobid = '1234567890'
    job.swrelease = 'NULL'  # bypass release-setup extraction, irrelevant here
    job.platform = 'x86_64-centos7-gcc8-opt'
    job.alrbuserplatform = ''
    job.imagename = ''
    job.jobparams = ''
    job.preprocess = None
    job.containeroptions = {}
    job.pandasecrets = {}
    job.infosys = _FakeInfoSys()
    job.is_analysis = lambda: False

    return job


class TestAlrbWrapperContCHome(unittest.TestCase):
    """Tests that alrb_wrapper() sets ALRB_CONT_CHOME for the payload container."""

    def setUp(self):
        """Create a job workdir and ensure no site-level override is in effect."""
        self.workdir = tempfile.mkdtemp()
        self.job = _make_job(self.workdir)
        self._saved_chome = os.environ.pop('ALRB_CONT_CHOME', None)

    def tearDown(self):
        """Restore any pre-existing ALRB_CONT_CHOME and clean up the workdir."""
        os.environ.pop('ALRB_CONT_CHOME', None)
        if self._saved_chome is not None:
            os.environ['ALRB_CONT_CHOME'] = self._saved_chome
        shutil.rmtree(self.workdir, ignore_errors=True)

    def test_payload_container_gets_alrb_cont_chome(self):
        """The payload container command must export ALRB_CONT_CHOME=<job.workdir>."""
        cmd = alrb_wrapper('/bin/true', self.workdir, job=self.job)
        self.assertIn(f'export ALRB_CONT_CHOME={self.workdir};', cmd)

    def test_site_level_override_is_respected(self):
        """If ALRB_CONT_CHOME is already set in the environment, do not override it."""
        os.environ['ALRB_CONT_CHOME'] = '/site/defined/chome'
        cmd = alrb_wrapper('/bin/true', self.workdir, job=self.job)
        self.assertNotIn('export ALRB_CONT_CHOME=', cmd)

    def test_no_job_object_is_unaffected(self):
        """Without a job object, alrb_wrapper() must bail out early and not raise."""
        cmd = alrb_wrapper('/bin/true', self.workdir, job=None)
        self.assertEqual(cmd, '/bin/true')


if __name__ == '__main__':
    unittest.main()
