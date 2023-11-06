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
# - Pavlo Svirin, pavlo.svirin@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-23

import unittest
import string
import tempfile
import shutil
import random
import os.path

from pilot.copytool.mv import copy_in, copy_out
from pilot.common.exception import StageInFailure, StageOutFailure
from pilot.util.container import execute

from pilot.control.job import get_fake_job
from pilot.info import JobData


class TestCopytoolMv(unittest.TestCase):
    """
    Unit tests for mv copytool.
    """

    #filelist = []
    numfiles = 10
    maxfilesize = 100 * 1024

    def setUp(self):

        """ Create temp source directory """
        self.tmp_src_dir = tempfile.mkdtemp()
        """ Create temp destination directory """
        self.tmp_dst_dir = os.path.join(self.tmp_src_dir, 'dest')
        os.mkdir(self.tmp_dst_dir)

        os.environ['PILOT_USER'] = 'atlas'
        #self.filelist = []

        # need a job data object, but we will overwrite some of its info
        res = get_fake_job(input=False)
        jdata = JobData(res)

        infiles = ""
        #outfiles = ""
        fsize = ""
        realdatasetsin = ""
        dispatchdblocktokenforout = ""
        guid = ""
        checksum = ""
        scope = ""
        ddmendpointin = ""
        ddmendpointout = ""
        turl = ""
        """ Create temp files in source dir """
        for i in range(0, self.numfiles):
            # generate random name
            fname = ''.join(random.choice(string.ascii_lowercase) for x in range(20))
            if infiles == "":
                infiles = fname
            else:
                infiles += "," + fname
            # generate random data and write
            filesize = random.randint(1, self.maxfilesize)
            if fsize == "":
                fsize = str(filesize)
            else:
                fsize += "," + str(filesize)
            if realdatasetsin == "":
                realdatasetsin = "dataset1"
            else:
                realdatasetsin += ",dataset1"
            if guid == "":
                guid = "abcdefaaaaaa"
            else:
                guid += ",abcdefaaaaaa"
            if checksum == "":
                checksum = "abcdef"
            else:
                checksum += ",abcdef"
            if scope == "":
                scope = "scope1"
            else:
                scope += ",scope1"
            if ddmendpointin == "":
                ddmendpointin = "ep1"
            else:
                ddmendpointin += ",ep1"
            if ddmendpointout == "":
                ddmendpointout = "ep1"
            else:
                ddmendpointout += ",ep1"
            if dispatchdblocktokenforout == "":
                dispatchdblocktokenforout = "NULL"
            else:
                dispatchdblocktokenforout += ",NULL"
            _data = [random.randint(0, 255) for x in range(0, filesize)]
            fname = os.path.join(self.tmp_src_dir, fname)
            if turl == "":
                turl = fname
            else:
                turl = "," + fname
            new_file = open(fname, "wb")
            new_file.write(str(_data).encode('utf-8'))
            new_file.close()
            # add to list
            #self.filelist.append({'name': fname, 'source': self.tmp_src_dir, 'destination': self.tmp_dst_dir})

        # add a 'log file'
        #lfname = 'name.job.log.tgz'
        #filesize = random.randint(1, self.maxfilesize)
        #_data = [random.randint(0, 255) for x in range(0, filesize)]
        #lfname = os.path.join(self.tmp_src_dir, lfname)
        #new_file = open(lfname, "wb")
        #new_file.write(str(_data).encode('utf-8'))
        #new_file.close()

        # overwrite
        #data = {'inFiles': infiles, 'realDatasetsIn': realdatasetsin, 'GUID': guid,
        #        'fsize': fsize, 'checksum': checksum, 'scopeIn': scope,
        #        'ddmEndPointIn': ddmendpointin}
        data = {'inFiles': infiles, 'realDatasetsIn': realdatasetsin, 'GUID': guid,
                'fsize': fsize, 'checksum': checksum, 'scopeIn': scope,
                'ddmEndPointIn': ddmendpointin}
        self.indata = jdata.prepare_infiles(data)
        for _file in self.indata:
            _file.workdir = self.tmp_dst_dir
            _file.turl = os.path.join(self.tmp_src_dir, _file.lfn)

        #infiles += f',{lfname}'
        data = {'outFiles': infiles, 'dispatchDBlockTokenForOut': dispatchdblocktokenforout, 'logGUID': 'abcdefaaaaaa',
                'scopeOut': scope, 'ddmEndPointOut': ddmendpointout}

        #self.outdata = []  # jdata.prepare_outfiles(data)
        self.outdata, self.logdata = jdata.prepare_outfiles(data)
        for _file in self.outdata:
            _file.workdir = self.tmp_src_dir
            subdir = 'abc/def'
            _file.turl = os.path.join(os.path.join(self.tmp_dst_dir, subdir), _file.lfn)
            _file.checksum = {'adler32': 'abcdef'}
            _file.fsize = 'abcdef'

    def test_copy_in_mv(self):
        _, stdout1, stderr1 = execute(' '.join(['ls', self.tmp_src_dir, '|', 'grep', '-v', 'dest']))
        copy_in(self.indata, copy_type='mv', workdir=self.tmp_dst_dir)
        # here check files moved
        self.assertEqual(self.__dirs_content_valid(self.tmp_src_dir, self.tmp_dst_dir, dir2_expected_content=stdout1), 0)

    def test_copy_in_cp(self):
        copy_in(self.indata, copy_type='cp', workdir=self.tmp_dst_dir)
        self.assertEqual(self.__dirs_content_equal(self.tmp_src_dir, self.tmp_dst_dir), 0)

    def test_copy_in_symlink(self):
        copy_in(self.indata, copy_type='symlink', workdir=self.tmp_dst_dir)
        # here check files linked
        self.assertEqual(self.__dirs_content_equal(self.tmp_src_dir, self.tmp_dst_dir), 0)
        # check dst files are links
        _, stdout, _ = execute(r'find %s -type l -exec echo -n l \;' % self.tmp_dst_dir)
        self.assertEqual(stdout, ''.join('l' for i in range(self.numfiles)))

    def test_copy_in_invalid(self):
        self.assertRaises(StageInFailure, copy_in, self.indata, **{'copy_type': ''})
        self.assertRaises(StageInFailure, copy_in, self.indata, **{'copy_type': None})

    #def test_copy_out_mv(self):
    #    _, stdout1, stderr1 = execute(' '.join(['ls', self.tmp_src_dir, '|', 'grep', '-v', 'dest']))
    #    copy_out(self.outdata, workdir=self.tmp_src_dir)
    #    # for testing: _, stdout2, stderr2 = execute(' '.join(['ls', os.path.join(self.tmp_dst_dir, 'abc/def')]))
    #    # here check files linked
    #    self.assertEqual(self.__dirs_content_valid(self.tmp_src_dir, os.path.join(self.tmp_dst_dir, 'abc/def'), dir2_expected_content=stdout1), 0)

    def test_copy_out_cp(self):
        pass
        # copy_out(self.outdata, copy_type='cp')
        # self.assertEqual(self.__dirs_content_equal(self.tmp_src_dir, self.tmp_dst_dir), 0)

    def test_copy_out_invalid(self):
        self.assertRaises(StageOutFailure, copy_out, self.outdata, **{'copy_type': ''})
        self.assertRaises(StageOutFailure, copy_out, self.outdata, **{'copy_type': 'symlink'})
        self.assertRaises(StageOutFailure, copy_out, self.outdata, **{'copy_type': None})

    def tearDown(self):
        """ Drop temp directories """
        shutil.rmtree(self.tmp_dst_dir)
        shutil.rmtree(self.tmp_src_dir)

    def __dirs_content_equal(self, dir1, dir2):
        if dir1 == '' or dir2 == '' or dir1 is None or dir2 is None:
            return -1
        _, stdout1, stderr1 = execute(' '.join(['ls', dir1, '|', 'grep', '-v', 'dest']))
        _, stdout2, stderr2 = execute(' '.join(['ls', dir2, '|', 'grep', '-v', 'dest']))
        if stdout1 != stdout2:
            return -2
        return 0

    def __dirs_content_valid(self, dir1, dir2, dir1_expected_content=None, dir2_expected_content=None):
        # currently this fails: need to fix
        if dir1 == '' or dir2 == '' or dir1 is None or dir2 is None:
            return -1
        _, stdout1, stderr1 = execute(' '.join(['ls', dir1, '|', 'grep', '-v', 'dest']))
        if dir1_expected_content is not None and stdout1 != dir1_expected_content:
            return -3
        _, stdout2, stderr2 = execute(' '.join(['ls', dir2, '|', 'grep', '-v', 'dest']))
        if dir2_expected_content is not None and stdout2 != dir2_expected_content:
            return -4
        return 0


if __name__ == '__main__':
    unittest.main()
