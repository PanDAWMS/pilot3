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

"""Unit tests for QueueData.use_per_core_attr() and QueueData.apply_per_core_scaling().

Covers the pilot-side implementation of the transitional 'per_core_attr' PQ flag
(ATLASPANDA-1609). When a PQ declares 'per_core_attr' in its catchall field, CRIC
provides 'maxrss' and 'maxwdir' as per-core values instead of the traditional
PQ-scale (full corecount) values. QueueData.clean() converts them to PQ-scale
values exactly once, at queue data load time, mirroring the equivalent
conversion applied server-side by JEDI/panda-server
(pandaserver.taskbuffer.db_proxy_mods.entity_module, PR #648):

    if ret.use_per_core_attr() and ret.coreCount > 0:
        if ret.maxrss:
            ret.maxrss = ret.maxrss * ret.coreCount
        if ret.maxwdir:
            ret.maxwdir = ret.maxwdir * ret.coreCount

Because the conversion happens once, at load time, no other pilot code
(pilot.util.monitoring.get_max_allowed_work_dir_size(),
pilot.user.atlas.memory.calculate_memory_limit_kb(), etc.) needs to change:
they continue to consume PQ-scale values exactly as before.

Note that the pilot's use_per_core_attr() intentionally diverges from JEDI's
SiteSpec.use_per_core_attr()/hasValueInCatchall() in one respect: JEDI's check
is presence-only (regex `^{key}(=|)*`) and would treat 'per_core_attr=False'
as *enabled*, since it never inspects the value after '='. See:
https://github.com/PanDAWMS/panda-server/blob/b83af5ff03b356ec127edc88294ea19eba45d091/pandaserver/taskbuffer/SiteSpec.py#L9-L148
The pilot instead parses the value as a proper boolean (true/yes/1 vs.
false/no/0, matching BaseData.clean_boolean()'s vocabulary), so an explicit
'per_core_attr=False' correctly disables per-core scaling.

Two related PQ attributes are deliberately NOT scaled here:
- 'maxinputsize': not scaled by JEDI either (confirmed no-op in PR #648), and not
  actually used as a numeric limit anywhere in the pilot - the pilot's own input
  size checks are derived from 'maxwdir' (see pilot.util.parameters.get_maximum_input_sizes()),
  not from the 'maxinputsize' PQ attribute.
- 'minrss': not a pilot concept at all. It is only used by JEDI for site
  brokerage/selection (pandaserver.brokerage.SiteMapper) and is never sent to,
  or consumed by, the pilot.
"""

import logging
import sys
import unittest

from pilot.info.queuedata import QueueData

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


class TestUsePerCoreAttr(unittest.TestCase):
    """Tests for QueueData.use_per_core_attr() catchall parsing."""

    def setUp(self):
        """Build a minimal QueueData instance shared by tests in this class."""
        self.qdata = QueueData({'maxrss': 2000, 'maxwdir': 4000, 'corecount': 8})

    def test_flag_absent(self):
        """catchall without the flag should not enable per-core scaling."""
        self.qdata.catchall = 'some_other_flag=1'
        self.assertFalse(self.qdata.use_per_core_attr())

    def test_flag_empty_catchall(self):
        """An empty catchall should not enable per-core scaling."""
        self.qdata.catchall = ''
        self.assertFalse(self.qdata.use_per_core_attr())

    def test_flag_bare_token(self):
        """A bare 'per_core_attr' token in catchall should enable per-core scaling."""
        self.qdata.catchall = 'per_core_attr'
        self.assertTrue(self.qdata.use_per_core_attr())

    def test_flag_bare_token_among_others(self):
        """A bare 'per_core_attr' token mixed with other catchall entries should be found."""
        self.qdata.catchall = 'gpu,per_core_attr,useJumboJobs'
        self.assertTrue(self.qdata.use_per_core_attr())

    def test_flag_key_value_form(self):
        """A 'per_core_attr=<value>' entry in catchall should also enable per-core scaling."""
        self.qdata.catchall = 'per_core_attr=true'
        self.assertTrue(self.qdata.use_per_core_attr())

    def test_flag_explicit_false_is_disabled(self):
        """'per_core_attr=False' must be parsed as disabled, unlike JEDI's presence-only check."""
        self.qdata.catchall = 'per_core_attr=False'
        self.assertFalse(self.qdata.use_per_core_attr())

    def test_flag_explicit_false_lowercase_is_disabled(self):
        """'per_core_attr=false' (lowercase) must be parsed as disabled."""
        self.qdata.catchall = 'per_core_attr=false'
        self.assertFalse(self.qdata.use_per_core_attr())

    def test_flag_zero_is_disabled(self):
        """'per_core_attr=0' must be parsed as disabled."""
        self.qdata.catchall = 'per_core_attr=0'
        self.assertFalse(self.qdata.use_per_core_attr())

    def test_flag_no_is_disabled(self):
        """'per_core_attr=no' must be parsed as disabled."""
        self.qdata.catchall = 'per_core_attr=no'
        self.assertFalse(self.qdata.use_per_core_attr())

    def test_flag_one_is_enabled(self):
        """'per_core_attr=1' must be parsed as enabled."""
        self.qdata.catchall = 'per_core_attr=1'
        self.assertTrue(self.qdata.use_per_core_attr())

    def test_flag_yes_is_enabled(self):
        """'per_core_attr=yes' must be parsed as enabled."""
        self.qdata.catchall = 'per_core_attr=yes'
        self.assertTrue(self.qdata.use_per_core_attr())

    def test_flag_unrecognized_value_is_treated_as_disabled(self):
        """An unrecognized value must not silently enable per-core scaling."""
        self.qdata.catchall = 'per_core_attr=maybe'
        self.assertFalse(self.qdata.use_per_core_attr())

    def test_flag_with_surrounding_whitespace(self):
        """Whitespace around catchall tokens should not prevent the flag from being found."""
        self.qdata.catchall = ' gpu , per_core_attr , other=1 '
        self.assertTrue(self.qdata.use_per_core_attr())

    def test_flag_substring_not_matched(self):
        """A token that merely contains 'per_core_attr' as a substring must not match."""
        self.qdata.catchall = 'not_per_core_attr_related'
        self.assertFalse(self.qdata.use_per_core_attr())


class TestApplyPerCoreScaling(unittest.TestCase):
    """Tests for QueueData.apply_per_core_scaling()."""

    def test_flag_not_set_leaves_values_unchanged(self):
        """maxrss/maxwdir must be left untouched when per_core_attr is not set."""
        qdata = QueueData({'maxrss': 2000, 'maxwdir': 4000, 'corecount': 8, 'catchall': ''})
        self.assertEqual(qdata.maxrss, 2000)
        self.assertEqual(qdata.maxwdir, 4000)

    def test_flag_set_scales_maxrss_and_maxwdir(self):
        """maxrss and maxwdir must be multiplied by corecount when per_core_attr is set."""
        qdata = QueueData({'maxrss': 2000, 'maxwdir': 4000, 'corecount': 8, 'catchall': 'per_core_attr'})
        self.assertEqual(qdata.maxrss, 2000 * 8)
        self.assertEqual(qdata.maxwdir, 4000 * 8)

    def test_flag_explicit_false_skips_scaling(self):
        """maxrss/maxwdir must be left unchanged when per_core_attr is explicitly set to False."""
        qdata = QueueData({'maxrss': 2000, 'maxwdir': 4000, 'corecount': 8, 'catchall': 'per_core_attr=False'})
        self.assertEqual(qdata.maxrss, 2000)
        self.assertEqual(qdata.maxwdir, 4000)

    def test_flag_set_corecount_one_is_a_noop_multiplication(self):
        """With corecount=1, scaling multiplies by 1 (values unchanged, but path is exercised)."""
        qdata = QueueData({'maxrss': 2000, 'maxwdir': 4000, 'corecount': 1, 'catchall': 'per_core_attr'})
        self.assertEqual(qdata.maxrss, 2000)
        self.assertEqual(qdata.maxwdir, 4000)

    def test_flag_set_maxrss_zero_is_left_as_zero(self):
        """A zero/unset maxrss must not be turned into a truthy value by scaling."""
        qdata = QueueData({'maxrss': 0, 'maxwdir': 4000, 'corecount': 8, 'catchall': 'per_core_attr'})
        self.assertEqual(qdata.maxrss, 0)
        self.assertEqual(qdata.maxwdir, 4000 * 8)

    def test_flag_set_maxwdir_zero_is_left_as_zero(self):
        """A zero/unset maxwdir must not be turned into a truthy value by scaling."""
        qdata = QueueData({'maxrss': 2000, 'maxwdir': 0, 'corecount': 8, 'catchall': 'per_core_attr'})
        self.assertEqual(qdata.maxrss, 2000 * 8)
        self.assertEqual(qdata.maxwdir, 0)

    def test_flag_set_corecount_missing_defaults_to_one(self):
        """Missing corecount defaults to 1 (via clean__corecount) and scaling is a no-op multiplication."""
        qdata = QueueData({'maxrss': 2000, 'maxwdir': 4000, 'catchall': 'per_core_attr'})
        self.assertEqual(qdata.corecount, 1)
        self.assertEqual(qdata.maxrss, 2000)
        self.assertEqual(qdata.maxwdir, 4000)

    def test_flag_set_negative_corecount_skips_scaling(self):
        """A corecount that ends up <= 0 must not be used as a scaling factor."""
        qdata = QueueData({'maxrss': 2000, 'maxwdir': 4000, 'corecount': -1, 'catchall': 'per_core_attr'})
        # clean__corecount() only defaults falsy (0/None) values to 1, so a negative
        # value passes through unchanged; apply_per_core_scaling() must still guard it.
        self.assertEqual(qdata.maxrss, 2000)
        self.assertEqual(qdata.maxwdir, 4000)

    def test_maxinputsize_never_scaled(self):
        """maxinputsize must never be scaled, flag set or not (matches JEDI's no-op, PR #648)."""
        qdata = QueueData(
            {'maxrss': 2000, 'maxwdir': 4000, 'maxinputsize': 16336, 'corecount': 8, 'catchall': 'per_core_attr'}
        )
        self.assertEqual(qdata.maxinputsize, 16336)

    def test_scaling_applied_only_once(self):
        """A freshly constructed QueueData must only ever be scaled once (no double-scaling)."""
        qdata = QueueData({'maxrss': 2000, 'maxwdir': 4000, 'corecount': 8, 'catchall': 'per_core_attr'})
        expected_maxrss = 2000 * 8
        expected_maxwdir = 4000 * 8
        self.assertEqual(qdata.maxrss, expected_maxrss)
        self.assertEqual(qdata.maxwdir, expected_maxwdir)

        # calling apply_per_core_scaling() again would double-scale if invoked a second time;
        # verify that clean()/__init__ do not do this by construction (single call site in clean()).
        qdata.apply_per_core_scaling()
        self.assertNotEqual(qdata.maxrss, expected_maxrss, "sanity check: a second explicit call does scale again")
        self.assertEqual(qdata.maxrss, expected_maxrss * 8)


if __name__ == '__main__':
    unittest.main()
