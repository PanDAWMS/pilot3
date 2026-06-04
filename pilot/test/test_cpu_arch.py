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
# - Paul Nilsson, paul.nilsson@cern.ch, 2026

"""Unit tests for pilot/scripts/cpu_arch.py.

Tests cover the pure classification functions (check_flags, x86_checks,
arm_checks) and the spec builders (build_x86_specs, build_x86naive_specs,
build_arm_spec).  /proc/cpuinfo and uname are not touched.
"""

import unittest

from pilot.scripts.cpu_arch import (
    FlagSpec,
    X86FlagSpecs,
    arm_checks,
    build_arm_spec,
    build_x86_specs,
    build_x86naive_specs,
    check_flags,
    x86_checks,
)


# ---------------------------------------------------------------------------
# Representative flag strings taken from real worker nodes / ATLAS pilot logs
# ---------------------------------------------------------------------------

# Skylake-X — x86-64-v4 (has AVX-512 family)
_FLAGS_V4 = (
    "fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat "
    "pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx "
    "pdpe1gb rdtscp lm constant_tsc art arch_perfmon pebs bts rep_good "
    "nopl xtopology nonstop_tsc cpuid aperfmperf pni pclmulqdq dtes64 "
    "monitor ds_cpl vmx smx est tm2 ssse3 sdbg fma cx16 xtpr pdcm pcid "
    "dca sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave "
    "avx f16c rdrand lahf_lm abm 3dnowprefetch cpuid_fault epb cat_l3 "
    "cdp_l3 invpcid_single intel_ppin ssbd mba ibrs ibpb stibp ibrs_enhanced "
    "tpr_shadow vnmi flexpriority ept vpid ept_ad fsgsbase tsc_adjust bmi1 "
    "avx2 smep bmi2 erms invpcid cqm mpx rdt_a avx512f avx512dq rdseed adx "
    "smap clflushopt clwb intel_pt avx512cd avx512bw avx512vl xsaveopt "
    "xsavec xgetbv1 xsaves cqm_llc cqm_occup_llc cqm_mbm_total cqm_mbm_local "
    "dtherm ida arat pln pts pku ospke md_clear flush_l1d arch_capabilities"
)

# Haswell — x86-64-v3 (AVX2 present, no AVX-512)
_FLAGS_V3 = (
    "fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat "
    "pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx "
    "pdpe1gb rdtscp lm constant_tsc arch_perfmon pebs bts rep_good nopl "
    "xtopology nonstop_tsc cpuid aperfmperf pni pclmulqdq dtes64 monitor "
    "ds_cpl vmx smx est tm2 ssse3 sdbg fma cx16 xtpr pdcm pcid dca "
    "sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx "
    "f16c rdrand lahf_lm abm cpuid_fault epb invpcid_single ssbd ibrs ibpb "
    "stibp tpr_shadow vnmi flexpriority ept vpid ept_ad fsgsbase tsc_adjust "
    "bmi1 avx2 smep bmi2 erms invpcid xsaveopt dtherm ida arat pln pts md_clear"
)

# Sandy Bridge — x86-64-v2 (SSE4.2, no AVX2, no AVX-512)
_FLAGS_V2 = (
    "fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat "
    "pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx "
    "pdpe1gb rdtscp lm constant_tsc arch_perfmon pebs bts rep_good nopl "
    "xtopology nonstop_tsc cpuid aperfmperf pni pclmulqdq dtes64 monitor "
    "ds_cpl vmx smx est tm2 ssse3 cx16 xtpr pdcm pcid dca sse4_1 sse4_2 "
    "x2apic popcnt tsc_deadline_timer aes xsave avx lahf_lm epb ssbd ibrs "
    "ibpb stibp tpr_shadow ept fsgsbase smep erms xsaveopt dtherm ida arat pln"
)

# Hypothetical ancient CPU — x86-64-v1 (SSE2 only)
_FLAGS_V1 = "fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov mmx fxsr sse sse2"

# ARMv8 (Graviton2-style)
_FLAGS_ARM_V8 = (
    "fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics fphp asimdhp "
    "cpuid asimdrdm lrcpc dcpop asimddp ssbs"
)

# ARM without mandatory flags
_FLAGS_ARM_BARE = "evtstrm aes pmull sha1 sha2 crc32"


class TestCheckFlags(unittest.TestCase):
    """Tests for the low-level check_flags() predicate."""

    def test_must_all_present(self):
        """All required flags present → no failure."""
        spec = FlagSpec(must=[r"sse", r"sse2"])
        self.assertFalse(check_flags(spec, ["sse", "sse2", "avx"]))

    def test_must_one_missing(self):
        """One required flag absent → failure."""
        spec = FlagSpec(must=[r"sse", r"avx512f"])
        self.assertTrue(check_flags(spec, ["sse", "sse2"]))

    def test_must_not_present(self):
        """A prohibited flag present → failure."""
        spec = FlagSpec(must_not=[r"avx512f"])
        self.assertTrue(check_flags(spec, ["sse", "avx512f"]))

    def test_must_not_absent(self):
        """Prohibited flag absent → no failure."""
        spec = FlagSpec(must_not=[r"avx512f"])
        self.assertFalse(check_flags(spec, ["sse", "avx2"]))

    def test_regex_pattern(self):
        """Regex pattern (AVX512.*) matches any AVX-512 variant."""
        spec = FlagSpec(must=[r"AVX512.*"])
        self.assertFalse(check_flags(spec, ["avx512f", "avx512bw"]))
        self.assertTrue(check_flags(spec, ["avx2"]))

    def test_case_insensitive(self):
        """Flag matching is case-insensitive."""
        spec = FlagSpec(must=[r"SSE4_2"])
        self.assertFalse(check_flags(spec, ["sse4_2"]))
        self.assertFalse(check_flags(spec, ["SSE4_2"]))

    def test_empty_spec(self):
        """Empty spec always passes (no requirements)."""
        self.assertFalse(check_flags(FlagSpec(), ["sse"]))
        self.assertFalse(check_flags(FlagSpec(), []))


class TestX86ChecksGCC(unittest.TestCase):
    """x86_checks() with the GCC-derived spec (build_x86_specs)."""

    def setUp(self):
        self.specs = build_x86_specs()

    def test_v4(self):
        """Skylake-X flags → x86-64-v4."""
        self.assertEqual(x86_checks(self.specs, _FLAGS_V4, "SkylakeX"), "x86-64-v4")

    def test_v3(self):
        """Haswell flags → x86-64-v3."""
        self.assertEqual(x86_checks(self.specs, _FLAGS_V3, "Haswell"), "x86-64-v3")

    def test_v2(self):
        """Sandy Bridge flags → x86-64-v2."""
        self.assertEqual(x86_checks(self.specs, _FLAGS_V2, "SandyBridge"), "x86-64-v2")

    def test_v1(self):
        """Ancient SSE2-only CPU flags → x86-64-v1."""
        self.assertEqual(x86_checks(self.specs, _FLAGS_V1, "AncientCPU"), "x86-64-v1")

    def test_empty_flags(self):
        """Empty flag string → x86-64-v1 (nothing passes)."""
        self.assertEqual(x86_checks(self.specs, "", "NoCPU"), "x86-64-v1")


class TestX86ChecksNaive(unittest.TestCase):
    """x86_checks() with the simplified naive spec (build_x86naive_specs)."""

    def setUp(self):
        self.specs = build_x86naive_specs()

    def test_v4_naive(self):
        """AVX-512 present → x86-64-v4 (naive)."""
        self.assertEqual(x86_checks(self.specs, _FLAGS_V4, "SkylakeX"), "x86-64-v4")

    def test_v3_naive(self):
        """AVX2 present, no AVX-512 → x86-64-v3 (naive)."""
        self.assertEqual(x86_checks(self.specs, _FLAGS_V3, "Haswell"), "x86-64-v3")

    def test_v2_naive(self):
        """SSE4.2 present, no AVX2 → x86-64-v2 (naive)."""
        self.assertEqual(x86_checks(self.specs, _FLAGS_V2, "SandyBridge"), "x86-64-v2")

    def test_v1_naive(self):
        """No SSE4.2 → x86-64-v1 (naive)."""
        self.assertEqual(x86_checks(self.specs, _FLAGS_V1, "AncientCPU"), "x86-64-v1")


class TestArmChecks(unittest.TestCase):
    """arm_checks() with the ARMv8 spec (build_arm_spec)."""

    def setUp(self):
        self.spec = build_arm_spec()

    def test_armv8_full_flags(self):
        """FP + ASIMD present → ARMv8."""
        self.assertEqual(arm_checks(self.spec, _FLAGS_ARM_V8, "8"), "ARMv8")

    def test_arm_missing_asimd(self):
        """ASIMD absent → UNKNOWN."""
        flags = "fp evtstrm aes"
        self.assertEqual(arm_checks(self.spec, flags, "8"), "UNKNOWN")

    def test_arm_missing_fp(self):
        """FP absent → UNKNOWN."""
        flags = "asimd evtstrm aes"
        self.assertEqual(arm_checks(self.spec, flags, "8"), "UNKNOWN")

    def test_arm_bare_flags(self):
        """Neither FP nor ASIMD → UNKNOWN."""
        self.assertEqual(arm_checks(self.spec, _FLAGS_ARM_BARE, "8"), "UNKNOWN")

    def test_arm_empty_flags(self):
        """Empty flag string → UNKNOWN."""
        self.assertEqual(arm_checks(self.spec, "", "8"), "UNKNOWN")


class TestSpecBuilders(unittest.TestCase):
    """Smoke tests for the spec builder functions."""

    def test_build_x86_specs_returns_x86flagspecs(self):
        specs = build_x86_specs()
        self.assertIsInstance(specs, X86FlagSpecs)
        self.assertTrue(specs.v4.must)
        self.assertTrue(specs.v3.must)
        self.assertTrue(specs.v2.must)

    def test_build_x86naive_specs_returns_x86flagspecs(self):
        specs = build_x86naive_specs()
        self.assertIsInstance(specs, X86FlagSpecs)
        self.assertTrue(specs.v4.must)
        self.assertTrue(specs.v3.must)
        self.assertTrue(specs.v2.must)

    def test_build_arm_spec_returns_flagspec(self):
        spec = build_arm_spec()
        self.assertIsInstance(spec, FlagSpec)
        self.assertIn(r"FP", spec.must)
        self.assertIn(r"ASIMD", spec.must)

    def test_v4_is_superset_of_v3(self):
        """Every flag required for v3 is also required for v4 (GCC spec)."""
        specs = build_x86_specs()
        self.assertTrue(set(specs.v3.must).issubset(set(specs.v4.must)))

    def test_v3_is_superset_of_v2(self):
        """Every flag required for v2 is also required for v3 (GCC spec)."""
        specs = build_x86_specs()
        self.assertTrue(set(specs.v2.must).issubset(set(specs.v3.must)))


if __name__ == "__main__":
    unittest.main()
