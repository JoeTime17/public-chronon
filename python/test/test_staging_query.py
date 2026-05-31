#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import warnings

import gen_thrift.common.ttypes as common
from ai.chronon.staging_query import TableDependency


def _days(n):
    return common.Window(length=n, timeUnit=common.TimeUnit.DAYS)


def test_nothing_set_defaults_both_sides_to_zero():
    td = TableDependency(table="ns.upstream").to_thrift()
    assert td.startOffset == _days(0)
    assert td.endOffset == _days(0)
    assert td.startCutOff is None
    assert td.endCutOff is None


def test_partition_column_without_offsets_no_longer_raises():
    # Previously validated; now defaults to [query.start, query.end].
    td = TableDependency(table="ns.upstream", partition_column="ds").to_thrift()
    assert td.startOffset == _days(0)
    assert td.endOffset == _days(0)


def test_start_offset_only_sets_start_and_defaults_end_to_zero():
    td = TableDependency(table="ns.upstream", start_offset=7).to_thrift()
    assert td.startOffset == _days(7)
    assert td.endOffset == _days(0)


def test_end_offset_only_sets_end_and_defaults_start_to_zero():
    td = TableDependency(table="ns.upstream", end_offset=1).to_thrift()
    assert td.startOffset == _days(0)
    assert td.endOffset == _days(1)


def test_asymmetric_start_and_end_offsets():
    td = TableDependency(table="ns.upstream", start_offset=30, end_offset=1).to_thrift()
    assert td.startOffset == _days(30)
    assert td.endOffset == _days(1)


def test_start_cutoff_alone_pins_start_and_defaults_end_to_zero():
    # startOffset is None so the planner resolves start = max(null, cutoff) = cutoff.
    td = TableDependency(table="ns.upstream", start_cutoff="2024-01-01").to_thrift()
    assert td.startOffset is None
    assert td.endOffset == _days(0)
    assert td.startCutOff == "2024-01-01"
    assert td.endCutOff is None


def test_end_cutoff_alone_clamps_end_with_zero_offset():
    # No pin-on-end: null endOffset breaks downstream planners, so end always clamps.
    td = TableDependency(table="ns.upstream", end_cutoff="2024-12-31").to_thrift()
    assert td.startOffset == _days(0)
    assert td.endOffset == _days(0)
    assert td.endCutOff == "2024-12-31"


def test_start_offset_combines_with_start_cutoff_for_clamp_with_floor():
    td = TableDependency(
        table="ns.upstream", start_offset=7, start_cutoff="2024-01-01"
    ).to_thrift()
    assert td.startOffset == _days(7)
    assert td.startCutOff == "2024-01-01"


def test_end_offset_combines_with_end_cutoff():
    td = TableDependency(
        table="ns.upstream", end_offset=1, end_cutoff="2024-12-31"
    ).to_thrift()
    assert td.endOffset == _days(1)
    assert td.endCutOff == "2024-12-31"


def test_deprecated_offset_applies_to_both_sides():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        td = TableDependency(table="ns.upstream", offset=3).to_thrift()
    assert td.startOffset == _days(3)
    assert td.endOffset == _days(3)
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)


def test_start_offset_overrides_deprecated_offset_on_start_side():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        td = TableDependency(
            table="ns.upstream", offset=5, start_offset=1
        ).to_thrift()
    assert td.startOffset == _days(1)
    assert td.endOffset == _days(5)


def test_end_offset_overrides_deprecated_offset_on_end_side():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        td = TableDependency(
            table="ns.upstream", offset=5, end_offset=0
        ).to_thrift()
    assert td.startOffset == _days(5)
    assert td.endOffset == _days(0)


def test_deprecated_offset_with_start_cutoff_clamps_not_pins():
    # offset wins the start-side fallback over the cutoff-implied pin default.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        td = TableDependency(
            table="ns.upstream", offset=7, start_cutoff="2024-01-01"
        ).to_thrift()
    assert td.startOffset == _days(7)
    assert td.startCutOff == "2024-01-01"


def test_deprecated_offset_zero_still_warns():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        TableDependency(table="ns.upstream", offset=0).to_thrift()
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)


def test_end_cutoff_passthrough_with_zero_end_offset():
    td = TableDependency(
        table="ns.upstream", end_offset=0, end_cutoff="2024-12-31"
    ).to_thrift()
    assert td.endCutOff == "2024-12-31"
    assert td.endOffset == _days(0)
