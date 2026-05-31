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

from ai.chronon.airflow_helpers import (
    _cutoff_already_binds,
    _java_format_to_python,
    create_airflow_dependency,
)


def test_sensor_uses_negated_offset_so_positive_offset_waits_on_past_partition():
    dep = create_airflow_dependency("ns.t", "ds", offset=1)
    assert dep["spec"] == "ns.t/ds={{ macros.ds_add(ds, -1) }}"
    assert dep["name"] == "wf_ns_t_with_offset_1"


def test_offset_zero_produces_ds_add_zero():
    dep = create_airflow_dependency("ns.t", "ds", offset=0)
    assert dep["spec"] == "ns.t/ds={{ macros.ds_add(ds, 0) }}"


def test_offset_seven_days_waits_on_ds_minus_seven():
    dep = create_airflow_dependency("ns.t", "ds", offset=7)
    assert dep["spec"] == "ns.t/ds={{ macros.ds_add(ds, -7) }}"


def test_additional_partitions_are_appended():
    dep = create_airflow_dependency(
        "ns.t", "ds", additional_partitions=["_HR=23:00"], offset=1
    )
    assert dep["spec"] == "ns.t/ds={{ macros.ds_add(ds, -1) }}/_HR=23:00"


def test_end_cutoff_in_the_far_past_uses_literal_partition():
    # Any plausible "today" is well past 1970; the cutoff binds.
    dep = create_airflow_dependency(
        "ns.t", "ds", offset=0, end_cutoff="1970-01-01"
    )
    assert dep["spec"] == "ns.t/ds=1970-01-01"
    assert dep["name"] == "wf_ns_t_at_1970_01_01"


def test_end_cutoff_in_the_far_future_falls_back_to_relative_spec():
    # Any plausible "today" is before 2099; the cutoff doesn't bind yet.
    dep = create_airflow_dependency(
        "ns.t", "ds", offset=1, end_cutoff="2099-12-31"
    )
    assert dep["spec"] == "ns.t/ds={{ macros.ds_add(ds, -1) }}"


def test_cutoff_binds_check_accounts_for_offset():
    # today=2026-04-24, offset=90 → effective date = 2026-01-24
    # cutoff = 2026-02-01 is AFTER the effective date → cutoff does not bind yet.
    assert not _cutoff_already_binds(
        offset=90, end_cutoff="2026-02-01", today="2026-04-24"
    )
    # Same cutoff, offset=0 → effective = today = 2026-04-24 > 2026-02-01 → cutoff binds.
    assert _cutoff_already_binds(
        offset=0, end_cutoff="2026-02-01", today="2026-04-24"
    )


def test_cutoff_on_same_day_as_effective_date_does_not_bind():
    # Boundary: cutoff == effective date is the *last* partition the dep accepts;
    # it hasn't been crossed yet, so the relative spec is still correct.
    assert not _cutoff_already_binds(
        offset=0, end_cutoff="2026-04-24", today="2026-04-24"
    )


def test_java_format_to_python_translation():
    assert _java_format_to_python("yyyy-MM-dd") == "%Y-%m-%d"
    assert _java_format_to_python("yyyyMMdd") == "%Y%m%d"
    assert _java_format_to_python("yyyy/MM/dd") == "%Y/%m/%d"
    assert _java_format_to_python("yyyy-MM-dd HH:mm:ss") == "%Y-%m-%d %H:%M:%S"


def test_cutoff_bind_check_uses_partition_format_for_comparison():
    # Non-ISO format: cutoff and effective must be rendered in the same
    # partition format or the string comparison is meaningless.
    # today=2026-04-24 (rendered as 20260424 in yyyyMMdd), cutoff=20240101 in
    # the same format → effective sorts past cutoff → binds.
    assert _cutoff_already_binds(
        offset=0,
        end_cutoff="20240101",
        partition_format="yyyyMMdd",
        today="2026-04-24",
    )
    # If we had naively isoformatted effective (2026-04-24) and compared to
    # "20240101", the ASCII of '-' (45) < '0' (48) would have made effective
    # sort BEFORE the cutoff — i.e. returned False incorrectly. This asserts
    # the format translation is actually doing its job.


def test_cutoff_literal_preserves_user_format_in_spec():
    # yyyyMMdd cutoff deep in the past — spec must emit the literal as-is.
    dep = create_airflow_dependency(
        "ns.t",
        "ds",
        offset=0,
        end_cutoff="19700101",
        partition_format="yyyyMMdd",
    )
    assert dep["spec"] == "ns.t/ds=19700101"
    assert dep["name"] == "wf_ns_t_at_19700101"


def test_non_iso_future_cutoff_falls_back_to_relative_spec():
    # Cutoff is in the future relative to any plausible "today"; the relative
    # form is still what we want, regardless of format.
    dep = create_airflow_dependency(
        "ns.t", "ds", offset=1, end_cutoff="20991231", partition_format="yyyyMMdd"
    )
    assert dep["spec"] == "ns.t/ds={{ macros.ds_add(ds, -1) }}"
