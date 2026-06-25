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

from collections import OrderedDict
from typing import Dict, List, Union

import ai.chronon.windows as window_utils
import gen_thrift.api.ttypes as api
import gen_thrift.common.ttypes as common


def Query(
    selects: Dict[str, str] = None,
    wheres: List[str] = None,
    start_partition: str = None,
    end_partition: str = None,
    time_column: str = None,
    setups: List[str] = None,
    mutation_time_column: str = None,
    reversal_column: str = None,
    partition_column: str = None,
    partition_format: str = None,
    partition_interval: Union[common.Window, str] = None,
    partition_offset: Union[common.Window, str] = None,
    partition_lag: Union[common.Window, str] = None,
    sub_partitions_to_wait_for: List[str] = None,
    time_partitioned: bool = None,
) -> api.Query:
    """
    Create a query object that is used to scan data from various data sources.
    This contains partition ranges, row level transformations and filtering logic.
    Additionally we also require a time_column for TEMPORAL events, mutation_time_column & reversal
    for TEMPORAL entities.

    :param selects: Spark sql expressions with only arithmetic, function application & inline lambdas.
        You can also apply udfs see setups param below.::

            Example: {
                "alias": "built_in_function(col1) * my_udf(col2)",
                "alias1": "aggregate(array_col, 0, (acc, x) -> acc + x)"
            }

        See: https://spark.apache.org/docs/latest/api/sql/#built-in-functions
        When none, we will assume that no transformations are needed and will pick columns necessary for aggregations.
    :type selects: List[str], optional
    :param wheres: Used for filtering. Same as above, but each expression must return boolean.
        Expressions are joined using AND.
    :type wheres: List[str], optional
    :param start_partition: From which partition of the source is the data valid from - inclusive.
        When absent we will consider all available data is usable.
    :type start_partition: str, optional
    :param end_partition: Till what partition of the source is the data valid till - inclusive.
        Not specified unless you know for a fact that a particular source has expired after a partition and you
        should instead use another source after this partition.
    :type end_partition: str, optional
    :param time_column: a single expression to produce time as ** milliseconds since epoch**.
    :type time_column: str, optional
    :param setups: you can register UDFs using setups
        ["ADD JAR YOUR_JAR", "create temporary function YOU_UDF_NAME as YOUR_CLASS"]
    :type setups: List[str], optional
    :param mutation_time_column: For entities, with real time accuracy, you need to specify an expression that
        represents mutation time. Time should be milliseconds since epoch.
        This is not necessary for event sources, defaults to "mutation_ts"
    :type mutation_time_column: str, optional
    :param reversal_column: (defaults to "is_before")
        For entities with realtime accuracy, we divide updates into two additions & reversal.
        updates have two rows - one with is_before = True (the old value) & is_before = False (the new value)
        inserts only have is_before = false (just the new value).
        deletes only have is_before = true (just the old value).
        This is not necessary for event sources.
    :type reversal_column: str, optional
    :param partition_column:
        Specify this to override spark.chronon.partition.column set in teams.py for this particular query.
    :type partition_column: str, optional
    :param sub_partitions_to_wait_for:
        Additional partitions to be used in sensing that the source data has landed. Should be a full partition string, such as `hr=23:00'
    :type sub_partitions_to_wait_for: List[str], optional
    :param partition_format:
        Date format string to expect the partition values to be in.
    :type partition_format: str, optional
    :param partition_interval:
        Partition interval for the source table. Examples: "1d", "3h", "15m".
        Sub-daily partition values use "yyyy-MM-dd-HH-mm" unless partition_format is explicitly set.
    :type partition_interval: Union[common.Window, str], optional
    :param partition_offset:
        Offset from UTC midnight/epoch for the source partition grid. Examples: "1h" for
        partitions at 01:00, 04:00, ... with a "3h" partition_interval.
    :type partition_offset: Union[common.Window, str], optional
    :param partition_lag:
        Partition lag to apply when resolving dependencies. Examples: "1d", "3h", "15m".
    :type partition_lag: Union[common.Window, str], optional
    :param time_partitioned:
        Indicates the source table uses a timestamp or date column for time-based filtering
        instead of traditional Hive-style string partitioning. When True, partition_column
        should be set to the timestamp/date column name. The engine will derive virtual
        partitions via MIN/MAX of that column and use timestamp-based WHERE clauses.
        Common for BigQuery, Snowflake, and Delta Lake tables that aren't Hive-partitioned.
    :type time_partitioned: bool, optional
    :return: A Query object that Chronon can use to scan just the necessary data efficiently.
    """
    partition_spec = window_utils.PartitionSpec(
        column=partition_column,
        format=partition_format,
        interval=partition_interval,
        offset=partition_offset,
        time_partitioned=time_partitioned,
    )
    return api.Query(
        selects=selects,
        wheres=wheres,
        startPartition=start_partition,
        endPartition=end_partition,
        timeColumn=time_column,
        setups=setups,
        mutationTimeColumn=mutation_time_column,
        reversalColumn=reversal_column,
        subPartitionsToWaitFor=sub_partitions_to_wait_for,
        partitionLag=window_utils.normalize_window(partition_lag)
        if partition_lag is not None
        else None,
        **partition_spec.query_kwargs(),
    )


def selects(*args, **kwargs):
    """
    Create a dictionary required for the selects parameter of Query.

    .. code-block:: python
        selects(
            "event_id",
            user_id="user_id",
        )

    creates the following dictionary:

    .. code-block:: python
        {
            "event_id": "event_id",
            "user_id": "user_id"
        }
    """
    result = OrderedDict()
    for x in args:
        result[x] = x
    for k, v in kwargs.items():
        result[k] = v
    return result
