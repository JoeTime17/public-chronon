"""
Wrappers to directly create Source objects.
"""

import gen_thrift.api.ttypes as ttypes
from ai.chronon import utils


def EventSource(
    table: str,
    query: ttypes.Query,
    topic: str = None,
    is_cumulative: bool = None,
) -> ttypes.Source:
    """
    Event Sources represent data that gets generated over-time.
    Typically, but not necessarily, logged to message buses like kafka, kinesis or google pub/sub.
    fct tables are also event source worthy.

    Attributes:

     - table: Table currently needs to be a 'ds' (date string - yyyy-MM-dd) partitioned hive table.
              Table names can contain subpartition specs, example db.table/system=mobile/currency=USD
     - topic: Topic is a kafka table. The table contains all the events historically came through this topic.
     - query: The logic used to scan both the table and the topic. Contains row level transformations
              and filtering expressed as Spark SQL statements.
     - isCumulative: If each new hive partition contains not just the current day's events but the entire set
                     of events since the begininng. The key property is that the events are not mutated
                     across partitions.

    """
    return ttypes.Source(
        events=ttypes.EventSource(
            table=str(table),
            topic=topic,
            query=utils.propagate_table_reference_grid(query, table),
            isCumulative=is_cumulative,
        )
    )


def EntitySource(
    snapshot_table: str,
    query: ttypes.Query,
    mutation_table: str = None,
    mutation_topic: str = None,
) -> ttypes.Source:
    """
    Entity Sources represent data that gets mutated over-time - at row-level. This is a group of three data elements.
    snapshotTable, mutationTable and mutationTopic. mutationTable and mutationTopic are only necessary if we are trying
    to create realtime or point-in-time aggregations over these sources. Entity sources usually map 1:1 with a database
    tables in your OLTP store that typically serves live application traffic. When mutation data is absent they map 1:1
    to `dim` tables in star schema.

    Attributes:
     - snapshotTable: Snapshot table currently needs to be a 'ds' (date string - yyyy-MM-dd) partitioned hive table.
     - mutationTable: Topic is a kafka table. The table contains
                      all the events that historically came through this topic.
                      We need all the fields present in the snapshot table, PLUS two additional fields,
                      `mutation_time` - milliseconds since epoch of type Long that represents the time of the mutation
                      `is_before` - a boolean flag that represents whether
                                    this row contains values before or after the mutation.
     - mutationTopic: The logic used to scan both the table and the topic. Contains row level transformations
                      and filtering expressed as Spark SQL statements.
     - query: If each new hive partition contains not just the current day's events but the entire set
              of events since the begininng. The key property is that the events are not mutated across partitions.
    """
    return ttypes.Source(
        entities=ttypes.EntitySource(
            snapshotTable=str(snapshot_table),
            mutationTable=str(mutation_table) if mutation_table is not None else None,
            mutationTopic=mutation_topic,
            query=utils.propagate_table_reference_grid(query, snapshot_table),
        )
    )


def JoinSource(join: ttypes.Join, query: ttypes.Query = None) -> ttypes.Source:
    """
    The output of a join can be used as a source for `GroupBy`.
    Useful for expressing complex computation in chronon.

    Offline this simply means that we will compute the necessary date ranges of the join
    before we start computing the `GroupBy`.

    Online we will:
    1. enrich the stream/topic of `join.left` with all the columns defined by the join
    2. apply the selects & wheres defined in the `query`
    3. perform aggregations defined in the *downstream* `GroupBy`
    4. write the result to the kv store.
    """
    return ttypes.Source(joinSource=ttypes.JoinSource(join=join, query=query))
