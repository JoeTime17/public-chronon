namespace py gen_thrift.common
namespace java ai.chronon.api

// integers map to milliseconds in the timeunit
enum TimeUnit {
    HOURS = 0
    DAYS = 1
    MINUTES = 2
}

struct Window {
    1: i32 length
    2: TimeUnit timeUnit
}

struct DateRange {
    1: string startDate
    2: string endDate
}

/**
* env vars for different modes of execution - with "common" applying to all modes
* the submitter will set these env vars prior to launching the job
*
* these env vars are layered in order of priority
*   1. company file defaults specified in teams.py - in the "common" team
*   2. team wide defaults that apply to all objects in the team folder
*   3. object specific defaults - applies to only the object that are declares them
*
* All the maps from the above three places are merged to create final env var
**/
struct EnvironmentVariables {
    1: map<string, string> common
    2: map<string, map<string, string>> modeEnvironments
}

/**
* job config for different modes of execution - with "common" applying to all modes
* usually these are spark or flink conf params like "spark.executor.memory" etc
*
* these confs are layered in order of priority
*   1. company file defaults specified in teams.py - in the "common" team
*   2. team wide defaults that apply to all objects in the team folder
*   3. object specific defaults - applies to only the object that are declares them
*
* All the maps from the above three places are merged to create final conf map
**/
struct ConfigProperties {
    1: map<string, string> common
    2: map<string, map<string, string>> modeConfigs
}

/**
* Cluster config for different modes of execution as a json string - with "common" applying to all modes
* These are settings for creating a new cluster for running the job
*
* these confs are layered in order of priority
*   1. company file defaults specified in teams.py - in the "common" team
*   2. team wide defaults that apply to all objects in the team folder
*   3. object specific defaults - applies to only the object that are declares them
*
*   All the maps from the above three places are merged to create final cluster config
**/
struct ClusterConfigProperties {
    1: map<string, string> common
    2: map<string, map<string, string>> modeClusterConfigs
}

struct TableInfo {
    // fully qualified table name
    1: optional string table

    // if not present we will pull from defaults
    // needed to enumerate what partitions are in a range
    100: optional string partitionColumn
    101: optional string partitionFormat
    102: optional Window partitionInterval
    // Offset from the UTC epoch/day boundary used when interpreting partitionInterval.
    103: optional Window partitionOffset

    /**
    * If isCumulative is true, then for a given output partition any single partition from input on or after the output
    * is sufficient. What this means is that latest available partition prior to end cut off will be used.
    **/
    200: optional bool isCumulative

    /**
    * Some append only, unpartitioned tables (bigtable subscriptions, delta tables) may require special handling for
    * when to trigger the downstream nodes. For this case we add a trigger expression that is going to be compared
    * against the end of the partition range to compute.
    * For example. If there's a timestamp in defined.
    * triggerExpr = DATE(MAX(ts)) generates a sensor that triggers when
    * SELECT DATE(MAX(ts)) > DATE '{{ ds }}' FROM table
    * returns true.
    * This only takes effect for the external tables -> Chronon tables entrypoint. All chronon tables are partitioned.
    **/
    300: optional string triggerExpr

    /**
    * Indicates the source table uses a timestamp or date column for time-based filtering
    * instead of traditional Hive-style string partitioning. When true, partitionColumn
    * should reference the timestamp/date column. The engine derives virtual partitions
    * via MIN/MAX of that column and uses a MAX-based sensor check.
    **/
    400: optional bool timePartitioned
}

struct TableDependency {
    // fully qualified table name
    1: optional TableInfo tableInfo

    // DEPENDENCY_RANGE_LOGIC
    // 1. get final start_partition, end_partition
    // 2. break into step ranges
    // 3. for each dependency
    //     a. dependency_start: max(query.start - startOffset, startCutOff)
    //     b. dependency_end: min(query.end - endOffset, endCutOff)
    2: optional Window startOffset
    3: optional Window endOffset
    4: optional string startCutOff
    5: optional string endCutOff
    // indicates a soft dependency at the node level that doesn't warrant a table presence check
    6: optional bool isSoftNodeDependency

    7: optional string semanticHash

    /**
    * JoinParts could use data from batch backfill-s or upload tables when available
    * When not available they shouldn't force computation of the backfills and upload tables.
    **/
    201: optional bool forceCompute
}

enum KvScanStrategy {
    ALL = 0
    LATEST = 1
}

struct KvInfo {
    1: optional string cluster
    2: optional string table
    3: optional string keyBase64
}

struct KvDependency {
    1: optional KvInfo kvInfo

    10: optional i64 startMillis
    11: optional i64 endMillis

    20: optional KvScanStrategy scanStrategy
}

struct ExecutionInfo {
    # information that needs to be present on every physical node
    1: optional EnvironmentVariables env
    2: optional ConfigProperties conf
    3: optional i64 dependencyPollIntervalMillis
    4: optional i64 healthCheckIntervalMillis
    5: optional ClusterConfigProperties clusterConf

    # relevant for batch jobs
    # temporal workflow nodes maintain their own cron schedule
    10: optional string offlineSchedule
    # day-denominated regardless of output partitionInterval: sub-daily nodes get a day's
    # worth of partitions (24h / interval) per step
    11: optional i32 stepDays
    12: optional bool historicalBackfill
    13: optional list<TableDependency> tableDependencies
    14: optional TableInfo outputTableInfo
    15: optional bool enableStatsCompute
    16: optional string onlineSchedule

    200: optional list<KvDependency> kvDependencies
    201: optional KvInfo outputKvInfo
    202: optional i64 kvPollIntervalMillis
    # note that batch jobs could in theory also depend on model training runs
    # in which case we will be polling
    # in the future we will add other types of dependencies
}

// Shared constants that can be used in python and scala

// Training CLI Parser keyword arguments
const string INPUT_TABLE_KEYWORD = "--input-table"
const string START_DS_KEYWORD = "--start-ds"
const string END_DS_KEYWORD = "--end-ds"
