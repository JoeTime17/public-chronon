namespace py gen_thrift.api
namespace java ai.chronon.api

include "common.thrift"
include "observability.thrift"

// cd /path/to/chronon
// thrift --gen py -out api/python/ api/thrift/api.thrift

struct Query {
    1: optional map<string, string> selects
    2: optional list<string> wheres
    3: optional string startPartition
    4: optional string endPartition
    5: optional string timeColumn
    6: optional list<string> setups
    7: optional string mutationTimeColumn
    8: optional string reversalColumn

    /**
    * Chronon expects all its batch input data to be date/time partitioned.
    * We in-turn produce partitioned outputs.
    **/
    20: optional string partitionColumn

    /**
    * Partition format in the java DateFormatter spec:
    * see: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
    **/
    21: optional string partitionFormat

    /**
    * Indicates the timespan of a given interval of the source
    **/
    22: optional common.Window partitionInterval

    /**
    * Indicates how often this source is typically delayed by.
    * Should be a multiple of partitionInterval
    **/
    23: optional common.Window partitionLag

    /**
    * Additional partitions to be used in sensing that the source data has landed.
    * Should be a full partition string, such as `hr=23:00'
    **/
    24: optional list<string> subPartitionsToWaitFor

    /**
    * Indicates whether the source table uses a timestamp or date column for time-based
    * filtering instead of traditional Hive-style string partitioning.
    * When true, partition_column should be set to the timestamp/date column name.
    * The engine will:
    *   - Derive virtual partitions via MIN/MAX of the partition column
    *   - Use timestamp-based WHERE clauses for scanning
    *   - Convert the timestamp column to formatted date strings for output partitioning
    * Common for BigQuery, Snowflake, and Delta Lake tables that aren't Hive-partitioned.
    **/
    25: optional bool timePartitioned

    /**
    * Offset from the UTC epoch/day boundary used when interpreting partitionInterval.
    * For example, a 3-hour source partitioned at 01:00, 04:00, ... should set
    * partitionInterval=3h and partitionOffset=1h.
    **/
    26: optional common.Window partitionOffset

}
 
/**
    Staging Query encapsulates arbitrary spark computation. One key feature is that the computation follows a
    "fill-what's-missing" pattern. Basically instead of explicitly specifying dates you specify two macros.
    `{{ start_date }}` and `{{end_date}}`. Chronon will pass in earliest-missing-partition for `start_date` and
    execution-date / today for `end_date`. So the query will compute multiple partitions at once.
 */
struct StagingQuery {
    /**
    * Contains name, team, output_namespace, execution parameters etc. Things that don't change the semantics of the computation itself.
    **/
    1: optional MetaData metaData

    /**
    * Arbitrary spark query that should be written with `{{ start_date }}`, `{{ end_date }}` and `{{ latest_date }}` templates
    *      - `{{ start_date }}` will be set to this user provided start date, future incremental runs will set it to the latest existing partition + 1 day.
    *      - `{{ start_date(offset=-10, lower_bound='2023-01-01', upper_bound='2024-01-01') }}` will shift the date back one day and bound it with lower and upper bounds.
    *      - `{{ end_date }}` is the end partition of the computing range. offsetting and bounding the end_date also works as described above.
    *      - `{{ latest_date }}` is the end partition independent of the computing range (meant for cumulative sources). offsetting and bounding the end_date also works as described above.
    *      - `{{ max_date(table=namespace.my_table) }}` is the max partition available for a given table.
    *
    **/
    2: optional string query

    /**
    * on the first run, `{{ start_date }}` will be set to this user provided start date, future incremental runs will set it to the latest existing partition + 1 day.
    **/
    3: optional string startPartition

    /**
    * Spark SQL setup statements. Used typically to register UDFs.
    **/
    4: optional list<string> setups

    /**
    * By default, spark is the compute engine. You can specify an override (eg. bigquery, etc.)
    **/
    5: optional EngineType engineType

    6: optional list<common.TableDependency> tableDependencies

    /* If specified, will recompute the output of this StagingQuery for the given number of days after initial computation
    * Should be used in one of two scenarios:
    * 1: When the source data is changed in-place (i.e. existing partitions overwritten with new data each day up to X days later)
    * 2: When you want partially mature aggregations (i.e. a 7 day window, but start computing it from day 1, and refresh it for the next 6 days)
    * Both of these cases are common labeling flows.
    **/
    20: optional i32 recomputeDays
}

struct EventSource {
    /**
    * Table currently needs to be a 'ds' (date string - yyyy-MM-dd) partitioned hive table. Table names can contain subpartition specs, example db.table/system=mobile/currency=USD
    **/
    1: optional string table

    /**
    * Topic is a kafka table. The table contains all the events historically came through this topic.
    **/
    2: optional string topic

    /**
    * The logic used to scan both the table and the topic. Contains row level transformations and filtering expressed as Spark SQL statements.
    **/
    3: optional Query query

    /**
    * If each new hive partition contains not just the current day's events but the entire set of events since the begininng. The key property is that the events are not mutated across partitions.
    **/
    4: optional bool isCumulative
}


/**
    Entity Sources represent data that gets mutated over-time - at row-level. This is a group of three data elements.
    snapshotTable, mutationTable and mutationTopic. mutationTable and mutationTopic are only necessary if we are trying
    to create realtime or point-in-time aggregations over these sources. Entity sources usually map 1:1 with a database
    tables in your OLTP store that typically serves live application traffic. When mutation data is absent they map 1:1
    to `dim` tables in star schema.
 */
struct EntitySource {
    /**
    Snapshot table currently needs to be a 'ds' (date string - yyyy-MM-dd) partitioned hive table.
    */
    1: optional string snapshotTable

    /**
    Topic is a kafka table. The table contains all the events that historically came through this topic.
    */
    2: optional string mutationTable

    /**
    The logic used to scan both the table and the topic. Contains row level transformations and filtering expressed as Spark SQL statements.
    */
    3: optional string mutationTopic

    /**
    If each new hive partition contains not just the current day's events but the entire set of events since the begininng. The key property is that the events are not mutated across partitions.
    */
    4: optional Query query
}

struct ExternalSource {
    1: optional MetaData metadata
    2: optional TDataType keySchema
    3: optional TDataType valueSchema
}

/**
* Output of a Join can be used as input to downstream computations like GroupBy or a Join.
* Below is a short description of each of the cases we handle.
* Case #1: a join's source is another join [TODO]
*   - while serving, we expect the keys for the upstream join to be passed in the request.
*     we will query upstream first, and use the result to query downstream
*   - while backfill, we will backfill the upstream first, and use the table as the left of the subsequent join
*   - this is currently a "to do" because users can achieve this by themselves unlike case 2:
* Case #2: a join is the source of another GroupBy
*   - We will support arbitrarily long transformation chains with this.
*   - for batch (Accuracy.SNAPSHOT), we simply backfill the join first and compute groupBy as usual
*     - will substitute the joinSource with the resulting table and continue computation
*     - we will add a "resolve source" step prior to backfills that will compute the parent join and update the source
*   - for realtime (Accuracy.TEMPORAL), we need to do "stream enrichment"
*     - we will simply issue "fetchJoin" and create an enriched source. Note the join left should be of type "events".
**/
struct JoinSource {
    1: optional Join join
    2: optional Query query
}

union Source {
    1: EventSource events
    2: EntitySource entities
    3: JoinSource joinSource
    4: ModelTransforms modelTransforms
}

enum Operation {
    // Un-Deletable operations - Monoids
    // Once an aggregate is created from a group of elements,
    // asking to delete on of the elements is invalid - and ignored.
    MIN = 0
    MAX = 1
    FIRST = 2
    LAST = 3
    UNIQUE_COUNT = 4
    APPROX_UNIQUE_COUNT = 5

    // Deletable operations - Abelian Groups
    // Once an aggregate is created from a group of elements,
    // deletion of any particular element can be done freely.
    COUNT = 6
    SUM = 7
    AVERAGE = 8
    VARIANCE = 9
    SKEW = 10
    KURTOSIS = 11
    APPROX_PERCENTILE = 12

    LAST_K = 13
    FIRST_K = 14,
    TOP_K = 15,
    BOTTOM_K = 16

    HISTOGRAM = 17, // use this only if you know the set of inputs is bounded

    APPROX_FREQUENT_K = 18, // returns map(string -> int) of top k most frequent elems
    APPROX_HEAVY_HITTERS_K = 19,  // returns skewed elements of the column upto size k
    UNIQUE_TOP_K = 20  // returns top k unique elements based on frequency
}



/**
    Chronon provides a powerful aggregations primitive - that takes the familiar aggregation operation, via groupBy in
    SQL and extends it with three things - windowing, bucketing and auto-explode.
 */
struct Aggregation {
    /**
    *  The column as specified in source.query.selects - on which we need to aggregate with.
    **/
    1: optional string inputColumn
    /**
    * The type of aggregation that needs to be performed on the inputColumn.
    **/
    2: optional Operation operation
    /**
    * Extra arguments that needs to be passed to some of the operations like LAST_K, APPROX_PERCENTILE.
    **/
    3: optional map<string, string> argMap

    /**
    For TEMPORAL case windows are sawtooth. Meaning head slides ahead continuously in time, whereas, the tail only hops ahead, at discrete points in time. Hop is determined by the window size automatically. The maximum hop size is 1/12 of window size. You can specify multiple such windows at once.
      - Window > 12 days  -> Hop Size = 1 day
      - Window > 12 hours -> Hop Size = 1 hr
      - Window > 1hr      -> Hop Size = 5 minutes
    */
    4: optional list<common.Window> windows

    /**
    This is an additional layer of aggregation. You can key a group_by by user, and bucket a “item_view” count by “item_category”. This will produce one row per user, with column containing map of “item_category” to “view_count”. You can specify multiple such buckets at once
    */
    5: optional list<string> buckets
}

// used internally not exposed - maps 1:1 with a field in the output
struct AggregationPart {
    1: optional string inputColumn
    2: optional Operation operation
    3: optional map<string, string> argMap
    4: optional common.Window window
    5: optional string bucket
}

enum Accuracy {
    TEMPORAL = 0,
    SNAPSHOT = 1
}

enum EngineType {
    SPARK = 0,
    BIGQUERY = 1,
    SNOWFLAKE = 2,

}

enum Environment {
    PROD = 0,
    CANARY = 1,
}

/**
* contains configs params that don't change the contents of the output.
**/
struct MetaData {
    1: optional string name


    2: optional string team

    // will be set by the compiler based on what user sets (currently only for GB and Join)
    3: optional string version

    4: optional string outputNamespace

    /**
    * By default we will just partition the output by the date column - set via "spark.chronon.partition.column"
    * With this we will partition the output with the specified additional columns
    **/
    5: optional list<string> additionalOutputPartitionColumns

    6: optional map<string, string> tableProperties

    // tag_key -> tag_value - tags allow for repository wide querying, deprecations etc
    // this is object level tag - applies to all columns produced by the object - GroupBy, Join, Model etc
    20: optional map<string, string> tags
    // column -> tag_key -> tag_value
    21: optional map<string, map<string, string>> columnTags
    // A map of output column name to semantic hash for that column
    // Includes the source expression of the input column, the source table, filters, and key and time expressions
    // Does not need to include anything that effects the output name (i.e. aggregation operation, window, bucket)
    22: optional map<string, string> columnHashes

    // marking this as true means that the conf can be served online
    // once marked online, a conf cannot be changed - compiling the conf won't be allowed
    100: optional bool online

    // marking this as true means that the conf automatically generates a staging copy
    // this flag is also meant to help a monitoring system re-direct alerts appropriately
    101: optional bool production

    102: optional string sourceFile

    // users can put anything they want in here, but the compiler shouldn't
    103: optional string customJson

    104: optional list<Environment> environments

    // enable job to compute consistency metrics
    200: optional bool consistencyCheck

    // percentage of online serving requests to log to warehouse
    201: optional double samplePercent

    // percentage of online serving requests used to compute consistency metrics
    202: optional double consistencySamplePercent

    // specify how to compute drift
    203: optional observability.DriftSpec driftSpec

    # information that needs to be present on every physical node
    204: optional common.ExecutionInfo executionInfo
}

// This has to be 0-indexed for Java usage
enum ConfType {
    GROUP_BY = 0,
    JOIN = 1,
    STAGING_QUERY = 2,
    MODEL = 3,
    MODEL_TRANSFORMS = 4,
}

struct Conf {
    1: optional string name
    2: optional string hash
    3: optional string contents
    4: optional ConfType confType
    5: optional string localPath
}

// Equivalent to a FeatureSet in chronon terms
struct GroupBy {
    1: optional MetaData metaData
    // CONDITION: all sources select the same columns
    // source[i].select.keys() == source[0].select.keys()
    2: optional list<Source> sources
    // CONDITION: all keyColumns are selected by each of the
    // set(sources[0].select.keys()) <= set(keyColumns)
    3: optional list<string> keyColumns
    // when aggregations are not specified,
    // we assume the source is already grouped by keys
    4: optional list<Aggregation> aggregations
    5: optional Accuracy accuracy
    // support for offline only for now
    7: optional list<Derivation> derivations
}

struct JoinPart {
    1: optional GroupBy groupBy
    2: optional map<string, string> keyMapping
    3: optional string prefix
    // useLongName on joinPart is inherited from the join, but needs to be set at the JoinPart level for the spark engine
    10: optional bool useLongNames
}

struct ExternalPart {
    1: optional ExternalSource source
    // what keys on the left becomes what keys in the external source
    // currently this only supports renaming, in the future this will run catalyst expressions
    2: optional map<string, string> keyMapping
    3: optional string prefix
}

struct Derivation {
    1: optional string name
    2: optional string expression
    // do not put tags here as they can make the payload heavy
    // in the python api we will expose tags but only duck type attach them to the object
    // and when we ship it to orchestrator / agent etc, we will strip the tags
}

// A Temporal join - with a root source, with multiple groupby's.
struct Join {
    1: optional MetaData metaData
    2: optional Source left
    3: list<JoinPart> joinParts
    // map of left key column name and values representing the skew keys
    // these skew keys will be excluded from the output
    // specifying skew keys will also help us scan less raw data before aggregation & join
    // example: {"zipcode": ["94107", "78934"], "country": ["'US'", "'IN'"]}
    4: optional map<string,list<string>> skewKeys
    // users can register external sources into Api implementation. Chronon fetcher can invoke the implementation.
    // This is applicable only for online fetching. Offline this will not be produce any values.
    5: optional list<ExternalPart> onlineExternalParts
    7: optional list<BootstrapPart> bootstrapParts
    // Fields on left that uniquely identifies a single record
    8: optional list<string> rowIds
    /**
    * List of a derived column names to the expression based on joinPart / externalPart columns
    * The expression can be any valid Spark SQL select clause without aggregation functions.
    *
    * joinPart column names are automatically constructed according to the below convention
    *  `{join_part_prefix}_{group_by_name}_{input_column_name}_{aggregation_operation}_{window}_{by_bucket}`
    *  prefix, window and bucket are optional. You can find the type information of columns using the analyzer tool.
    *
    * externalPart column names are automatically constructed according to the below convention
    *  `ext_{external_source_name}_{value_column}`
    * Types are defined along with the schema by users for external sources.
    *
    * Including a column with key "*" and value "*", means that every raw column will be included along with the derived
    * columns.
    **/
    9: optional list<Derivation> derivations
    // If useLongNames is true, then we include the groupby team and name in the column names, prefixes are always included
    50: optional bool useLongNames
}

struct BootstrapPart {
    1: optional MetaData metaData
    // Precomputed table that is joinable to rows on the left
    2: optional string table
    3: optional Query query
    // Join keys to the left. If null, defaults to rowIds on the Join.
    4: optional list<string> keyColumns
}

// This is written by the bulk upload process into the metaDataset
// streaming uses this to
//     1. gather inputSchema from the kafka stream
//     2. to check if the groupByJson is the same as the one it received - and emits a
struct GroupByServingInfo {
    1: optional GroupBy groupBy
    // a. When groupBy is
    //  1. temporal accurate - batch uploads irs, streaming uploads inputs
    //                         the fetcher further aggregates irs and inputs into outputs
    //  2. snapshot accurate - batch uploads outputs, fetcher doesn't do any further aggregation
    // irSchema and outputSchema are derivable once inputSchema is known


    // schema before applying select expressions
    2: optional string inputAvroSchema
    // schema after applying select expressions
    3: optional string selectedAvroSchema
    // schema of the keys in kv store
    4: optional string keyAvroSchema
    // "date", in 'yyyy-MM-dd' format, the bulk-upload data corresponds to
    // we need to scan streaming events only starting this timestamp
    // used to compute
    //       1. batch_data_lag = current_time - batch_data_time
    //       2. batch_upload_lag = batch_upload_time - batch_data_time
    5: optional string batchEndDate
    6: optional string dateFormat

    // partitionInterval and partitionOffset of the upload partition grid; absent means daily at midnight UTC
    7: optional common.Window partitionInterval
    8: optional common.Window partitionOffset

    /**
    * Authoritative watermark of the last upload: the exact epoch boundary (exclusive) the batch
    * data covers up to. Streaming events at or after this timestamp are merged in by the fetcher.
    * When absent, derived by parsing batchEndDate with dateFormat under a daily spec.
    **/
    9: optional i64 batchEndTs
}

// DataKind + TypeParams = DataType
// for primitive types there is no need for params
// IMPORTANT: Declaration order must match enum values (ordinal == value) for TSimpleJSONProtocol compatibility
enum DataKind {
    // non parametric types
    BOOLEAN = 0,
    BYTE = 1,
    SHORT = 2,
    INT = 3,
    LONG = 4,
    FLOAT = 5,
    DOUBLE = 6,
    STRING = 7,
    BINARY = 8,
    DATE = 9,
    TIMESTAMP = 10,

    // parametric types
    MAP = 11,
    LIST = 12,
    STRUCT = 13,

    // non-parametric but placed here to maintain ordinal == value ordering
    DECIMAL = 14,
}

struct DataField {
    1: optional string name
    2: optional TDataType dataType
}

// TDataType because DataType has idiomatic implementation in scala and py
struct TDataType {
    1: DataKind kind
    2: optional list<DataField> params
    3: optional string name // required only for struct types
}

struct DataSpec {
    1: optional TDataType schema
    2: optional list<string> partitionColumns
    3: optional i32 retentionDays
    4: optional map<string, string> props
}

struct Team {
    1: optional string name
    2: optional string description
    3: optional string email

    10: optional string outputNamespace
    11: optional map<string, string> tableProperties

    20: optional common.EnvironmentVariables env
    21: optional common.ConfigProperties conf
    22: optional common.ClusterConfigProperties clusterConf

}

enum DataModel {
    ENTITIES = 0
    EVENTS = 1
}

// ====================== Model related concepts ======================

/**
 * A Model represents a single ML model and includes details to help with model inference (batch & online)
 * as well as model training and deployment orchestration
*/
struct Model {
    1: optional MetaData metaData

    // Model + model backend specific details necessary to perform inference
    2: optional InferenceSpec inferenceSpec

    // Spark SQL queries to transform input data to the format expected by the model
    3: optional map<string, string> inputMapping

    // Spark SQL queries to transform model output to desired output format
    4: optional map<string, string> outputMapping

    // Schema of the output fields generated by the model
    5: optional TDataType valueSchema

    // orchestration related fields

    // where trained model artifacts are stored
    6: optional string modelArtifactBaseUri

    // configs related to orchestrating model training jobs
    7: optional TrainingSpec trainingConf

    // configs related to orchestrating model deployment
    8: optional DeploymentSpec deploymentConf
}

enum ModelBackend {
    VertexAI = 0
    SageMaker = 1
    // other backends that we support will go here
}

struct InferenceSpec {
    1: optional ModelBackend modelBackend

    // Captures params such as batch size that are used to configure calls
    2: optional map<string, string> modelBackendParams

    // If present, indicates the resources needed for the model inference calls
    // Not needed for hosted models (e.g. OpenAI's embedding models), useful for custom models on VertexAI/SageMaker
    // to scale batch inference jobs
    3: optional ResourceConfig resourceConfig
}

struct ResourceConfig {
    1: optional i32 minReplicaCount
    2: optional i32 maxReplicaCount
    3: optional string machineType
}

/**
* ModelTransforms allows us to take the output of an existing source (E.g. Event / Entity / Join) and
* enrich it with 1/more model outputs. This in-turn can be fed into downstream computations like GroupBy or hit directly
* via the fetcher. The GroupBy path allows for async materialization of model outputs to the online KV store for low latency
* serving. The fetcher path allows for on-demand model inference during online serving (at the cost of higher latency / more
* model inference calls).
**/
struct ModelTransforms {
    1: optional list<Source> sources
    2: optional list<Model> models

    // fields from the source that we want to passthrough alongside the model outputs
    3: optional list<string> passthroughFields
    4: optional MetaData metaData

    // Schema of the keys fields in the source that are fed into the models
    // If the source is a JoinSource, this can be skipped as we look it up from the join metadata
    5: optional TDataType keySchema
}

struct TrainingSpec {
    // source pointing to training data - this could be a Chronon generated source / external source
    1: optional Source trainingDataSource

    // window we expand to gather training data for each scheduled job
    2: optional common.Window trainingDataWindow

    // schedule over which we want training jobs kicked off (when the training data lands)
    3: optional string schedule

    // container image to use for training jobs
    4: optional string image

    // python module to use as entry point for training jobs
    5: optional string pythonModule

    // resources to use for training jobs
    6: optional ResourceConfig resourceConfig

    // additional job configs to pass to the training job
    7: optional map<string, string> jobConfigs
}

struct ServingContainerConfig {
    // container image to use for serving
    1: optional string image

    // REST route for container health check
    2: optional string servingHealthRoute

    // REST route for model prediction
    3: optional string servingPredictRoute

    // additional environment variables to pass to the serving container
    4: optional map<string, string> servingContainerEnvVars
}

/**
* Endpoints are the serving targets for deployed models.
**/
struct EndpointConfig {
    1: optional string endpointName
    2: optional map<string, string> additionalConfigs
}

enum DeploymentStrategyType {
    // deploys the model in a blue-green fashion (~2x capacity) to another endpoint and gradually ramps traffic
    BLUE_GREEN = 0
    // deploys the model in a rolling manner by gradually scaling down existing instances and scaling up new instances
    ROLLING = 1
    // deploys the model immediately to the endpoint without any traffic ramping
    IMMEDIATE = 2
}

struct Metric {
    1: optional string name
    2: optional double threshold
}

struct RolloutStrategy {
    1: optional DeploymentStrategyType rolloutType

    // In case of Blue/Green or Rolling deployments, we can specify traffic ramps and validation durations
    // to gradually shift traffic to the new model deployment
    // e.g. validationTrafficPercentRamps = [10, 30, 60, 100]
    //      validationTrafficDurationMins = [15, 30, 60, 120]
    // means we will ramp traffic to 10% for first 15 mins, then 30% for next 30 mins, ..
    2: optional list<i32> validationTrafficPercentRamps
    3: optional list<i32> validationTrafficDurationMins

    // list of metrics and their thresholds that need to be validated at each ramp step
    4: optional list<Metric> rolloutMetricThresholds
}

struct DeploymentSpec {
    1: optional ServingContainerConfig containerConfig
    2: optional EndpointConfig endpointConfig
    3: optional ResourceConfig resourceConfig
    4: optional RolloutStrategy rolloutStrategy
}
