---
title: "Testing GroupBy and Join"
order: 1
---

# Testing `GroupBy`s and `Join`s

This doc assumes that you have already a `GroupBy`, `Join`, or `StagingQuery` created that you wish to test.

Testing will be for one of the following flows:

1. [Analyze](#analyze) - A quick job that sanity checks your `GroupBy` or `Join` by returning some metadata such as the output schema, row counts, etc. This is a lightweight validation that can optionally be used before running a more compute intensive backfill.
2. [Backfill](#backfill) - Computes historical feature values and writes them to the output table for inspection.
3. [Serve](#serve) - A multi-part workflow that includes uploading data to the online KV store, in batch and streaming (if applicable), and fetching feature values for that entity.

The first step in any of these workflows is to [Compile](#compile) the Chronon entity which you wish to test.

## Install the Zipline CLI
To run the commands below, you need to have the Zipline CLI installed. If you haven't done so already, you can 
download the latest .whl release from https://github.com/zipline-ai/chronon/releases,
and with the .whl file you can install via a python package manager like pip:

```bash
pip3 install $ZIPLINE_WHEEL_PATH
```

## Compile

Compiling converts the python Chronon definition into thrift - as json - which can be passed to the Chronon engine.

From the root of the main Chronon directory, run:

```bash
zipline compile
```
This will compile all Chronon configuration objects and produce their compiled files in the `compiled/` directory. 
For example, if you have a `Join` named `v1` defined at `joins/team/join.py`, 
then the compiled file will be at `compiled/joins/team/join.v1`.

## Analyze

The analyzer will compute the following information by simply taking a Chronon config path.
* Heavy hitters of all the key columns - for users to filter out keys that are not genuine, but skewed.
* A simple count of items by year - to sanity check the timestamps.
* A row count - to give users a sense of how large the data is.
* Output schemas - to quickly validate the sql statements and understand the output schema.
* Timestamp Validations for configs for GroupBys and Joins (with EventSources) 
  * Confirms that timestamp columns are not all NULLs
  * Confirms that timestamp columns are in epoch milliseconds in the range between 1971-01-01 and 2099-01-01
* Validations for JOIN config - to make sure the join conf is valid for backfill. Here is a list of items we validate:
  * Confirm Join keys are matching on the left and right side
  * Confirm you have access to all the tables involved in the join
  * Confirm the underlying tables have partitions going as far back as needed for backfill aggregation
  * Provide a list of GroupBys which have `startPartition` filters for sanity check. GroupBy `startPartition` should not be after Join backfill start partition.

Please note that these validations will also be executed as a prerequisite check for join backfill. In the event of any validation failure, the job backfill will be aborted to ensure better efficiency.


### How to run analyzer

```
# run the analyzer
zipline run compiled/joins/<path_to_conf_file> --mode=analyze --skew-detection
```

Optional parameters:

`--skew-detection`: enable skewed data analysis - include the frequent key analysis in output, only output schema if not specified

`--start-date` : Finds heavy hitters & time-distributions for a specified start date. Default 3 days prior to "today"

`--count` : Finds the specified number of heavy hitters approximately. The larger this number is the more accurate the analysis will be. Default 128

`--sample` : Sampling ratio - what fraction of rows into incorporate into the heavy hitter estimate. Default 0.1

## Backfill

You can run the compiled configs (either `Join`/`GroupBy`/`StagingQuery`) with `backfill` mode to generate data.

```sh
zipline run compiled/joins/team/join.v1 --mode=backfill --start-ds=2022-07-02 --end-ds=2022-07-10
```

This runs a spark job which will compute the data.

Most of the time, backfilling a `Join` is what you want because this backfills any `GroupBy`s used in the join as well.

To backfill a `GroupBy` directly, you must specify the `--start-partition` flag to control where the backfill starts:

```sh
zipline run compiled/group_bys/team/group_by.v1 --mode=backfill --start-partition=2022-07-02 --end-ds=2022-07-10
```

Backfilling `GroupBy` is usually an analytics use-case, as online uploads have their own flow (see [Serve](#serve) below).

## Serve

You can either serve a `GroupBy` on its own, or a `Join` if you wish to fetch results for many `GroupBy`s together in one request.

Manually running the test workflow for serving is optional. If you've validated that your Chronon config generates the correct results in backfill runs, then most of the time you can simply merge your config and let the scheduled airflow runs orchestrate the necessary steps to enable serving.

### GroupBy Upload

You need to upload some data into a KV store to be able to fetch your data. For a join, this means:

1. All the relevant `GroupBy`'s data should be uploaded to the KV store.
2. The `Join`'s metadata should be uploaded to the KV store (this allows Chronon to know which `GroupBy`s to fetch when the request comes in).

For a `GroupBy`, you just need to run one upload.

First, make sure your `Join`s and `GroupBy`s as marked as `online=True`. This is an argument you can pass to both objects like so:

```python
your_join = Join(
    ...
    online=True,
)

# or

your_group_by = GroupBy(
    ...
    online=True,
)
```

Once you have marked a particular Chronon definition as online and compiled it, you need to upload the relevant `GroupBy`'s data into your KV store. 

The following command will generate a table with key-value bytes that's ready for upload to your KV store:

```bash
zipline run compiled/group_bys/your_group_by.v1 --mode upload --ds 2023-12-01
```

and then to actually upload to your KV store: 
```bash
zipline run compiled/group_bys/your_group_by.v1 --mode upload-to-kv --ds 2023-12-01
```

The next step is to move the data from these tables into your KV store. For this, you need to use your internal implementation of KV store integration. This should already be what your Airflow jobs are configured to run, so you can always rely on that, or if your Chronon team has provided you with a manual upload command to run you can use that.

### Deploy Flink streaming job

If your GroupBy is a streaming GroupBy, you will want to also run the streaming job that will aggregate and push intermediate data to the KV store. Flink is the preferred streaming engine, though Spark streaming is also supported but not recommended.

```sh
zipline run compiled/group_bys/your_group_by.v1 --mode streaming deploy --version-check --disable-cloud-logging [--latest-savepoint| --no-savepoint | --custom-savepoint]
```

Only one of the three savepoint options below can be specified:

1. `--latest-savepoint` will deploy the streaming job from the latest savepoint available. Based on the job id, the Flink checkpoint directory will be checked and scanned for the latest checkpoint number.  
2. `--no-savepoint` will deploy the streaming job without a savepoint.

3. `--custom-savepoint` will deploy the streaming job from a custom savepoint path to start from.

`--version-check`: Checks if Zipline engine version of running streaming job is different from the Zipline engine version running locally on the CLI (on Airflow or laptop) and deploys the job if they are different.

`--disable-cloud-logging`: Disables cloud logging from being outputted locally on the CLI. This is **required** when running the streaming CLI command on Airflow so that the command will exit, or otherwise the streaming CLI command with cloud logging outputted will run in the foreground and not exit as streaming jobs are long lived.

### Metadata upload
After running `upload`, `upload-to-kv`, and `streaming` for all the `GroupBy`s in a `Join`, in order to fetch features for this `Join`, we need to 
run a metadata upload of the `Join` config to the KV store. This will be used by the fetcher below to understand 
how to deserialize the batch (and streaming) data and provide the feature values.

```bash
zipline run compiled/joins/your_join.v1 --mode metadata-upload --ds 2023-12-01
```

Chronon also supports uploading `GroupBy` metadata on its own, in case you want to fetch directly from a `GroupBy` without a `Join`. The process is similar - just run the following command to upload the `GroupBy` metadata so that the fetcher knows
```bash
zipline run compiled/group_bys/your_group_by.v1 --mode metadata-upload --ds 2023-12-01
```

### Fetching results

You can fetch features for your uploaded conf by its name and with a json of keys. Json types needs to match the key types. So a string should be quoted, an int/long shouldn't be quoted. Note that the json key cannot contain spaces - or it should be properly quoted. Similarly, when a join has multiple keys, there should be no space around the comma. For example, `-k '{"user_id":123,"merchant_id":456}'`. The fetch would return partial results if only some keys are provided. It would also output an error message indicating the missing keys, which is useful in case of typos.

```bash
zipline run compiled/joins/your_join.v1 --mode=fetch -k '{"user_or_visitor":"u_106386039"}' --name <JOIN_NAME>
```

Note that this is simply the test workflow for fetching. For production serving, see the [Serving documentation](./Serve.md).


### Online offline consistency metrics computation

After enabling online serving, you may be interested in the online offline consistency metrics for the job. 

See details on how to do that [here](../management_in_production/Online_Offline_Consistency.md).

## Status

The `zipline status` command checks the current state of a deployed `GroupBy` job — either the batch upload to the KV store or the streaming consumer lag.

### Upload-to-KV Status

Checks whether batch data has been uploaded to the KV store by fetching the `GroupByServingInfo`:

```bash
zipline status compiled/group_bys/team/your_group_by.v1 --mode upload-to-kv
```

Sample output:
```json
{"batchEndDate":"2024-06-15"}
```

If the serving info cannot be fetched (e.g. batch upload hasn't run yet), the output contains an error message instead:
```json
{"error":"Failed to get serving info for team.your_group_by. Make sure batch upload has completed successfully."}
```

### Streaming Status

Checks the Kafka consumer lag for a streaming `GroupBy`:

```bash
zipline status compiled/group_bys/team/your_group_by.v1 --mode streaming
```

Sample output:
```json
{"lag":42}
```

A lag of `0` means the streaming job is fully caught up. A lag of `-1` means the consumer group has no committed offsets (the streaming job likely hasn't started):
```json
{"lag":-1}
```

### Options

| Option | Description |
|---|---|
| `--mode` | Required. `upload-to-kv` or `streaming`. |
| `--repo` | Path to the chronon repo root. Default: `.` |
| `--online-jar` | Path to the online jar. Required when `CLOUD_PROVIDER` is not set. Can also be set via `CHRONON_ONLINE_JAR` env var. |
| `--online-class` | Api implementation class. Required for `upload-to-kv` when `CLOUD_PROVIDER` is not set. Can also be set via `CHRONON_ONLINE_CLASS` env var. |
| `--artifact-prefix` | Remote artifact URI for cloud jar downloads. |
| `--version` | Chronon version for cloud jar downloads. |
| `--enable-debug` | Enables verbose debug logging. |

When `CLOUD_PROVIDER` is set (e.g. `GCP`, `AWS`, `AZURE`), the required jars and online class are resolved automatically using `--artifact-prefix` and `--version`. In OSS environments without a cloud provider, you must supply `--online-jar` (and `--online-class` for `upload-to-kv`) explicitly.

## Useful tips to work with Chronon

### Getting the argument list

Most of the above commands run a scala process under the python shim, which could take a lot of arguments. You can see the help of the scala process by using `--sub-help`.

```bash
[gateway_machine] python3 ~/.local/bin/run.py --mode=fetch --sub-help
```

This example will print out the args that the fetch mode will take. You can do the same for other modes as well.

```bash
Running command: java -cp /tmp/spark_uber.jar ai.chronon.spark.Driver fetch --help
  -k, --key-json  <arg>        json of the keys to fetch
  -n, --name  <arg>            name of the join/group-by to fetch
      --online-class  <arg>    Fully qualified Online.Api based class. We expect
                               the jar to be on the class path
  -o, --online-jar  <arg>      Path to the jar contain the implementation of
                               Online.Api class
  -t, --type  <arg>            the type of conf to fetch Choices: join, group-by
  -Zkey=value [key=value]...
  -h, --help                   Show help message
```
