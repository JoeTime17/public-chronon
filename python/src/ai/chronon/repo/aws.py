import json
import multiprocessing
import os
import time
from typing import List

import boto3

from ai.chronon.logger import get_logger
from ai.chronon.repo.constants import ROUTES, ZIPLINE_DIRECTORY
from ai.chronon.repo.default_runner import Runner
from ai.chronon.repo.utils import (
    JobType,
    check_call,
    check_output,
    extract_filename_from_path,
    get_customer_id,
    split_date_range,
    upload_to_blob_store,
)

LOG = get_logger()

# AWS SPECIFIC CONSTANTS
EMR_ENTRY = "ai.chronon.integrations.aws.EmrServerlessSubmitter"
ZIPLINE_AWS_JAR_DEFAULT = "cloud_aws_lib_deploy.jar"
ZIPLINE_AWS_ONLINE_CLASS_DEFAULT = "ai.chronon.integrations.aws.AwsApiImpl"
ZIPLINE_AWS_FLINK_JAR_DEFAULT = "flink_assembly_deploy.jar"
ZIPLINE_AWS_SERVICE_JAR = "service_assembly_deploy.jar"

LOCAL_FILE_TO_ETAG_JSON = f"{ZIPLINE_DIRECTORY}/local_file_to_etag.json"

class AwsRunner(Runner):
    def __init__(self, args):
        artifacts_bucket_prefix = os.environ.get(
            "ARTIFACT_PREFIX", f"s3://zipline-artifacts-{get_customer_id()}"
        )
        aws_jar_path = AwsRunner.download_zipline_aws_jar(
            ZIPLINE_DIRECTORY, artifacts_bucket_prefix, args["version"], ZIPLINE_AWS_JAR_DEFAULT
        )
        service_jar_path = AwsRunner.download_zipline_aws_jar(
            ZIPLINE_DIRECTORY, artifacts_bucket_prefix, args["version"], ZIPLINE_AWS_SERVICE_JAR
        )
        jar_path = f"{service_jar_path}:{aws_jar_path}" if args["mode"] == "fetch" else aws_jar_path
        self.version = args.get("version", "latest")
        self._args = args

        super().__init__(args, os.path.expanduser(jar_path))

    @staticmethod
    def download_zipline_aws_jar(
        destination_dir: str, artifacts_prefix: str, version: str, jar_name: str
    ):
        s3_client = boto3.client("s3")
        destination_path = f"{destination_dir}/{jar_name}"
        # Parse bucket and optional key prefix from s3://bucket[/prefix]
        stripped = artifacts_prefix.removeprefix("s3://")
        parts = stripped.split("/", 1)
        bucket_name = parts[0]
        key_prefix = parts[1] if len(parts) > 1 else ""
        source_key_name = f"{key_prefix}/release/{version}/jars/{jar_name}" if key_prefix else f"release/{version}/jars/{jar_name}"

        are_identical = (
            AwsRunner.compare_s3_and_local_file_hashes(
                bucket_name, source_key_name, destination_path
            )
            if os.path.exists(destination_path)
            else False
        )

        if are_identical:
            print(f"{destination_path} matches S3 {bucket_name}/{source_key_name}")
        else:
            print(f"{destination_path} does NOT match S3 {bucket_name}/{source_key_name}")
            print(f"Downloading {jar_name} from S3...")

            s3_client.download_file(
                Filename=destination_path, Bucket=bucket_name, Key=source_key_name
            )
            # Persist ETag to prevent downloading the same file next time
            etag = AwsRunner.get_s3_file_hash(bucket_name, source_key_name)
            if os.path.exists(LOCAL_FILE_TO_ETAG_JSON):
                with open(LOCAL_FILE_TO_ETAG_JSON, "r") as file:
                    data = json.load(file)

                # Add the new entry
                data[destination_path] = etag

                # Write the updated dictionary back to the file
                with open(LOCAL_FILE_TO_ETAG_JSON, "w") as file:
                    json.dump(data, file)
            else:
                with open(LOCAL_FILE_TO_ETAG_JSON, "w") as file:
                    data = {destination_path: etag}
                    json.dump(data, file)

        return destination_path

    @staticmethod
    def get_s3_file_hash(bucket_name: str, file_name: str):
        s3_client = boto3.client("s3")
        response = s3_client.head_object(Bucket=bucket_name, Key=file_name)
        return response["ETag"].strip('"')

    @staticmethod
    def get_local_file_hash(file_name: str):
        # read in the json file
        if os.path.exists(LOCAL_FILE_TO_ETAG_JSON):
            with open(LOCAL_FILE_TO_ETAG_JSON, "r") as f:
                data = json.load(f)
                if file_name in data:
                    return data[file_name]
        return None

    @staticmethod
    def compare_s3_and_local_file_hashes(bucket_name: str, s3_file_path: str, local_file_path: str):
        try:
            s3_hash = AwsRunner.get_s3_file_hash(bucket_name, s3_file_path)
            local_hash = AwsRunner.get_local_file_hash(local_file_path)
            print(f"Local hash: {local_hash}, S3 hash: {s3_hash}")
            return s3_hash == local_hash
        except Exception as e:
            print(f"Error comparing files: {str(e)}")
            return False

    def generate_emr_submitter_args(
        self,
        user_args: str,
        job_type: JobType = JobType.SPARK,
        local_files_to_upload: List[str] = None,
    ):
        warehouse_bucket_prefix = os.environ.get(
            "WAREHOUSE_PREFIX", f"s3://zipline-warehouse-{get_customer_id()}"
        )
        artifacts_bucket_prefix = os.environ.get(
            "ARTIFACT_PREFIX", f"s3://zipline-artifacts-{get_customer_id()}"
        )
        s3_files = []
        for source_file in local_files_to_upload:
            # upload to `metadata` folder
            destination_file_path = f"metadata/{extract_filename_from_path(source_file)}"
            s3_files.append(
                upload_to_blob_store(
                    source_file, f"{warehouse_bucket_prefix}/{destination_file_path}"
                )
            )

        s3_file_args = ",".join(s3_files)

        # include jar uri. should also already be in the bucket
        jar_uri = (
            f"{artifacts_bucket_prefix}"
            + f"/release/{self.version}/jars/{ZIPLINE_AWS_JAR_DEFAULT}"
        )

        final_args = (
            "{user_args} --jar-uri={jar_uri} --job-type={job_type} --main-class={main_class}"
        )

        if job_type == JobType.FLINK:
            main_class = "ai.chronon.flink.FlinkJob"
            flink_jar_uri = (
                f"{artifacts_bucket_prefix}"
                + f"/jars/{ZIPLINE_AWS_FLINK_JAR_DEFAULT}"
            )
            return (
                final_args.format(
                    user_args=user_args,
                    jar_uri=jar_uri,
                    job_type=job_type.value,
                    main_class=main_class,
                )
                + f" --flink-main-jar-uri={flink_jar_uri}"
            )

        elif job_type == JobType.SPARK:
            main_class = "ai.chronon.spark.Driver"
            return (
                final_args.format(
                    user_args=user_args,
                    jar_uri=jar_uri,
                    job_type=job_type.value,
                    main_class=main_class,
                )
                + f" --files={s3_file_args}"
            )
        else:
            raise ValueError(f"Invalid job type: {job_type}")

    def run_eks_flink_streaming(self):
        args = self._args.get("args")
        if "check-if-job-is-running" in args:
            raise ValueError("check-if-job-is-running is not yet supported for AWS streaming.")
        if self.latest_savepoint:
            raise ValueError("--latest-savepoint is not yet supported for AWS streaming.")
        if self.version_check:
            raise ValueError("--version-check is not yet supported for AWS streaming.")

        artifacts_bucket_prefix = os.environ.get(
            "ARTIFACT_PREFIX", f"s3://zipline-artifacts-{get_customer_id()}"
        )
        jar_uri = f"{artifacts_bucket_prefix}/release/{self.version}/jars/{ZIPLINE_AWS_JAR_DEFAULT}"
        flink_jar_uri = f"{artifacts_bucket_prefix}/release/{self.version}/jars/{ZIPLINE_AWS_FLINK_JAR_DEFAULT}"

        eks_service_account = os.environ.get("FLINK_EKS_SERVICE_ACCOUNT")
        eks_namespace = os.environ.get("FLINK_EKS_NAMESPACE", "zipline-chronon-flink")
        eks_node_selector = os.environ.get("FLINK_EKS_NODE_SELECTOR")
        aws_region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))

        job_id = self.conf_metadata_name.replace(".", "_")

        user_args = {
            "--groupby-name": self.conf_metadata_name,
            "--online-class": ZIPLINE_AWS_ONLINE_CLASS_DEFAULT,
            "--local-conf-path": self.local_abs_conf_path,
            "--original-mode": self.mode,
            "--conf-type": self.conf_type,
            "--jar-uri": jar_uri,
            "--main-class": "ai.chronon.flink.FlinkJob",
            "--job-type": "flink",
            "--job-id": job_id,
            "--streaming-checkpoint-path": self.streaming_checkpoint_path,
            "--streaming-manifest-path": self.streaming_manifest_path,
            "--flink-main-jar-uri": flink_jar_uri,
            "--eks-service-account": eks_service_account,
            "--eks-namespace": eks_namespace,
            "-ZAWS_REGION": aws_region,
            "-ZAWS_DEFAULT_REGION": aws_region,
            "-ZDYNAMO_API_CALL_ATTEMPT_TIMEOUT": os.environ.get("DYNAMO_API_CALL_ATTEMPT_TIMEOUT", "PT5S"),
            "-ZDYNAMO_API_CALL_TIMEOUT": os.environ.get("DYNAMO_API_CALL_TIMEOUT", "PT30S"),
            "-ZDYNAMO_CONNECTION_TIMEOUT": os.environ.get("DYNAMO_CONNECTION_TIMEOUT", "PT5S"),
        }

        if "check-if-job-is-running" in args:
            user_args["--streaming-mode"] = "check-if-job-is-running"
        elif "deploy" in args:
            user_args["--streaming-mode"] = "deploy"

        flag_args = {}

        if self.latest_savepoint:
            flag_args["--latest-savepoint"] = self.latest_savepoint
        elif self.custom_savepoint:
            user_args["--custom-savepoint"] = self.custom_savepoint
        else:
            flag_args["--no-savepoint"] = True

        if self.additional_jars:
            user_args["--additional-jars"] = self.additional_jars

        if self.flink_jars_uri:
            user_args["--flink-jars-uri"] = self.flink_jars_uri

        kinesis_jar_uri = os.environ.get("FLINK_KINESIS_JAR_URI")
        if not kinesis_jar_uri and os.environ.get("ENABLE_KINESIS", "false").lower() == "true":
            kinesis_jar_uri = f"{artifacts_bucket_prefix}/release/{self.version}/jars/connectors_kinesis_deploy.jar"
        if kinesis_jar_uri:
            user_args["--flink-kinesis-jar-uri"] = kinesis_jar_uri

        if eks_node_selector:
            user_args["--eks-node-selector"] = eks_node_selector

        user_args_str = " ".join(f"{key}={value}" for key, value in user_args.items() if value)
        if self.online_args:
            user_args_str += " " + self.online_args

        flag_args_str = " ".join(key for key, value in flag_args.items() if value)
        all_args = " ".join(filter(None, [user_args_str, flag_args_str]))

        return f"java -cp {self.jar_path} {EMR_ENTRY} {all_args}"

    def run(self):
        command_list = []
        if self.mode == "info":
            command_list.append(
                "python3 {script} --conf {conf} --ds {ds} --repo {repo}".format(
                    script=self.render_info, conf=self.conf, ds=self.ds, repo=self.repo
                )
            )
        elif self.sub_help or self.mode == "fetch":
            entrypoint = "ai.chronon.online.fetcher.FetcherMain"
            command_list.append(
                "java -cp {jar} {entrypoint} {subcommand} {args}".format(
                    jar=self.jar_path,
                    entrypoint=entrypoint,
                    args="--help" if self.sub_help else self._gen_final_args(),
                    subcommand=ROUTES[self.conf_type][self.mode],
                )
            )
        elif self.mode in ["streaming", "streaming-client"]:
            command = self.run_eks_flink_streaming()
            check_output(command, streaming=True)
            return
        else:
            local_files_to_upload_to_aws = []
            if self.conf:
                local_files_to_upload_to_aws.append(os.path.join(self.repo, self.conf))
            s3_conf_path = (
                extract_filename_from_path(self.conf)
                if self.conf
                else None
            )
            if self.parallelism > 1:
                assert self.start_ds is not None and self.ds is not None, (
                    "To use parallelism, please specify --start-ds and --end-ds to "
                    "break down into multiple backfill jobs"
                )
                date_ranges = split_date_range(self.start_ds, self.ds, self.parallelism)
                for start_ds, end_ds in date_ranges:
                    user_args = "{subcommand} {args} {additional_args}".format(
                        subcommand=ROUTES[self.conf_type][self.mode],
                        args=self._gen_final_args(
                            start_ds=start_ds,
                            end_ds=end_ds,
                            override_conf_path=s3_conf_path,
                        ),
                        additional_args=os.environ.get("CHRONON_CONFIG_ADDITIONAL_ARGS", ""),
                    )

                    emr_args = self.generate_emr_submitter_args(
                        local_files_to_upload=local_files_to_upload_to_aws,
                        user_args=user_args,
                    )
                    command = f"java -cp {self.jar_path} {EMR_ENTRY} {emr_args}"
                    command_list.append(command)
            else:
                user_args = ("{subcommand} {args} {additional_args}").format(
                    subcommand=ROUTES[self.conf_type][self.mode],
                    args=self._gen_final_args(
                        start_ds=self.start_ds,
                        override_conf_path=s3_conf_path,
                    ),
                    additional_args=os.environ.get("CHRONON_CONFIG_ADDITIONAL_ARGS", ""),
                )

                emr_args = self.generate_emr_submitter_args(
                    # for now, self.conf is the only local file that requires uploading
                    local_files_to_upload=local_files_to_upload_to_aws,
                    user_args=user_args,
                )
                command = f"java -cp {self.jar_path} {EMR_ENTRY} {emr_args}"
                command_list.append(command)

        if len(command_list) > 1:
            # parallel backfill mode
            with multiprocessing.Pool(processes=int(self.parallelism)) as pool:
                LOG.info(
                    "Running args list {} with pool size {}".format(command_list, self.parallelism)
                )
                pool.map(check_call, command_list)
        elif len(command_list) == 1:
            output = check_output(command_list[0]).decode("utf-8").split("\n")
            print(*output, sep="\n")

            # Parse job ID from EmrServerlessSubmitter output (format: appId|jobRunId)
            app_id = None
            job_run_id = None
            for line in output:
                if "Job submitted with ID:" in line:
                    job_id_str = line.split("Job submitted with ID:")[-1].strip()
                    parts = job_id_str.split("|")
                    if len(parts) == 2:
                        app_id, job_run_id = parts

            if not app_id or not job_run_id:
                raise RuntimeError(
                    "Failed to parse job ID from EMR Serverless submission output. "
                    "Expected 'Job submitted with ID: <appId>|<jobRunId>'."
                )

            if self.disable_cloud_logging:
                LOG.info("Cloud logging disabled; skipping status polling for job %s", job_run_id)
                return

            poll_interval = 30
            max_attempts = 120
            region = os.environ.get("AWS_REGION", "us-west-2")
            emr_serverless = boto3.client("emr-serverless", region_name=region)

            LOG.info("Waiting for EMR Serverless job %s on app %s...", job_run_id, app_id)
            for _attempt in range(max_attempts):
                resp = emr_serverless.get_job_run(
                    applicationId=app_id, jobRunId=job_run_id
                )
                state = resp["jobRun"]["state"]
                LOG.info("EMR Serverless job %s status: %s", job_run_id, state)

                if state == "SUCCESS":
                    LOG.info("EMR Serverless job %s completed successfully.", job_run_id)
                    return
                elif state in ("FAILED", "CANCELLED"):
                    state_details = resp["jobRun"].get("stateDetails", "no details")
                    raise RuntimeError(
                        f"EMR Serverless job {job_run_id} {state}: {state_details}"
                    )

                time.sleep(poll_interval)

            raise RuntimeError(
                f"Timed out waiting for EMR Serverless job {job_run_id} after"
                f" {max_attempts * poll_interval} seconds."
            )
