import base64
import json
import multiprocessing
import os
import time
import uuid
from urllib.parse import urlparse

import crcmod
from google.cloud import storage

from ai.chronon.logger import get_logger
from ai.chronon.repo.constants import ROUTES, ZIPLINE_DIRECTORY
from ai.chronon.repo.default_runner import Runner
from ai.chronon.repo.utils import (
    JobType,
    check_call,
    check_output,
    extract_filename_from_path,
    get_environ_arg,
    retry_decorator,
    split_date_range,
    upload_to_blob_store,
)

LOG = get_logger()

# GCP DATAPROC SPECIFIC CONSTANTS
DATAPROC_ENTRY = "ai.chronon.integrations.cloud_gcp.DataprocSubmitter"
ZIPLINE_GCP_JAR_DEFAULT = "cloud_gcp_lib_deploy.jar"
ZIPLINE_GCP_ONLINE_CLASS_DEFAULT = "ai.chronon.integrations.cloud_gcp.GcpApiImpl"
ZIPLINE_GCP_FLINK_JAR_DEFAULT = "flink_assembly_deploy.jar"
ZIPLINE_GCP_FLINK_PUBSUB_JAR_DEFAULT = "connectors_pubsub_deploy.jar"
ZIPLINE_GCP_SERVICE_JAR = "service_assembly_deploy.jar"


class GcpRunner(Runner):
    def __init__(self, args):
        self._remote_artifact_prefix = args.get("artifact_prefix")
        if not self._remote_artifact_prefix:
            raise ValueError("GCP artifact prefix not set.")

        self._version = args.get("version")
        gcp_jar_path = GcpRunner.download_zipline_dataproc_jar(
            self._remote_artifact_prefix,
            ZIPLINE_DIRECTORY,
            self._version,
            ZIPLINE_GCP_JAR_DEFAULT,
        )
        service_jar_path = GcpRunner.download_zipline_dataproc_jar(
            self._remote_artifact_prefix,
            ZIPLINE_DIRECTORY,
            self._version,
            ZIPLINE_GCP_SERVICE_JAR,
        )
        jar_path = f"{gcp_jar_path}:{service_jar_path}" if args["mode"] == "fetch" else gcp_jar_path

        self._args = args
        self.job_id = str(uuid.uuid4())
        self.flink_deployment_mode = args.get("flink_deployment_mode", "default")

        super().__init__(args, os.path.expanduser(jar_path))

    @staticmethod
    def get_gcp_project_id() -> str:
        return get_environ_arg("GCP_PROJECT_ID")

    @staticmethod
    def get_gcp_bigtable_instance_id() -> str:
        return get_environ_arg("GCP_BIGTABLE_INSTANCE_ID")

    @staticmethod
    def is_pubsub_enabled() -> bool:
        pubsub_enabled_env = get_environ_arg("ENABLE_PUBSUB", ignoreError=True)
        return pubsub_enabled_env and pubsub_enabled_env.lower() == "true"

    @staticmethod
    def get_gcp_region_id() -> str:
        return get_environ_arg("GCP_REGION")

    @staticmethod
    @retry_decorator(retries=2, backoff=5)
    def download_gcs_to_text(remote_file_name: str):
        """Download from the bucket using path."""
        parsed = urlparse(remote_file_name)
        bucket_name = parsed.netloc
        source_blob_name = parsed.path.lstrip("/")
        try:
            storage_client = storage.Client(project=GcpRunner.get_gcp_project_id())
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(source_blob_name)

            if not blob.exists():
                return blob.exists(), None
            else:
                return blob.exists(), blob.download_as_text()
        except Exception as e:
            raise RuntimeError(f"Failed to download {source_blob_name}: {str(e)}") from e

    @staticmethod
    @retry_decorator(retries=2, backoff=5)
    def download_gcs_file(remote_file_name: str, destination_file_name: str):
        """Download from the bucket using path."""
        parsed = urlparse(remote_file_name)
        bucket_name = parsed.netloc
        source_blob_name = parsed.path.lstrip("/")
        try:
            storage_client = storage.Client(project=GcpRunner.get_gcp_project_id())
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(source_blob_name)

            if not blob.exists():
                raise FileNotFoundError(
                    f"Blob {source_blob_name} not found in bucket {bucket_name}"
                )
            blob.download_to_filename(destination_file_name)
            LOG.info(
                "Downloaded storage object {} from bucket {} to local file {}.".format(
                    source_blob_name, bucket_name, destination_file_name
                )
            )
        except Exception as e:
            raise RuntimeError(f"Failed to download {source_blob_name}: {str(e)}") from e

    @staticmethod
    def get_gcs_file_hash(remote_file_path: str) -> str:
        """
        Get the hash of a file stored in Google Cloud Storage.
        """
        parsed = urlparse(remote_file_path)
        storage_client = storage.Client(project=GcpRunner.get_gcp_project_id())
        bucket_name = parsed.netloc
        blob_name = parsed.path.lstrip("/")
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name)

        if not blob:
            raise FileNotFoundError(f"File {blob_name} not found in bucket {bucket_name}")

        return blob.crc32c

    @staticmethod
    def get_local_file_hash(file_path: str) -> str:
        """
        Calculate CRC32C hash of a local file.

        Args:
            file_path: Path to the local file

        Returns:
            Base64-encoded string of the file's CRC32C hash
        """
        crc32c_hash = crcmod.predefined.Crc("crc-32c")

        with open(file_path, "rb") as f:
            # Read the file in chunks to handle large files efficiently
            for chunk in iter(lambda: f.read(4096), b""):
                crc32c_hash.update(chunk)

        # Convert to base64 to match GCS format
        return base64.b64encode(crc32c_hash.digest()).decode("utf-8")

    @staticmethod
    def compare_gcs_and_local_file_hashes(remote_file_path: str, local_file_path: str) -> bool:
        """
        Compare hashes of a GCS file and a local file to check if they're identical.

        Args:
            remote_file_path: URI of the remote object in GCS
            local_file_path: Path to the local file to compare

        Returns:
            True if files are identical, False otherwise
        """
        try:
            gcs_hash = GcpRunner.get_gcs_file_hash(remote_file_path)
            local_hash = GcpRunner.get_local_file_hash(local_file_path)

            LOG.info(
                f"Local hash of {local_file_path}: {local_hash}. GCS file {remote_file_path} hash: {gcs_hash}"
            )

            return gcs_hash == local_hash

        except Exception as e:
            LOG.info(f"Error comparing files: {str(e)}")
            return False

    @staticmethod
    def download_zipline_dataproc_jar(
        remote_file_path: str, local_file_path: str, version: str, jar_name: str
    ):
        source_path = os.path.join(remote_file_path, "release", version, "jars", jar_name)
        dest_path = os.path.join(local_file_path, jar_name)

        are_identical = (
            GcpRunner.compare_gcs_and_local_file_hashes(source_path, dest_path)
            if os.path.exists(dest_path)
            else False
        )

        if are_identical:
            LOG.info(f"{dest_path} matches GCS {source_path}")
        else:
            LOG.info(f"{dest_path} does NOT match GCS {source_path}")
            LOG.info(f"Downloading {jar_name} from GCS...")

            GcpRunner.download_gcs_file(source_path, dest_path)
        return dest_path

    def generate_dataproc_submitter_args(
        self,
        user_args: str,
        version: str,
        customer_artifact_prefix: str,
        job_type: JobType = JobType.SPARK,
        metadata_conf_path: str = None,
    ):
        parsed = urlparse(customer_artifact_prefix)
        source_blob_name = parsed.path.lstrip("/")

        gcs_files = []

        # upload to `metadata` folder
        if metadata_conf_path:
            destination_file_path = os.path.join(
                source_blob_name,
                "metadata",
                self.job_id,
                f"{extract_filename_from_path(metadata_conf_path)}",
            )
            gcs_files.append(
                upload_to_blob_store(
                    metadata_conf_path, f"gs://{self.warehouse_bucket}/{destination_file_path}"
                )
            )

        gcs_file_args = ",".join(gcs_files)
        release_prefix = os.path.join(customer_artifact_prefix, "release", version, "jars")

        # include jar uri. should also already be in the bucket
        jar_uri = os.path.join(release_prefix, f"{ZIPLINE_GCP_JAR_DEFAULT}")

        final_args = "{user_args} --jar-uri={jar_uri} --job-type={job_type} --main-class={main_class} --zipline-version={zipline_version} --job-id={job_id}"

        if job_type == JobType.FLINK:
            main_class = "ai.chronon.flink.FlinkJob"
            flink_jar_uri = os.path.join(release_prefix, f"{ZIPLINE_GCP_FLINK_JAR_DEFAULT}")
            enable_pubsub = GcpRunner.is_pubsub_enabled()
            flink_pubsub_connector_jar_uri = os.path.join(
                release_prefix, f"{ZIPLINE_GCP_FLINK_PUBSUB_JAR_DEFAULT}"
            )
            base_formatted_args = (
                final_args.format(
                    user_args=user_args,
                    jar_uri=jar_uri,
                    job_type=job_type.value,
                    main_class=main_class,
                    zipline_version=self._version,
                    job_id=self.job_id,
                )
                + f" --flink-main-jar-uri={flink_jar_uri}"
            )
            if enable_pubsub:
                base_formatted_args += f" --flink-pubsub-jar-uri={flink_pubsub_connector_jar_uri}"
            return base_formatted_args

        elif job_type == JobType.SPARK:
            main_class = "ai.chronon.spark.Driver"
            return " ".join(
                [
                    final_args.format(
                        user_args=user_args,
                        jar_uri=jar_uri,
                        job_type=job_type.value,
                        main_class=main_class,
                        zipline_version=self._version,
                        job_id=self.job_id,
                    ),
                    "--is-gcp",
                    f"--gcp-project-id={GcpRunner.get_gcp_project_id()}",
                    f"--gcp-bigtable-instance-id={GcpRunner.get_gcp_bigtable_instance_id()}",
                    f"--files={gcs_file_args}" if gcs_file_args else "",
                ]
            )
        else:
            raise ValueError(f"Invalid job type: {job_type}")

    @staticmethod
    def get_state_dataproc_job(job_id):
        jobs_info_str = check_output(
            f"gcloud dataproc jobs describe {job_id} --region={GcpRunner.get_gcp_region_id()} "
            f"--project={GcpRunner.get_gcp_project_id()} --format=json"
        ).decode("utf-8")
        job_info = json.loads(jobs_info_str)
        return job_info.get("status", {}).get("state", "")

    def run_dataproc_flink_streaming(self):
        user_args = {
            "--groupby-name": self.conf_metadata_name,
            "--kafka-bootstrap": self.kafka_bootstrap,
            "--online-class": ZIPLINE_GCP_ONLINE_CLASS_DEFAULT,
            "-ZGCP_PROJECT_ID": GcpRunner.get_gcp_project_id(),
            "-ZGCP_BIGTABLE_INSTANCE_ID": GcpRunner.get_gcp_bigtable_instance_id(),
            "--validate-rows": self.validate_rows,
            "--streaming-manifest-path": self.streaming_manifest_path,
            "--streaming-checkpoint-path": self.streaming_checkpoint_path,
            "--local-zipline-version": self._version,
            # Need these for extracting metadata name in submitter
            "--local-conf-path": self.local_abs_conf_path,
            "--original-mode": self.mode,
            "--conf-type": self.conf_type,
        }

        args = self._args.get("args")
        if "check-if-job-is-running" in args:
            user_args["--streaming-mode"] = "check-if-job-is-running"
        elif "deploy" in args:
            user_args["--streaming-mode"] = "deploy"

        if self.flink_deployment_mode and self.flink_deployment_mode != "default":
            user_args["--flink-deployment-mode"] = self.flink_deployment_mode

        flag_args = {"--validate": self.validate, "--enable-debug": self.enable_debug}

        # Set the savepoint deploy strategy
        if self.latest_savepoint:
            flag_args["--latest-savepoint"] = self.latest_savepoint
        elif self.custom_savepoint:
            user_args["--custom-savepoint"] = self.custom_savepoint
        else:
            flag_args["--no-savepoint"] = self.no_savepoint

        # Set version check deploy
        if self.version_check:
            flag_args["--version-check"] = self.version_check

        # Set additional jars
        if self.additional_jars:
            user_args["--additional-jars"] = self.additional_jars

        # Set flink jars base path
        if self.flink_jars_uri:
            user_args["--flink-jars-uri"] = self.flink_jars_uri

        user_args_str = " ".join(f"{key}={value}" for key, value in user_args.items() if value)
        # if online args are set we add them to the user_args_str
        if self.online_args:
            user_args_str += " " + self.online_args

        flag_args_str = " ".join(key for key, value in flag_args.items() if value)
        dataproc_args = self.generate_dataproc_submitter_args(
            job_type=JobType.FLINK,
            version=self._version,
            customer_artifact_prefix=self._remote_artifact_prefix,
            user_args=" ".join([user_args_str, flag_args_str]),
        )
        command = f"java -cp {self.jar_path} {DATAPROC_ENTRY} {dataproc_args}"

        return command

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
        elif self.mode == "metastore":
            # We could presumably support other metastore options but
            # for now only poking for a particular partition is supported.
            args = self._args.get("args")
            supported_subcommands = ["check-partitions"]
            assert "check-partitions" in args, (
                f"Must specify one of the following subcommands: {supported_subcommands}"
            )
            assert "--partition-names" in args, (
                "Must specify a list of `--partition-names=schema.table/pk1=pv1/pk2=pv2"
            )

            dataproc_args = self.generate_dataproc_submitter_args(
                # for now, self.conf is the only local file that requires uploading to gcs
                user_args=self._gen_final_args(),
                version=self._version,
                customer_artifact_prefix=self._remote_artifact_prefix,
                metadata_conf_path=str(os.path.join(self.repo, self.conf)) if self.conf else None,
            )
            command = f"java -cp {self.jar_path} {DATAPROC_ENTRY} {dataproc_args}"
            command_list.append(command)
        elif self.mode in ["streaming", "streaming-client"]:
            args = self._args.get("args")
            # streaming mode
            command = self.run_dataproc_flink_streaming()
            command_list.append(command)
        else:
            if self.parallelism > 1:
                assert self.start_ds is not None and self.ds is not None, (
                    "To use parallelism, please specify --start-ds and --end-ds to "
                    "break down into multiple backfill jobs"
                )
                date_ranges = split_date_range(self.start_ds, self.ds, self.parallelism)
                for start_ds, end_ds in date_ranges:
                    user_args = ("{subcommand} {args} {additional_args}").format(
                        subcommand=ROUTES[self.conf_type][self.mode],
                        args=self._gen_final_args(
                            start_ds=start_ds,
                            end_ds=end_ds,
                            # overriding the conf here because we only want the
                            # filename, not the full path. When we upload this to
                            # GCS, the full path does get reflected on GCS. But
                            # when we include the gcs file path as part of dataproc,
                            # the file is copied to root and not the complete path
                            # is copied.
                            override_conf_path=(
                                extract_filename_from_path(self.conf) if self.conf else None
                            ),
                        ),
                        additional_args=os.environ.get("CHRONON_CONFIG_ADDITIONAL_ARGS", ""),
                    )

                    dataproc_args = self.generate_dataproc_submitter_args(
                        # for now, self.conf is the only local file that requires uploading to gcs
                        user_args=user_args,
                        version=self._version,
                        customer_artifact_prefix=self._remote_artifact_prefix,
                        metadata_conf_path=str(os.path.join(self.repo, self.conf))
                        if self.conf
                        else None,
                    )
                    command = f"java -cp {self.jar_path} {DATAPROC_ENTRY} {dataproc_args}"
                    command_list.append(command)
            else:
                user_args = ("{subcommand} {args} {additional_args}").format(
                    subcommand=ROUTES[self.conf_type][self.mode],
                    args=self._gen_final_args(
                        start_ds=self.start_ds,
                        # overriding the conf here because we only want the filename,
                        # not the full path. When we upload this to GCS, the full path
                        # does get reflected on GCS. But when we include the gcs file
                        # path as part of dataproc, the file is copied to root and
                        # not the complete path is copied.
                        override_conf_path=(
                            extract_filename_from_path(self.conf) if self.conf else None
                        ),
                    ),
                    additional_args=os.environ.get("CHRONON_CONFIG_ADDITIONAL_ARGS", ""),
                )
                dataproc_args = self.generate_dataproc_submitter_args(
                    user_args=user_args,
                    version=self._version,
                    customer_artifact_prefix=self._remote_artifact_prefix,
                    metadata_conf_path=str(os.path.join(self.repo, self.conf))
                    if self.conf
                    else None,
                )
                command = f"java -cp {self.jar_path} {DATAPROC_ENTRY} {dataproc_args}"
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

            dataproc_submitter_id_str = "Dataproc submitter job id"

            dataproc_submitter_logs = [s for s in output if dataproc_submitter_id_str in s]

            submitted_job_id = None
            if dataproc_submitter_logs:
                log = dataproc_submitter_logs[0]
                submitted_job_id = (
                    log[log.index(dataproc_submitter_id_str) + len(dataproc_submitter_id_str) + 1 :]
                ).strip()

            # Check if this is a streaming deploy job
            is_streaming_deploy = self.mode in [
                "streaming",
                "streaming-client",
            ] and "deploy" in self._args.get("args")

            # For non-streaming jobs or streaming jobs that aren't deploy, wait for completion
            if not self.disable_cloud_logging and submitted_job_id and not is_streaming_deploy:
                LOG.info(
                    """
                <-----------------------------------------------------------------------------------
                ------------------------------------------------------------------------------------
                                                  DATAPROC LOGS
                ------------------------------------------------------------------------------------
                ------------------------------------------------------------------------------------>
                """
                )
                check_call(
                    f"gcloud dataproc jobs wait {submitted_job_id} --region={GcpRunner.get_gcp_region_id()} "
                    f"--project={GcpRunner.get_gcp_project_id()}"
                )

                # Fetch the final job state
                job_state = GcpRunner.get_state_dataproc_job(submitted_job_id)

                LOG.info(
                    "<<<<<<<<<<<<<<<<-----------------JOB STATUS----------------->>>>>>>>>>>>>>>>>"
                )
                if job_state != "DONE":
                    LOG.info(
                        f"Job {submitted_job_id} is not in DONE state. Current state: {job_state}"
                    )
                    raise RuntimeError(f"Job {submitted_job_id} failed.")
                else:
                    LOG.info(f"Job {submitted_job_id} is in DONE state.")
                return

            # If streaming deploy job, poll and check for initial success instead of waiting for completion
            if submitted_job_id and is_streaming_deploy:
                LOG.info(f"Streaming deploy job {submitted_job_id} submitted successfully.")
                LOG.info(
                    f"Monitor job status at: https://console.cloud.google.com/dataproc/jobs/{submitted_job_id}"
                    f"/monitoring?region={GcpRunner.get_gcp_region_id()}&project={GcpRunner.get_gcp_project_id()}"
                )
                # Poll the dataproc job id for 5 minutes to verify deployment
                total_time_seconds = 5 * 60
                interval_seconds = 10
                start_time = time.time()
                while time.time() - start_time < total_time_seconds:
                    current_state = GcpRunner.get_state_dataproc_job(submitted_job_id)

                    non_terminal_states = ["SETUP_DONE", "RUNNING", "PENDING", "STATE_UNSPECIFIED"]
                    if current_state not in non_terminal_states:
                        raise RuntimeError(
                            f"Flink job is not in {non_terminal_states}. "
                            f"Current state: {current_state}"
                        )

                    manifest_path = os.path.join(
                        self.streaming_manifest_path, self.conf_metadata_name, "manifest.txt"
                    )
                    manifest_exists, raw_manifest = self.download_gcs_to_text(str(manifest_path))

                    if manifest_exists:
                        manifest = raw_manifest.strip()
                        LOG.info(
                            f"Checking Flink manifest to confirm deployment. Manifest: [{manifest}]"
                        )
                        manifest_tuples = manifest.split(",")

                        flink_job_id = [
                            f.split("=")[1] for f in manifest_tuples if f.startswith("flinkJobId")
                        ][0]
                        parent_job_id = [
                            f.split("=")[1] for f in manifest_tuples if f.startswith("parentJobId")
                        ][0]

                        if parent_job_id == submitted_job_id:
                            LOG.info(
                                f"Flink job has been deployed successfully. Flink job ID = [{flink_job_id}]."
                                f" Dataproc job ID = [{submitted_job_id}]"
                            )
                            break
                        else:
                            LOG.info(
                                f"Flink manifest not updated with new Dataproc job id {submitted_job_id}."
                            )
                    LOG.info(f"Sleeping for {interval_seconds} seconds...")
                    time.sleep(interval_seconds)
                else:
                    raise RuntimeError(
                        f"Failed to confirm Flink manifest for new deployment with Dataproc job id {submitted_job_id}."
                    )
