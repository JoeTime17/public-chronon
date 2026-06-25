from collections.abc import Sequence
from dataclasses import dataclass
from typing import Dict, List, Optional, Union

import gen_thrift.api.ttypes as ttypes
import gen_thrift.common.ttypes as common
from ai.chronon import utils
from ai.chronon import windows as window_utils
from ai.chronon.cli.compile.config_origin import mark_factory_created_config
from ai.chronon.data_types import DataType, FieldsType
from ai.chronon.utils import ANY_SOURCE_TYPE, normalize_source, normalize_sources


class ModelBackend:
    VERTEXAI = ttypes.ModelBackend.VertexAI
    SAGEMAKER = ttypes.ModelBackend.SageMaker


class DeploymentStrategyType:
    # deploys the model in a blue-green fashion (~2x capacity) to another endpoint and gradually ramps traffic
    BLUE_GREEN = ttypes.DeploymentStrategyType.BLUE_GREEN

    # deploys the model in a rolling manner by gradually scaling down existing instances and scaling up new instances
    ROLLING = ttypes.DeploymentStrategyType.ROLLING

    # deploys the model immediately to the endpoint without any traffic ramping
    IMMEDIATE = ttypes.DeploymentStrategyType.IMMEDIATE


@dataclass
class ResourceConfig:
    min_replica_count: Optional[int] = None
    max_replica_count: Optional[int] = None
    machine_type: Optional[str] = None

    def to_thrift(self):
        return ttypes.ResourceConfig(
            minReplicaCount=self.min_replica_count,
            maxReplicaCount=self.max_replica_count,
            machineType=self.machine_type,
        )


@dataclass
class InferenceSpec:
    model_backend: Optional[ModelBackend] = None
    model_backend_params: Optional[Dict[str, str]] = None
    resource_config: Optional[ResourceConfig] = None

    def to_thrift(self):
        return ttypes.InferenceSpec(
            modelBackend=self.model_backend,
            modelBackendParams=self.model_backend_params,
            resourceConfig=self.resource_config.to_thrift() if self.resource_config else None,
        )


@dataclass
class TrainingSpec:
    # TODO: may want to try to support staging query as a training_data_source
    training_data_source: Optional[ANY_SOURCE_TYPE] = None
    training_data_window: Optional[Union[common.Window, str]] = None
    schedule: Optional[str] = None
    image: Optional[str] = None
    python_module: Optional[str] = None
    resource_config: Optional[ResourceConfig] = None
    job_configs: Optional[Dict[str, str]] = None

    def to_thrift(self):
        return ttypes.TrainingSpec(
            trainingDataSource=normalize_source(self.training_data_source)
            if self.training_data_source
            else None,
            trainingDataWindow=window_utils.normalize_window(self.training_data_window)
            if self.training_data_window
            else None,
            schedule=self.schedule,
            image=self.image,
            pythonModule=self.python_module,
            resourceConfig=self.resource_config.to_thrift() if self.resource_config else None,
            jobConfigs=self.job_configs,
        )


@dataclass
class ServingContainerConfig:
    image: Optional[str] = None
    serving_health_route: Optional[str] = None
    serving_predict_route: Optional[str] = None
    serving_container_env_vars: Optional[Dict[str, str]] = None

    def to_thrift(self):
        return ttypes.ServingContainerConfig(
            image=self.image,
            servingHealthRoute=self.serving_health_route,
            servingPredictRoute=self.serving_predict_route,
            servingContainerEnvVars=self.serving_container_env_vars,
        )


@dataclass
class EndpointConfig:
    endpoint_name: Optional[str] = None
    additional_configs: Optional[Dict[str, str]] = None

    def to_thrift(self):
        return ttypes.EndpointConfig(
            endpointName=self.endpoint_name,
            additionalConfigs=self.additional_configs,
        )


@dataclass
class Metric:
    name: Optional[str] = None
    threshold: Optional[float] = None

    def to_thrift(self):
        return ttypes.Metric(
            name=self.name,
            threshold=self.threshold,
        )


@dataclass
class RolloutStrategy:
    rollout_type: Optional[DeploymentStrategyType] = None
    validation_traffic_percent_ramps: Optional[List[int]] = None
    validation_traffic_duration_mins: Optional[List[int]] = None
    rollout_metric_thresholds: Optional[List[Metric]] = None

    def to_thrift(self):
        return ttypes.RolloutStrategy(
            rolloutType=self.rollout_type,
            validationTrafficPercentRamps=self.validation_traffic_percent_ramps,
            validationTrafficDurationMins=self.validation_traffic_duration_mins,
            rolloutMetricThresholds=[m.to_thrift() for m in self.rollout_metric_thresholds]
            if self.rollout_metric_thresholds
            else None,
        )


@dataclass
class DeploymentSpec:
    container_config: Optional[ServingContainerConfig] = None
    endpoint_config: Optional[EndpointConfig] = None
    resource_config: Optional[ResourceConfig] = None
    rollout_strategy: Optional[RolloutStrategy] = None

    def to_thrift(self):
        return ttypes.DeploymentSpec(
            containerConfig=self.container_config.to_thrift() if self.container_config else None,
            endpointConfig=self.endpoint_config.to_thrift() if self.endpoint_config else None,
            resourceConfig=self.resource_config.to_thrift() if self.resource_config else None,
            rolloutStrategy=self.rollout_strategy.to_thrift() if self.rollout_strategy else None,
        )


def Model(
    version: str,
    inference_spec: Optional[InferenceSpec] = None,
    input_mapping: Optional[Dict[str, str]] = None,
    output_mapping: Optional[Dict[str, str]] = None,
    value_fields: Optional[FieldsType] = None,
    model_artifact_base_uri: Optional[str] = None,
    training_conf: Optional[TrainingSpec] = None,
    deployment_conf: Optional[DeploymentSpec] = None,
    output_namespace: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
    tags: Optional[Dict[str, str]] = None,
    environments: Optional[List[str]] = None,
    partition_interval: Optional[Union[common.Window, str]] = None,
    partition_offset: Optional[Union[common.Window, str]] = None,
) -> ttypes.Model:
    """
    Creates a Model object for ML model inference and orchestration.

    :param version:
        Version string for the model configuration
    :type version: str
    :param inference_spec:
        Model + model backend specific details necessary to perform inference
    :type inference_spec: InferenceSpec
    :param input_mapping:
        Spark SQL queries to transform input data to the format expected by the model
    :type input_mapping: Dict[str, str]
    :param output_mapping:
        Spark SQL queries to transform model output to desired output format
    :type output_mapping: Dict[str, str]
    :param value_fields:
        List of tuples of (field_name, DataType) defining the schema of the model's output values.
        If provided, creates a STRUCT schema that will be set as the model's valueSchema.
        Example: [('score', DataType.DOUBLE), ('category', DataType.STRING)]
    :type value_fields: FieldsType
    :param model_artifact_base_uri:
        Base URI where trained model artifacts are stored
    :type model_artifact_base_uri: str
    :param training_conf:
        Configs related to orchestrating model training jobs
    :type training_conf: TrainingSpec
    :param deployment_conf:
        Configs related to orchestrating model deployment
    :type deployment_conf: DeploymentSpec
    :param output_namespace:
        Namespace for the model output
    :type output_namespace: str
    :param table_properties:
        Additional table properties for the model output
    :type table_properties: Dict[str, str]
    :param tags:
        Additional metadata that does not directly affect computation, but is useful for management.
    :type tags: Dict[str, str]
    :param environments:
        List of environments where this Model should be deployed/available.
        Defaults to ['prod']. Valid values: 'prod', 'canary' (case-insensitive).
    :type environments: List[str]
    :param partition_interval:
        Output partition interval for model training/deploy nodes. Examples: "1d", "3h", "15m".
        When set below daily, Chronon uses "yyyy-MM-dd-HH-mm" ds values.
    :type partition_interval: Optional[Union[common.Window, str]]
    :param partition_offset:
        Offset from UTC midnight/epoch for the output partition grid.
    :type partition_offset: Optional[Union[common.Window, str]]
    :return:
        A Model object
    """
    # `environments` is left unset (None) when the author doesn't specify it.
    # Downstream consumers (e.g. hub schedule-all) default the missing/empty
    # case to ['prod']. Keeping it unset on disk avoids baking a default into
    # every compiled conf.
    if environments:
        environments = utils.convert_environments_to_enum(environments)

    # Get caller's filename to assign team
    team = utils._get_team_from_caller()

    assert isinstance(version, str), f"Version must be a string, but found {type(version).__name__}"

    # output_table_info handles the no-grid case itself (returns None, and rejects a lone
    # partition_offset loudly instead of silently dropping it)
    output_info = window_utils.output_table_info(partition_interval, partition_offset=partition_offset)

    # Create metadata
    meta_data = ttypes.MetaData(
        outputNamespace=output_namespace,
        team=team,
        tags=tags,
        tableProperties=table_properties,
        version=version,
        environments=environments,
        executionInfo=common.ExecutionInfo(outputTableInfo=output_info)
        if output_info is not None
        else None,
    )

    model = ttypes.Model(
        metaData=meta_data,
        inferenceSpec=inference_spec.to_thrift() if inference_spec else None,
        inputMapping=input_mapping,
        outputMapping=output_mapping,
        valueSchema=DataType.STRUCT("model_value_schema", *value_fields) if value_fields else None,
        modelArtifactBaseUri=model_artifact_base_uri,
        trainingConf=training_conf.to_thrift() if training_conf else None,
        deploymentConf=deployment_conf.to_thrift() if deployment_conf else None,
    )

    if (
        output_info is not None
        and window_utils.window_millis(output_info.partitionInterval) < window_utils.DAY_MILLIS
        and model.trainingConf is not None
    ):
        source = model.trainingConf.trainingDataSource
        if source is not None:
            inner = source.events or source.entities or source.joinSource
            source_table = getattr(inner, "table", None) or getattr(inner, "snapshotTable", None)
            window_utils.validate_source_grid(
                "This Model", window_utils.source_query(source), f"training source {source_table}"
            )

    return mark_factory_created_config(model)


def _get_model_transforms_output_table_name(
    model_transforms: ttypes.ModelTransforms, full_name: bool = False
):
    """Generate output table name for ModelTransforms"""
    return utils._ensure_name_and_get_output_table_reference(
        model_transforms, ttypes.ModelTransforms, "models", full_name
    )


def ModelTransforms(
    sources: Sequence[ANY_SOURCE_TYPE],
    models: List[ttypes.Model],
    version: int,
    passthrough_fields: Optional[List[str]] = None,
    key_fields: Optional[FieldsType] = None,
    output_namespace: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
    tags: Optional[Dict[str, str]] = None,
    environments: Optional[List[str]] = None,
) -> ttypes.ModelTransforms:
    """
    ModelTransforms allows taking the output of existing sources (Event/Entity/Join) and
    enriching them with 1 or more model outputs. This can be used in GroupBys, Joins, or hit directly
    via the fetcher. The GroupBy path allows for async materialization of model outputs to the online KV store for low latency
    serving. The fetcher path allows for on-demand model inference during online serving (at the cost of higher latency / more
    model inference calls).

    Attributes:
     - sources: List of existing sources (Event/Entity/Join sources) to be enriched with model outputs
     - models: List of Model objects that will be used for inference on the source data
     - passthrough_fields: Fields from the source that we want to passthrough alongside the model outputs
    - key_fields: List of tuples of (field_name, DataType) defining the schema of the key fields.
        If provided, creates a STRUCT schema that will be set as the ModelTransforms' keySchema.
        Example: [('user_id', DataType.STRING), ('session_id', DataType.STRING)]
     - output_namespace: Namespace for the model output
     - table_properties: Additional table properties for the model output
     - tags: Additional metadata tags
     - environments: List of environments where this ModelTransforms should be deployed/available.
        Defaults to ['prod']. Valid values: 'prod', 'canary' (case-insensitive).
    """
    # `environments` is left unset (None) when the author doesn't specify it.
    # Downstream consumers (e.g. hub schedule-all) default the missing/empty
    # case to ['prod']. Keeping it unset on disk avoids baking a default into
    # every compiled conf.
    if environments:
        environments = utils.convert_environments_to_enum(environments)

    # Get caller's filename to assign team
    team = utils._get_team_from_caller()

    # Set names for Model objects if they don't have names yet
    if models:
        for model in models:
            if not model.metaData.name:
                utils.__set_name(model, ttypes.Model, "models")

    # Normalize all sources to ensure they are properly wrapped
    normalized_sources = normalize_sources(sources)

    # Create metadata
    meta_data = ttypes.MetaData(
        outputNamespace=output_namespace,
        team=team,
        tags=tags,
        tableProperties=table_properties,
        version=str(version),
        environments=environments,
    )

    model_transforms = ttypes.ModelTransforms(
        sources=normalized_sources,
        models=models,
        passthroughFields=passthrough_fields,
        metaData=meta_data,
        keySchema=DataType.STRUCT("modeltransform_key_schema", *key_fields) if key_fields else None,
    )

    # Add the table property for output table name generation
    model_transforms.__class__.table = property(
        lambda self: _get_model_transforms_output_table_name(self, full_name=True)
    )

    return mark_factory_created_config(model_transforms)
