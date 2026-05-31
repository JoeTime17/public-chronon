"""
importing ai.chronon.types will bring in all the api's needed to create any chronon object
"""

import ai.chronon.group_by as group_by
import ai.chronon.join as join
import ai.chronon.model as model
import ai.chronon.query as query
import ai.chronon.scd2 as scd2  # noqa: F401
import ai.chronon.source as source
import ai.chronon.staging_query as staging_query
import gen_thrift.api.ttypes as ttypes
import gen_thrift.common.ttypes as common
from ai.chronon.derivation import Derivation  # noqa: F401

# source related concepts
Query = query.Query
selects = query.selects

Source = ttypes.Source
EventSource = source.EventSource
EntitySource = source.EntitySource
JoinSource = source.JoinSource

# Aggregation / GroupBy related concepts
GroupBy = group_by.GroupBy
Aggregation = group_by.Aggregation
Aggregations = group_by.Aggregations
Operation = group_by.Operation
Window = group_by.Window
TimeUnit = group_by.TimeUnit
DefaultAggregation = group_by.DefaultAggregation

Accuracy = group_by.Accuracy
TEMPORAL = ttypes.Accuracy.TEMPORAL
SNAPSHOT = ttypes.Accuracy.SNAPSHOT


# join related concepts
Join = join.Join
JoinPart = join.JoinPart
BootstrapPart = join.BootstrapPart
ContextualSource = join.ContextualSource
ExternalPart = join.ExternalPart
ExternalSource = join.ExternalSource
DataType = join.DataType

# model related concepts
Model = model.Model
ModelTransforms = model.ModelTransforms
ModelBackend = model.ModelBackend
DeploymentStrategyType = model.DeploymentStrategyType
InferenceSpec = model.InferenceSpec
TrainingSpec = model.TrainingSpec
DeploymentSpec = model.DeploymentSpec
ResourceConfig = model.ResourceConfig
RolloutStrategy = model.RolloutStrategy
ServingContainerConfig = model.ServingContainerConfig
EndpointConfig = model.EndpointConfig
Metric = model.Metric

# Staging Query related concepts
StagingQuery = staging_query.StagingQuery
EngineType = staging_query.EngineType
TableDependency = staging_query.TableDependency
MetaData = ttypes.MetaData

# execution / config types
EnvironmentVariables = common.EnvironmentVariables
ConfigProperties = common.ConfigProperties
ClusterConfigProperties = common.ClusterConfigProperties
ExecutionInfo = common.ExecutionInfo

Team = ttypes.Team
