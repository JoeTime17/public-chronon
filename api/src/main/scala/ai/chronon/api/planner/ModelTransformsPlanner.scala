package ai.chronon.api.planner

import ai.chronon.api.{ModelTransforms, PartitionSpec, Source, TableDependency, TableInfo}
import ai.chronon.api.Extensions.{MetadataOps, ModelTransformsOps, SourceOps, WindowUtils}
import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.api.planner.TableDependencies.fromSource
import ai.chronon.planner.{ConfPlan, ModelTransformsBackfillNode, ModelTransformsUploadNode, Node}
import ai.chronon.planner

import scala.collection.JavaConverters._

class ModelTransformsPlanner(modelTransforms: ModelTransforms)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[ModelTransforms](modelTransforms)(outputPartitionSpec) {

  private val confOutputPartitionSpec: PartitionSpec =
    modelTransforms.partitionSpec(outputPartitionSpec)

  private def sources: Seq[Source] =
    Option(modelTransforms.sources)
      .map(_.toScala.toSeq)
      .getOrElse(Seq.empty)

  private def validatePartitionIntervals(): Unit = {
    for {
      source <- sources
      query <- Option(source.query)
    } {
      PartitionSpecResolver.validateQueryGrid(
        modelTransforms.metaData.name,
        confOutputPartitionSpec,
        query,
        s"source ${source.rawTable}",
        source.dataModel,
        PartitionSpecResolver.sourceSpec(source, confOutputPartitionSpec)
      )
    }
    PartitionSpecResolver.validateEmbeddedJoinSourceTrees(sources, confOutputPartitionSpec)
  }

  private def eraseExecutionInfo: ModelTransforms = {
    val result = modelTransforms.deepCopy()
    result.metaData.unsetExecutionInfo()
    result
  }

  private def semanticModelTransforms(modelTransforms: ModelTransforms): ModelTransforms = {
    val semantic = modelTransforms.deepCopy()
    semantic.unsetMetaData()
    semantic
  }

  def backfillNode: Node = {
    val sourceDeps = sources.flatMap(source => fromSource(source))

    // add model dependencies - we depend on the deployed model endpoint for models that are custom and trained by us
    val modelDeps = Option(modelTransforms.models)
      .map(_.toScala.toSeq)
      .getOrElse(Seq.empty)
      .flatMap { model =>
        if (model.isSetTrainingConf) {
          val deployNodeName = model.metaData.outputTable + "__model_deploy"
          val deployNodeTableDep = new TableDependency()
            .setTableInfo(
              new TableInfo()
                .setTable(deployNodeName)
            )
            .setStartOffset(WindowUtils.zero())
            .setEndOffset(WindowUtils.zero())
            .setIsSoftNodeDependency(true)
          Some(deployNodeTableDep)
        } else {
          None
        }
      }

    val tableDeps = sourceDeps ++ modelDeps

    val metaData =
      MetaDataUtils.layer(
        modelTransforms.metaData,
        "model_transforms_backfill",
        modelTransforms.metaData.name + "__model_transforms_backfill",
        tableDeps,
        outputTableOverride = Some(modelTransforms.metaData.outputTable)
      )(confOutputPartitionSpec)

    val node = new ModelTransformsBackfillNode().setModelTransforms(modelTransforms)

    val copy = semanticModelTransforms(modelTransforms)

    toNode(metaData, _.setModelTransformsBackfill(node), copy)
  }

  def uploadNode: Node = {
    val stepDays = 1 // Default step days for metadata upload

    // Create table dependencies only for JoinSource sources - we ensure join metadata is uploaded before proceeding
    val allDeps = TableDependencies.fromJoinSources(modelTransforms.sources)

    val metaData =
      MetaDataUtils.layer(
        modelTransforms.metaData,
        "model_transforms_upload",
        modelTransforms.metaData.name + "__model_transforms_upload",
        allDeps,
        Some(stepDays)
      )(confOutputPartitionSpec)

    val node = new ModelTransformsUploadNode().setModelTransforms(eraseExecutionInfo)

    val copy = semanticModelTransforms(modelTransforms)

    toNode(metaData, _.setModelTransformsUpload(node), copy)
  }

  override def buildPlan: ConfPlan = {
    validatePartitionIntervals()
    val upload = uploadNode
    val backfill = backfillNode

    val terminalNodeNames = Map(
      planner.Mode.BACKFILL -> backfill.metaData.name,
      planner.Mode.DEPLOY -> upload.metaData.name
    )

    new ConfPlan()
      .setNodes(Seq(upload, backfill).asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
  }
}

object ModelTransformsPlanner {
  def apply(modelTransforms: ModelTransforms)(implicit outputPartitionSpec: PartitionSpec): ModelTransformsPlanner =
    new ModelTransformsPlanner(modelTransforms)(outputPartitionSpec)
}
