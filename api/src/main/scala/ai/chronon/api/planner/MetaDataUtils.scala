package ai.chronon.api.planner
import ai.chronon.api.{ExecutionInfo, MetaData, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.JListOps

import java.util

/** Metadata layering for planner nodes: names, table dependencies, stepDays and mode-merged
  * conf/env.
  */
object MetaDataUtils {

  def layer(baseMetadata: MetaData,
            modeName: String,
            nodeName: String,
            tableDependencies: Seq[TableDependency],
            stepDays: Option[Int] = None,
            outputTableOverride: Option[String] = None)(implicit partitionSpec: PartitionSpec): MetaData = {

    val copy = baseMetadata.deepCopy()
    val effectivePartitionSpec = copy.partitionSpec(partitionSpec)
    val newName = nodeName
    copy.setName(newName)

    val baseExecutionInfo = Option(copy.executionInfo).getOrElse(new ExecutionInfo())
    val mergedExecutionInfo = mergeModeConfAndEnv(baseExecutionInfo, modeName)
    copy.setExecutionInfo(mergedExecutionInfo)

    // if stepDays is passed in respect it, otherwise use what's already there, otherwise set it to 1.
    if (stepDays.nonEmpty) {
      copy.executionInfo.setStepDays(stepDays.get)
    } else if (!copy.executionInfo.isSetStepDays) {
      copy.executionInfo.setStepDays(1)
    }

    // legacy output table and new style should match:
    // align metadata.outputTable == metadata.executionInfo.outputTableInfo.table
    if (copy.executionInfo.outputTableInfo == null) {
      copy.executionInfo.setOutputTableInfo(new TableInfo())
    }

    outputTableOverride match {
      case Some(outputTable) =>
        // Changing table identity also changes ownership of the partition metadata: rewrite all
        // partition fields so layered sensors cannot keep stale downstream grids.
        copy.executionInfo.outputTableInfo.setTable(outputTable).withSpec(partitionSpec)
      case None =>
        // if output table is not set, use the base metadata's output table
        // fully qualified: namespace + outputTable
        // effectivePartitionSpec already preserves author-declared output fields and fills
        // missing fields from the default spec.
        copy.executionInfo.outputTableInfo.setTable(copy.outputTable).withSpec(effectivePartitionSpec)
    }

    val resolvedTableDependencies = PartitionSpecResolver.resolveDependencies(tableDependencies, effectivePartitionSpec)

    // time-partitioned dependencies have no physical grid - their column is a real timestamp -
    // so apply the node's own grid to them: range math, sensing, and orchestration then
    // quantize intraday requirements on the node's own partitionInterval instead of assuming
    // daily (which would stall sub-daily readiness until the upstream day closes)
    resolvedTableDependencies.foreach { dep =>
      Option(dep.tableInfo).filter(ti => ti.isSetTimePartitioned && ti.timePartitioned).foreach { ti =>
        if (!ti.isSetPartitionInterval)
          ti.setPartitionInterval(WindowUtils.fromMillis(effectivePartitionSpec.spanMillis))
        if (!ti.isSetPartitionOffset && effectivePartitionSpec.offsetMillis != 0)
          ti.setPartitionOffset(WindowUtils.fromMillis(effectivePartitionSpec.offsetMillis))
        if (!ti.isSetPartitionFormat) ti.setPartitionFormat(effectivePartitionSpec.format)
      }
    }

    // set table dependencies
    copy.executionInfo.setTableDependencies(resolvedTableDependencies.toJava)

    copy
  }

  // merge common + mode confs and envs, discard others and return a simpler / leaner execution info
  private def mergeModeConfAndEnv(executionInfo: ExecutionInfo, mode: String): ExecutionInfo = {

    val result = executionInfo.deepCopy()

    if (executionInfo.conf != null) {
      val merged = new util.HashMap[String, String]()

      if (executionInfo.conf.common != null) merged.putAll(executionInfo.conf.common)

      if (executionInfo.conf.modeConfigs != null) {
        val modeConf = executionInfo.conf.modeConfigs.get(mode)
        if (modeConf != null) merged.putAll(modeConf)
      }

      result.conf.setCommon(merged)
      result.conf.unsetModeConfigs()
    }

    if (executionInfo.clusterConf != null) {
      val clusterMerged = new util.HashMap[String, String]()
      if (executionInfo.clusterConf.common != null) clusterMerged.putAll(executionInfo.clusterConf.common)
      if (executionInfo.clusterConf.modeClusterConfigs != null) {
        val modeConf = executionInfo.clusterConf.modeClusterConfigs.get(mode)
        if (modeConf != null) clusterMerged.putAll(modeConf)
      }

      result.clusterConf.setCommon(clusterMerged)
      result.clusterConf.unsetModeClusterConfigs()

    }

    if (executionInfo.env != null) {
      val merged = new util.HashMap[String, String]()

      if (executionInfo.env.common != null) merged.putAll(executionInfo.env.common)

      if (executionInfo.env.modeEnvironments != null) {
        val modeEnv = executionInfo.env.modeEnvironments.get(mode)
        if (modeEnv != null) merged.putAll(modeEnv)
      }

      result.env.setCommon(merged)
      result.env.unsetModeEnvironments()
    }

    result
  }

}
