package ai.chronon.spark

import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.MetaData
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.SparkSession

object RunnerUtils {

  def tableUtilsForMetadata(sparkSession: SparkSession, metadata: MetaData): TableUtils = {
    val defaultSpec = TableUtils(sparkSession).partitionSpec
    TableUtils(sparkSession, metadata.partitionSpec(defaultSpec))
  }
}
