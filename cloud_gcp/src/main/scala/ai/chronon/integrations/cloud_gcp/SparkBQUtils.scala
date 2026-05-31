package ai.chronon.integrations.cloud_gcp
import ai.chronon.spark.catalog.Format
import com.google.cloud.bigquery.connector.common.BigQueryUtil
import org.apache.spark.sql.SparkSession
import com.google.cloud.bigquery.TableId
import org.apache.spark.sql.connector.catalog.Identifier

object SparkBQUtils {

  def toTableId(tableName: String)(implicit spark: SparkSession): TableId = {
    val parts = Format.parseIdentifier(tableName)
    val shadedTid = BigQueryUtil.parseTableId(parts.mkString("."))
    scala
      .Option(shadedTid.getProject)
      .map(TableId.of(_, shadedTid.getDataset, shadedTid.getTable))
      .getOrElse(TableId.of(shadedTid.getDataset, shadedTid.getTable))
  }

  def toIdentifier(tableName: String)(implicit spark: SparkSession): Identifier = {
    val parts = Format.parseIdentifier(tableName)
    Identifier.of(parts.init.toArray, parts.last)
  }

  def toIdentifierNoCatalog(tableName: String)(implicit spark: SparkSession): Identifier = {
    val identifier = toIdentifier(tableName)
    val namespace = identifier.namespace()
    if (namespace.isEmpty) {
      Identifier.of(Array.empty[String], identifier.name())
    } else {
      Identifier.of(Array(namespace.last), identifier.name())
    }
  }

}
