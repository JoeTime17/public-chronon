package ai.chronon.integrations.cloud_azure

import ai.chronon.spark.catalog.{DefaultFormatProvider, Format, Iceberg}
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{CatalogNotFoundException, TableCatalog}

import scala.util.{Failure, Success, Try}

/** Azure format provider that checks for Iceberg tables and defaults to Snowflake.
  *
  * To use this provider, set the Spark config:
  *   spark.chronon.table.format_provider.class=ai.chronon.integrations.cloud_azure.AzureFormatProvider
  */
class AzureFormatProvider(override val sparkSession: SparkSession) extends DefaultFormatProvider(sparkSession) {

  override def readFormat(tableName: String): Option[Format] = {
    val resolved = Format.resolveTableName(tableName)(sparkSession)
    // Wrap the catalog lookup itself — an unregistered Spark V2 catalog name is a strong
    // signal the table is not Spark-managed (e.g. a Snowflake-qualified reference passing
    // through), not that format detection should fail. Without this guard, Spark's
    // CatalogManager.catalog(...) throws CatalogNotFoundException straight up the stack
    // and aborts BatchNodeRunner before the Snowflake fallback ever runs.
    Try(sparkSession.sessionState.catalogManager.catalog(resolved.catalog)) match {
      case Success(sparkCatalog: SparkCatalog) =>
        Try(sparkCatalog.loadTable(resolved.toIdentifier)) match {
          case Success(_: SparkTable) =>
            logger.info(s"AzureFormatProvider: Detected Iceberg table $tableName")
            Some(Iceberg)
          case _ =>
            Some(Snowflake)
        }
      case Success(tableCatalog: TableCatalog) =>
        Try(tableCatalog.loadTable(resolved.toIdentifier)) match {
          case Success(_: SparkTable) =>
            logger.info(s"AzureFormatProvider: Detected Iceberg table $tableName")
            Some(Iceberg)
          case _ =>
            Some(Snowflake)
        }
      case Success(_) =>
        Some(Snowflake)
      case Failure(_: CatalogNotFoundException) =>
        logger.info(
          s"AzureFormatProvider: catalog '${resolved.catalog}' not registered as a Spark V2 plugin; " +
            s"falling back to Snowflake for $tableName")
        Some(Snowflake)
      case Failure(e) =>
        // Unexpected catalog-resolution failure (not just "missing plugin"). Bubble up
        // rather than silently mask — this likely indicates a broken Spark conf rather
        // than a Snowflake-qualified reference.
        throw e
    }
  }

}
