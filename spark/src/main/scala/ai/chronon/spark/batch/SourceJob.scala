package ai.chronon.spark.batch
import ai.chronon.api.{Constants, DateRange, MetaData}
import ai.chronon.api.DataModel.EVENTS
import ai.chronon.api.Extensions.{MetadataOps, _}
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.planner.SourceWithFilterNode
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.Map
import scala.jdk.CollectionConverters._

/*
Runs and materializes a `Source` for a given `dateRange`. Used in the Join computation flow to first compute the Source,
then each join may have a further Bootstrap computation to produce the left side for use in the final join step.
 */
class SourceJob(node: SourceWithFilterNode, metaData: MetaData, range: DateRange)(implicit tableUtils: TableUtils) {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private val sourceWithFilter = node
  private val dateRange = range.toPartitionRange(tableUtils.partitionSpec)
  private val outputTable = metaData.outputTable

  def parseSkewKeys(jmap: java.util.Map[String, java.util.List[String]]): Option[Map[String, Seq[String]]] = {
    Option(jmap).map(_.toScala.map { case (key, list) => key -> list.asScala.toSeq }.toMap)
  }

  def run(): Unit = tableUtils.withJobDescription(s"SourceJob($outputTable) $dateRange") {

    val source = sourceWithFilter.source

    val timeProjection = if (source.dataModel == EVENTS) {
      Seq(Constants.TimeColumn -> Option(source.query).map(_.timeColumn).orNull)
    } else {
      Seq()
    }

    val skewKeys = parseSkewKeys(sourceWithFilter.excludeKeys)
    val skewFilter = formatFilterString(skewKeys)

    val skewFilteredSource = skewFilter
      .map(sf => {
        val copySource = source.deepCopy()
        val allFilters = source.query.wheres.asScala ++ Seq(sf)
        copySource.query.setWheres(allFilters.toSeq.toJava)
        copySource
      })
      .getOrElse(source)

    // This job benefits from a step day of 1 to avoid needing to shuffle on writing output (single partition)
    dateRange.stepsByDays(1).foreach { dayStep =>
      val df = tableUtils.scanDf(skewFilteredSource.query,
                                 skewFilteredSource.table,
                                 Some((Map(tableUtils.partitionColumn -> null) ++ timeProjection).toMap),
                                 range = Some(dayStep))

      if (df.isEmpty) {
        logger.warn(s"Query produced 0 rows in range $dayStep. Skipping this partition.")
      } else {
        val dfWithTimeCol = if (source.dataModel == EVENTS) {
          df.withTimeBasedColumn(Constants.TimePartitionColumn, spec = tableUtils.partitionSpec)
        } else {
          df
        }

        // Save using the provided outputTable or compute one if not provided
        dfWithTimeCol.save(outputTable, tableProperties = metaData.tableProps)
      }
    }
  }

  private def formatFilterString(keys: Option[Map[String, Seq[String]]] = None): Option[String] = {
    keys.map { keyMap =>
      keyMap
        .map { case (keyName, values) =>
          generateSkewFilterSql(keyName, values)
        }
        .filter(_.nonEmpty)
        .mkString(" OR ")
    }
  }

  def generateSkewFilterSql(key: String, values: Seq[String]): String = {
    val nulls = Seq("null", "Null", "NULL")
    val nonNullFilters = Some(s"$key NOT IN (${values.filterNot(nulls.contains).mkString(", ")})")
    val nullFilters = if (values.exists(nulls.contains)) Some(s"$key IS NOT NULL") else None
    (nonNullFilters ++ nullFilters).mkString(" AND ")
  }
}
