/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.api

import ai.chronon.api
import ai.chronon.api.Constants._
import ai.chronon.api.DataModel._
import ai.chronon.api.Operation._
import ai.chronon.api.ScalaJavaConversions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.PrintWriter
import java.io.StringWriter
import java.util
import java.util.regex.Pattern
import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.matching.Regex

object Extensions {

  private def _keyNameForKvStore(metaData: MetaData, keywordType: String): String = {
    s"$keywordType/" + metaData.name
  }

  implicit class TimeUnitOps(timeUnit: TimeUnit) {
    def str: String =
      timeUnit match {
        case TimeUnit.HOURS   => "h"
        case TimeUnit.DAYS    => "d"
        case TimeUnit.MINUTES => "m"
      }

    def millis: Long =
      timeUnit match {
        case TimeUnit.HOURS   => 3600 * 1000
        case TimeUnit.DAYS    => 24 * 3600 * 1000
        case TimeUnit.MINUTES => 60 * 1000
      }
  }

  implicit class OperationOps(operation: Operation) {
    def isSimple: Boolean =
      operation match {
        case Operation.FIRST | Operation.LAST | Operation.LAST_K | Operation.FIRST_K => false
        case _                                                                       => true
      }

    def stringified: String = operation.toString.toLowerCase

  }

  implicit class WindowOps(window: Window) {
    // distinct from the display notion of `unbounded` below: a zero-length window is a valid
    // no-op OFFSET, but Int.MaxValue means "no bound" and must never enter partition arithmetic
    def isUnboundedSentinel: Boolean = window.length == Int.MaxValue

    private def unbounded: Boolean = window.length == Int.MaxValue || window.length <= 0

    def str: String =
      if (unbounded) "unbounded" else s"${window.length}${window.timeUnit.str}"

    def suffix: String =
      if (unbounded) "" else s"_${window.length}${window.timeUnit.str}"

    def millis: Long = window.length.toLong * window.timeUnit.millis

    def inverse: Window = {
      if (window == null) return null
      window.deepCopy().setLength(0 - window.getLength)
    }

  }

  object WindowUtils {
    val Unbounded: Window = new Window(Int.MaxValue, TimeUnit.DAYS)

    val Hour: Window = new Window(1, TimeUnit.HOURS)
    val Day: Window = new Window(1, TimeUnit.DAYS)
    val Null: Window = null

    private val SecondMillis: Long = 1000
    val MinuteMillis: Long = 60 * SecondMillis
    val FiveMinutes: Long = 5 * MinuteMillis
    private val defaultPartitionSize: api.TimeUnit = api.TimeUnit.DAYS
    val onePartition: api.Window = new api.Window(1, defaultPartitionSize)

    def fromMillis(millis: Long): Window = {
      if (millis % Day.millis == 0) {
        new Window((millis / Day.millis).toInt, TimeUnit.DAYS)
      } else if (millis % Hour.millis == 0) {
        new Window((millis / Hour.millis).toInt, TimeUnit.HOURS)
      } else if (millis % MinuteMillis == 0) {
        new Window((millis / MinuteMillis).toInt, TimeUnit.MINUTES)
      } else {
        throw new IllegalArgumentException(s"Window duration must be whole minutes, found ${millis}ms")
      }
    }

    def millisToString(millis: Long): String = {
      if (millis % Day.millis == 0) {
        new Window((millis / Day.millis).toInt, TimeUnit.DAYS).str
      } else if (millis % Hour.millis == 0) {
        new Window((millis / Hour.millis).toInt, TimeUnit.HOURS).str
      } else if (millis % MinuteMillis == 0) {
        s"${millis / MinuteMillis} minutes"
      } else if (millis % SecondMillis == 0) {
        s"${millis / SecondMillis} seconds"
      } else {
        s"${millis}ms"
      }
    }

    // Returns the start of the window that contains the timestamp
    // As an example consider a 1hr window: 3600 * 1000. If the timestamp is 1735733820000 (2025-01-01 12:17:00)
    // the start of the window is 1735732800000 (2025-01-01 12:00:00)
    def windowStartMillis(timestampMs: Long, windowSizeMs: Long): Long = {
      timestampMs - (timestampMs % windowSizeMs)
    }

    def convertUnits(window: Window, outputUnit: api.TimeUnit): Window = {
      if (window == null) return null
      if (window.timeUnit == outputUnit) return window

      val offsetSpanMillis = new Window(1, outputUnit).millis
      val windowLength = math.ceil(window.millis.toDouble / offsetSpanMillis.toDouble).toInt
      new Window(windowLength, outputUnit)
    }

    def plus(a: Window, b: Window): Window = {
      if (a == null) return b
      if (b == null) return a

      fromMillis(a.millis + b.millis)
    }

    def minus(a: Window, b: Window): Window = {
      if (a == null) return null
      if (b == null) return a

      fromMillis(a.millis - b.millis)
    }

    def zero(timeUnits: api.TimeUnit = api.TimeUnit.DAYS): Window = new Window(0, timeUnits)
  }

  implicit class MetadataOps(metaData: MetaData) {
    def cleanName: String = metaData.name.sanitize

    /** The output partition grid is a data-layout property (changing partitionInterval or
      * partitionOffset renames every ds in the output table), so unlike the rest of executionInfo
      * it participates in semantic hashing — via a conditional token so that daily/unset grids
      * contribute nothing and existing hashes stay byte-stable. The schedule, and hence the
      * derived processing delay, never participates. Canonicalized to millis so
      * Window(1, DAYS) == Window(24, HOURS) == unset.
      */
    def outputGridToken: Option[String] =
      for {
        executionInfo <- Option(metaData.executionInfo)
        tableInfo <- Option(executionInfo.outputTableInfo)
        interval <- Option(tableInfo.partitionInterval)
        offsetMs = Option(tableInfo.partitionOffset).map(_.millis).getOrElse(0L)
        token <- PartitionGrid(interval.millis, offsetMs).semanticToken
      } yield token

    def mixGridToken(baseHash: String): String =
      outputGridToken.map(token => HashUtils.md5Base64(s"$baseHash|$token")).getOrElse(baseHash)

    def partitionSpec(defaultSpec: PartitionSpec): PartitionSpec =
      (for {
        md <- Option(metaData)
        executionInfo <- Option(md.executionInfo)
        outputTableInfo <- Option(executionInfo.outputTableInfo)
      } yield outputTableInfo.partitionSpec(defaultSpec)).getOrElse(defaultSpec)

    // TODO: we no longer use __v{version} - it is just __{version}, deprecate this method
    def cleanNameWithoutVersion: String = {
      val clean = metaData.name.sanitize
      clean.replaceAll("__v\\d+$", "")
    }

    def outputTable: String = {
      (for {
        metaData <- Option(metaData)
        executionInfo <- Option(metaData.executionInfo)
        outputTableInfo <- Option(executionInfo.outputTableInfo)
        tableInfo <- Option(outputTableInfo)
        table <- Option(tableInfo.table)
      } yield table).getOrElse(s"${metaData.outputNamespace}.${metaData.cleanName}")
    }

    def loggedTable: String = s"${outputTable}_logged"
    def summaryTable: String = s"${outputTable}_summary"
    def packedSummaryTable: String = s"${outputTable}_summary_packed"

    def bootstrapTable: String = s"${outputTable}_bootstrap"

    def stepSize: Int = {
      Option(metaData.executionInfo)
        .filter(_.isSetStepDays)
        .map(_.stepDays)
        .getOrElse(14)
    }

    private def comparisonPrefix = "comparison"

    def comparisonConfName: String = s"${metaData.getName}_$comparisonPrefix"

    def comparisonTable: String = s"${outputTable}_$comparisonPrefix"

    def consistencyTable: String = s"${outputTable}_consistency"
    def consistencyUploadTable: String = s"${consistencyTable}_upload"

    def uploadTable: String = s"${outputTable}__upload"

    def copyForVersioningComparison: MetaData = {
      // Changing name results in column rename, therefore schema change, other metadata changes don't effect output table
      val newMetaData = new MetaData()
      newMetaData.setName(metaData.name)
      newMetaData
    }

    def tableProps: Map[String, String] =
      Option(metaData.tableProperties)
        .map(_.toScala.toMap)
        .orNull

    def commonConf: Map[String, String] = {
      (for {
        execInfo <- Option(metaData.executionInfo)
        conf <- Option(execInfo.conf)
        common <- Option(conf.common)
      } yield common.toScala.toMap).getOrElse(Map.empty)
    }

    @deprecated("Use `name` instead.")
    def nameToFilePath: String = metaData.name.replaceFirst("\\.", "/")

    // if drift spec is set but tile size is not set, default to 30 minutes
    def driftTileSize: Option[Window] = {
      Option(metaData.getDriftSpec) match {
        case Some(driftSpec) =>
          Option(driftSpec.getTileSize).orElse(Some(Constants.DefaultDriftTileSize))
        case None => None
      }
    }

    private val trailingVersionPattern: Regex = """^(.*)__(\d+)$""".r

    private def splitTrailingVersion(s: String): (String, Option[Int]) = {
      s match {
        case trailingVersionPattern(prefix, num) => (prefix, Some(num.toInt))
        case _                                   => (s, None)
      }
    }

    def nameWithoutVersion: String = splitTrailingVersion(metaData.getName)._1

    def nameWithoutTeamOrVersion: String = {
      metaData.nameWithoutVersion.split('.').tail.mkString(".")
    }

  }

  // one per output column - so single window
  // not exposed to users
  implicit class AggregationPartOps(aggregationPart: AggregationPart) {

    def getInt(arg: String, default: Option[Int] = None): Int = {
      val argOpt = Option(aggregationPart.argMap)
        .flatMap(_.toScala.get(arg))
      require(
        argOpt.isDefined || default.isDefined,
        s"$arg needs to be specified in the `argMap` for ${aggregationPart.operation} type"
      )
      argOpt.map(_.toInt).getOrElse(default.get)
    }

    private def opSuffix =
      aggregationPart.operation match {
        case LAST_K       => s"last${getInt("k")}"
        case FIRST_K      => s"first${getInt("k")}"
        case TOP_K        => s"top${getInt("k")}"
        case BOTTOM_K     => s"bottom${getInt("k")}"
        case UNIQUE_TOP_K => s"unique_top${getInt("k")}"
        case other        => other.stringified
      }

    private def bucketSuffix = Option(aggregationPart.bucket).map("_by_" + _).getOrElse("")

    def outputColumnName: String =
      s"${aggregationPart.inputColumn}_$opSuffix${aggregationPart.window.suffix}$bucketSuffix"
  }

  implicit class AggregationOps(aggregation: Aggregation) {

    // one agg part per bucket per window
    // unspecified windows are converted to one unbounded window
    def unpack: Seq[AggregationPart] = {
      val windows = Option(aggregation.windows)
        .map(_.toScala)
        .getOrElse(Seq(WindowUtils.Unbounded))
        .toSeq
      val buckets = Option(aggregation.buckets)
        .map(_.toScala)
        .getOrElse(Seq(null))
        .toSeq
      for (bucket <- buckets; window <- windows) yield {
        Builders.AggregationPart(
          aggregation.operation,
          aggregation.inputColumn,
          window,
          Option(aggregation.argMap)
            .map(
              _.toScala
            )
            .orNull,
          bucket
        )
      }
    }

    // one agg part per bucket
    // ignoring the windowing
    def unWindowed: Seq[AggregationPart] = {
      val buckets = Option(aggregation.buckets)
        .map(_.toScala)
        .getOrElse(Seq(null))
        .toSeq
      for (bucket <- buckets) yield {
        Builders.AggregationPart(
          aggregation.operation,
          aggregation.inputColumn,
          WindowUtils.Unbounded,
          Option(aggregation.argMap)
            .map(
              _.toScala.toMap
            )
            .orNull,
          bucket
        )
      }
    }
  }

  case class WindowMapping(aggregationPart: AggregationPart, baseIrIndex: Int, millis: Long)

  case class UnpackedAggregations(perBucket: Array[AggregationPart], perWindow: Array[WindowMapping])

  object UnpackedAggregations {
    def from(aggregations: Seq[Aggregation]): UnpackedAggregations = {
      var counter = 0
      val perBucket = new mutable.ArrayBuffer[AggregationPart]
      val perWindow = new mutable.ArrayBuffer[WindowMapping]
      aggregations.foreach { agg =>
        val buckets = Option(agg.buckets)
          .map(_.toScala)
          .getOrElse(Seq(null)) // this doesn't return empty Seq because this will act as a "global"
        // unwindowed AggregationPart. in particular for collpasedIr in temporal cases
        val windows = Option(agg.windows)
          .map(_.toScala)
          .getOrElse(Seq(WindowUtils.Unbounded))
        for (bucket <- buckets) {
          perBucket += Builders.AggregationPart(
            agg.operation,
            agg.inputColumn,
            WindowUtils.Unbounded,
            Option(agg.argMap)
              .map(
                _.toScala.toMap
              )
              .orNull,
            bucket
          )
          for (window <- windows) {
            perWindow += WindowMapping(
              Builders.AggregationPart(agg.operation,
                                       agg.inputColumn,
                                       window,
                                       Option(agg.argMap)
                                         .map(
                                           _.toScala.toMap
                                         )
                                         .orNull,
                                       bucket),
              counter,
              if (window != null) window.millis else -1
            )
          }
          counter += 1
        }
      }
      UnpackedAggregations(perBucket = perBucket.toArray, perWindow = perWindow.toArray)
    }
  }

  implicit class AggregationsOps(aggregations: Seq[Aggregation]) {
    private def hasTimedAggregations: Boolean =
      aggregations.exists(_.operation match {
        case LAST_K | FIRST_K | LAST | FIRST => true
        case _                               => false
      })

    def hasWindows: Boolean = aggregations.exists(_.windows != null)

    def needsTimestamp: Boolean = hasWindows || hasTimedAggregations

    def allWindowsOpt: Option[Seq[Window]] =
      Option(aggregations).map { aggs =>
        aggs.flatMap { agg =>
          Option(agg.windows)
            .map(
              _.toScala
            )
            .getOrElse(Seq(null))
        }
      }
  }

  implicit class SourceOps(source: Source) {
    def dataModel: DataModel = {
      assert(source.isSetEntities || source.isSetEvents || source.isSetJoinSource, "Source type is not specified")
      if (source.isSetEntities) ENTITIES
      else if (source.isSetEvents) EVENTS
      else source.getJoinSource.getJoin.left.dataModel
    }

    def rootQuery: Query = {
      if (source.isSetEntities) {
        source.getEntities.query
      } else if (source.isSetEvents) {
        source.getEvents.query
      } else {
        source.getJoinSource.getJoin.getLeft.query
      }
    }

    def query: Query = {
      if (source.isSetEntities) {
        source.getEntities.query
      } else if (source.isSetEvents) {
        source.getEvents.query
      } else {
        source.getJoinSource.query
      }
    }

    lazy val rootTable: String = {
      if (source.isSetEntities) {
        source.getEntities.getSnapshotTable
      } else if (source.isSetEvents) {
        source.getEvents.getTable
      } else {
        source.getJoinSource.getJoin.left.table
      }
    }

    lazy val rawTable: String = {
      if (source.isSetEntities) { source.getEntities.getSnapshotTable }
      else if (source.isSetEvents) { source.getEvents.getTable }
      else { source.getJoinSource.getJoin.finalOutputTable }
    }

    def mutationsTable: Option[String] = {
      def mutationsTableFor(current: Source): Option[String] = {
        if (current == null) None
        else if (current.isSetEntities) {
          Option(current.getEntities.getMutationTable)
        } else if (current.isSetJoinSource) {
          // JoinSource is a union arm, so recurse through left-side joins before reading EntitySource fields.
          Option(current.getJoinSource)
            .flatMap(joinSource => Option(joinSource.getJoin))
            .flatMap(join => mutationsTableFor(join.getLeft))
        } else {
          None
        }
      }

      mutationsTableFor(source)
    }

    def overwriteTable(table: String): Unit = {
      if (source.isSetEntities) { source.getEntities.setSnapshotTable(table) }
      else if (source.isSetEvents) { source.getEvents.setTable(table) }
      else {
        val metadata = source.getJoinSource.getJoin.getMetaData
        val Array(namespace, tableName) = table.split(".")
        metadata.setOutputNamespace(namespace)
        metadata.setName(tableName)
      }
    }

    def table: String = rawTable.cleanSpec

    def subPartitionFilters: Map[String, String] = {
      val subPartitionFiltersTry = Try(
        rawTable
          .split("/")
          .tail
          .map { partitionDef =>
            val splitPartitionDef = partitionDef.split("=")
            (splitPartitionDef.head, splitPartitionDef(1))
          }
          .toMap)

      subPartitionFiltersTry match {
        case Success(value) => value
        case Failure(exception) => {
          throw new Exception(s"Table ${rawTable} has mal-formatted sub-partitions", exception)
        }
      }
    }

    def isCumulative: Boolean = {
      if (source.isSetEntities) false
      else if (source.isSetEvents) source.getEvents.isCumulative
      else source.getJoinSource.getJoin.left.isCumulative
    }

    def topic: String = {
      if (source.isSetEntities) {
        source.getEntities.getMutationTopic
      } else if (source.isSetEvents) {
        source.getEvents.getTopic
      } else if (source.isSetJoinSource) {
        source.getJoinSource.getJoin.getLeft.topic
      } else {
        null
      }
    }

    /** If the streaming topic has additional args. Parse them to be used by streamingImpl.
      * Example: kafkatopic/schema=deserializationClass/version=2.0/host=host_url/port=9999
      * -> Map(schema -> deserializationClass, version -> 2.0, host -> host_url, port -> 9999)
      */
    def topicTokens: Map[String, String] = {
      source.topic
        .split("/")
        .drop(1)
        .map { tok =>
          val tokens = tok.split("=", 2)
          tokens(0) -> tokens(1)
        }
        .toMap
    }

    /** Topic without kwargs
      */
    def cleanTopic: String = source.topic.cleanSpec

    def partitionInterval: Window = Option(source.query.partitionInterval).getOrElse(WindowUtils.onePartition)
  }

  implicit class GroupByOps(groupBy: GroupBy) extends GroupBy(groupBy) {

    def keyNameForKvStore: String = {
      _keyNameForKvStore(groupBy.metaData, GroupByFolder)
    }

    def allWindows: Array[Window] = {
      groupBy.aggregations
        .iterator()
        .toScala
        .flatMap { agg =>
          Option(agg.windows)
            .map(_.iterator().toScala)
            .getOrElse(Array(WindowUtils.Null).iterator)
        }
        .toArray
        .distinct
    }

    def maxWindow: Option[Window] = {
      val allWindowsOpt = Option(groupBy.aggregations)
        .flatMap(_.toScala.allWindowsOpt)
      allWindowsOpt.flatMap { windows =>
        if (windows.contains(null) || windows.isEmpty) None
        else Some(windows.maxBy(_.millis))
      }
    }

    def semanticHash: String = {
      val newGroupBy = groupBy.deepCopy()
      newGroupBy.unsetMetaData()
      val base = ThriftJsonCodec.md5Digest(newGroupBy)
      Option(groupBy.metaData).map(_.mixGridToken(base)).getOrElse(base)
    }

    def partitionSpec(defaultSpec: PartitionSpec): PartitionSpec =
      groupBy.metaData.partitionSpec(defaultSpec)

    def dataModel: DataModel = {
      val models = groupBy.sources.toScala
        .map(_.dataModel)
      assert(models.distinct.length == 1,
             s"All source of the groupBy: ${groupBy.metaData.name} " +
               "should be of the same type. Either 'Events' or 'Entities'")
      models.head
    }

    lazy val inferredAccuracy: Accuracy = inferredAccuracyImpl

    private def inferredAccuracyImpl: Accuracy = {
      // if user specified something - respect it
      if (groupBy.accuracy != null) return groupBy.accuracy
      // if a topic is specified - then treat it as temporally accurate
      val validTopics = groupBy.sources.toScala
        .map(_.topic)
        .filter(_ != null)
      if (validTopics.nonEmpty) Accuracy.TEMPORAL else Accuracy.SNAPSHOT
    }

    def setups: Seq[String] = {
      groupBy.sources
        .iterator()
        .toScala
        .map(_.query.setups)
        .flatMap(setupList => Option(setupList).map(_.iterator().toScala).getOrElse(Iterator.empty))
        .toSeq
        .distinct
    }

    def copyForVersioningComparison: GroupBy = {
      val newGroupBy = groupBy.deepCopy()
      newGroupBy.setMetaData(newGroupBy.metaData.copyForVersioningComparison)
      newGroupBy
    }

    lazy val batchDataset: String = s"${groupBy.metaData.cleanName.toUpperCase()}_BATCH"
    lazy val streamingDataset: String = s"${groupBy.metaData.cleanName.toUpperCase()}_STREAMING"

    def kvTable: String = s"${groupBy.metaData.outputTable}_upload"

    def streamingSource: Option[Source] =
      groupBy.sources.toScala
        .find(_.topic != null)

    // de-duplicate all columns necessary for aggregation in a deterministic order
    // so we use distinct instead of toSet here
    def aggregationInputs: Array[String] =
      groupBy.aggregations
        .iterator()
        .toScala
        .flatMap(agg =>
          Option(agg.buckets)
            .map(_.iterator().toScala.toSeq)
            .getOrElse(Seq.empty) :+ agg.inputColumn)
        .toArray
        .distinct

    def valueColumns: Array[String] =
      Option(groupBy.aggregations)
        .map(
          _.iterator().toScala
            .flatMap(agg => agg.unpack.map(_.outputColumnName))
            .toArray
        )
        .getOrElse(
          // no-agg case
          groupBy.sources
            .get(0)
            .query
            .selects
            .keySet()
            .iterator()
            .toScala
            .filterNot(groupBy.keyColumns.contains)
            .toArray
        )

    def keys(partitionColumn: String): Seq[String] = {
      val baseKeys = if (groupBy.isSetKeyColumns) groupBy.keyColumns.toScala else List()
      val partitionKey = if (baseKeys.contains(partitionColumn)) None else Some(partitionColumn)
      val timeKey =
        if (groupBy.inferredAccuracy == Accuracy.TEMPORAL && !baseKeys.contains(Constants.TimeColumn))
          Some(Constants.TimeColumn)
        else
          None

      baseKeys ++ partitionKey ++ timeKey
    }

    def hasDerivations: Boolean = groupBy.isSetDerivations && !groupBy.derivations.isEmpty
    lazy val derivationsScala: List[Derivation] =
      if (groupBy.hasDerivations) groupBy.derivations.toScala else List.empty
    lazy val derivationsContainStar: Boolean = groupBy.hasDerivations && derivationsScala.iterator.exists(_.name == "*")
    lazy val derivationsWithoutStar: List[Derivation] =
      if (groupBy.hasDerivations) derivationsScala.filterNot(_.name == "*") else List.empty
    lazy val areDerivationsRenameOnly: Boolean =
      groupBy.hasDerivations && groupBy.derivations.toScala.areDerivationsRenameOnly
    lazy val derivationExpressionSet: Set[String] =
      if (groupBy.hasDerivations) derivationsScala.iterator.map(_.expression).toSet else Set.empty

    case class QueryParts(selects: Option[Seq[String]], wheres: Seq[String])

    def buildQueryParts(query: Query): QueryParts = {
      val selects = query.getQuerySelects
      val timeColumn = Option(query.timeColumn).getOrElse(Constants.TimeColumn)

      val fillIfAbsent = (groupBy.dataModel match {
        case DataModel.ENTITIES =>
          Map(Constants.ReversalColumn -> Constants.ReversalColumn,
              Constants.MutationTimeColumn -> Constants.MutationTimeColumn)
        case DataModel.EVENTS => Map(Constants.TimeColumn -> timeColumn)
      })

      val baseWheres = Option(query.wheres).map(_.toScala).getOrElse(Seq.empty[String])
      val wheres = baseWheres ++ timeWheres(timeColumn)

      val allSelects = Option(selects).map(fillIfAbsent ++ _).map { m =>
        m.map { case (name, expr) =>
          s"($expr) AS $name"
        }.toSeq
      }
      QueryParts(allSelects, wheres)
    }

    def servingFlagValue(flag: String): Option[String] = {
      for (
        execInfo <- Option(groupBy.metaData.executionInfo);
        conf <- Option(execInfo.conf);
        servingConf <- Option(conf.modeConfigs.get("serving"));
        value <- Option(servingConf.get(flag))
      ) {
        return Some(value)
      }
      None
    }

    def dontThrowOnDecodeFailFlag: Boolean = servingFlagValue("decode.throw_on_fail").exists(_.toLowerCase() == "false")

    // build left streaming query for join source runner
    def buildLeftStreamingQuery(query: Query, defaultFieldNames: Seq[String]): String = {
      val queryParts = groupBy.buildQueryParts(query)
      val streamingInputTable =
        Option(groupBy.metaData.name).map(_.replaceAll("[^a-zA-Z0-9_]", "_")).orNull + "_stream"
      s"""SELECT
         |  ${queryParts.selects.getOrElse(defaultFieldNames).mkString(",\n  ")}
         |FROM $streamingInputTable
         |WHERE ${queryParts.wheres.map("(" + _ + ")").mkString(" AND ")}
         |""".stripMargin
    }

    def buildStreamingQuery: String = {
      val streamingSource = groupBy.streamingSource.get
      if (!streamingSource.isSetJoinSource) {
        val query = streamingSource.query
        val selects = query.getQuerySelects
        val timeColumn = Option(query.timeColumn).getOrElse(Constants.TimeColumn)
        val fillIfAbsent = groupBy.dataModel match {
          case DataModel.ENTITIES =>
            Some(
              Map(Constants.TimeColumn -> timeColumn,
                  Constants.ReversalColumn -> null,
                  Constants.MutationTimeColumn -> null))
          case DataModel.EVENTS => Some(Map(Constants.TimeColumn -> timeColumn))
        }
        val keys = groupBy.getKeyColumns.toScala

        val baseWheres = Option(query.wheres).map(_.toScala).getOrElse(Seq.empty[String])
        val selectMap = Option(selects).getOrElse(Map.empty[String, String])
        val keyWhereOption = keys
          .map { key =>
            s"${selectMap.getOrElse(key, key)} IS NOT NULL"
          }
          .mkString(" OR ")
        val streamingInputTable =
          Option(groupBy.metaData.name).map(_.replaceAll("[^a-zA-Z0-9_]", "_")).orNull + "_stream"
        QueryUtils.build(
          selects,
          streamingInputTable,
          baseWheres ++ timeWheres(timeColumn) :+ s"($keyWhereOption)",
          fillIfAbsent
        )
      } else {
        // todo: this logic is similar in JoinSourceRunner, we can simplify it to a single place
        val query = streamingSource.getJoinSource.join.left.query
        groupBy.buildLeftStreamingQuery(query, groupBy.keyColumns.toScala)
      }
    }

    private def timeWheres(timeColumn: String) = {
      groupBy.dataModel match {
        case DataModel.ENTITIES => Seq(s"${Constants.MutationTimeColumn} is NOT NULL")
        case DataModel.EVENTS   => Seq(s"$timeColumn is NOT NULL")
      }
    }
  }

  implicit class StringOps(string: String) {
    def sanitize: String = Option(string).map(_.replaceAll("[^a-zA-Z0-9_]", "_")).orNull

    def cleanSpec: String = string.split("/").head

    // derive a feature name key from path to file
    def confPathToKey: String = {
      // capture <conf_type>/<team>/<conf_name> as key e.g joins/team/team.example_join.v1
      string.split("/").takeRight(3).mkString("/")
    }
  }

  implicit class ExternalSourceOps(externalSource: ExternalSource) extends ExternalSource(externalSource) {
    private def schemaNames(schema: TDataType): Array[String] = schemaFields(schema).map(_.name)

    private def schemaFields(schema: TDataType): Array[StructField] =
      schema.params.toScala
        .map(field => StructField(field.name, DataType.fromTDataType(field.dataType)))
        .toArray

    lazy val keyNames: Array[String] = schemaNames(externalSource.keySchema)
    lazy val valueNames: Array[String] = schemaNames(externalSource.valueSchema)
    lazy val keyFields: Array[StructField] = schemaFields(externalSource.keySchema)
    lazy val valueFields: Array[StructField] = schemaFields(externalSource.valueSchema)

    def isContextualSource: Boolean = externalSource.metadata.name == Constants.ContextualSourceName
  }

  object KeyMappingHelper {
    // key mapping is defined as {left_col1: right_col1}, on the right there can be two keys [right_col1, right_col2]
    // Left is implicitly assumed to have right_col2
    // We need to convert a map {left_col1: a, right_col2: b, irrelevant_col: c} into {right_col1: a, right_col2: b}
    // The way to do this efficiently is to "flip" the keymapping into {right_col1: left_col1} and save it.
    // And later "apply" the flipped mapping.
    def flip(leftToRight: java.util.Map[String, String]): Map[String, String] = {
      Option(leftToRight)
        .map(mp =>
          mp.toScala
            .map({ case (key, value) => value -> key }))
        .getOrElse(Map.empty[String, String])
    }
  }

  implicit class ExternalPartOps(externalPart: ExternalPart) extends ExternalPart(externalPart) {
    lazy val fullName: String =
      Constants.ExternalPrefix + "_" +
        Option(externalPart.prefix).map(_ + "_").getOrElse("") +
        externalPart.source.metadata.name.sanitize

    def apply(query: Map[String, Any], flipped: Map[String, String], right_keys: Seq[String]): Map[String, AnyRef] = {
      val rightToLeft = right_keys.map(k => k -> flipped.getOrElse(k, k))
      val missingKeys = rightToLeft.iterator.map(_._2).filterNot(query.contains).toSet

      // for contextual features, we automatically populate null if any of the keys are missing
      // otherwise, an exception is thrown which will be converted to soft-fail in Fetcher code
      if (missingKeys.nonEmpty && !externalPart.source.isContextualSource) {
        throw KeyMissingException(externalPart.source.metadata.name, missingKeys.toSeq, query)
      }
      rightToLeft.map { case (rightKey, leftKey) =>
        rightKey -> query.getOrElse(leftKey, null).asInstanceOf[AnyRef]
      }.toMap
    }

    def applyMapping(query: Map[String, Any]): Map[String, AnyRef] =
      apply(query, rightToLeft, keyNames)

    lazy val rightToLeft: Map[String, String] = KeyMappingHelper.flip(externalPart.keyMapping)
    private lazy val keyNames = externalPart.source.keyNames

    def semanticHash: String = {
      val newExternalPart = externalPart.deepCopy()
      newExternalPart.source.unsetMetadata()
      ThriftJsonCodec.md5Digest(newExternalPart)
    }

    lazy val keySchemaFull: Array[StructField] = externalPart.source.keyFields.map(field =>
      StructField(externalPart.rightToLeft.getOrElse(field.name, field.name), field.fieldType))

    lazy val valueSchemaFull: Array[StructField] =
      externalPart.source.valueFields.map(field => StructField(fullName + "_" + field.name, field.fieldType))

    def isContextual: Boolean = externalPart.source.isContextualSource
  }

  implicit class JoinPartOps(joinPart: JoinPart) extends JoinPart(joinPart) {
    // FullPrefix is used for logging
    lazy val fullPrefix: String = (Option(prefix) ++ Some(groupBy.getMetaData.cleanName)).mkString("_")

    // columnPrefix is the "effective" prefix used for output column name generation
    // For long names, we use the gb name, else for short names we use the keys
    // We set the default to false in python, however if it's unset in the config, default back to true (legacy)
    private lazy val gbPrefix: String = if (Option(joinPart.useLongNames).getOrElse(true)) {
      groupBy.getMetaData.cleanNameWithoutVersion
    } else {
      groupBy.getKeyColumns.toScala.mkString("_")
    }
    lazy val columnPrefix: String = {
      val raw = (Option(prefix) ++ Some(gbPrefix)).mkString("_")
      if (raw.isEmpty) "" else raw + "_"
    }

    lazy val leftToRight: Map[String, String] = rightToLeft.map { case (key, value) => value -> key }

    def valueColumns: Seq[String] = joinPart.groupBy.valueColumns.map(columnPrefix + _)

    def rightToLeft: Map[String, String] = {
      val rightToRight = joinPart.groupBy.keyColumns.toScala.map { key => key -> key }.toMap
      Option(joinPart.keyMapping)
        .map { leftToRight =>
          val rToL = leftToRight.toScala.map { case (left, right) =>
            right -> left
          }.toMap
          rightToRight ++ rToL
        }
        .getOrElse(rightToRight)
    }

    def copyForVersioningComparison: JoinPart = {
      val newJoinPart = joinPart.deepCopy()
      newJoinPart.setGroupBy(newJoinPart.groupBy.copyForVersioningComparison)
      newJoinPart
    }

    def constructJoinPartSchema(schemaField: StructField): StructField = {
      StructField(joinPart.columnPrefix + schemaField.name, schemaField.fieldType)
    }
  }

  implicit class BootstrapPartOps(val bootstrapPart: BootstrapPart) extends Serializable {

    /** Compress the info such that the hash can be stored at record and
      * used to track which records are populated by which bootstrap tables
      */
    def semanticHash: String = {
      val newPart = bootstrapPart.deepCopy()
      bootstrapPart.unsetMetaData()
      ThriftJsonCodec.md5Digest(newPart)
    }

    def keys(join: Join, partitionColumn: String): Seq[String] = {
      val definedKeys = if (bootstrapPart.isSetKeyColumns) {
        bootstrapPart.keyColumns.toScala
      } else if (join.isSetRowIds) {
        join.getRowIds.toScala
      } else {
        throw new Exception(s"Bootstrap's join key for bootstrap is NOT set for join ${join.metaData.name}")
      }
      if (definedKeys.contains(partitionColumn)) {
        definedKeys
      } else {
        definedKeys :+ partitionColumn
      }
    }

    lazy val startPartition: String = Option(bootstrapPart.query).map(_.startPartition).orNull
    lazy val endPartition: String = Option(bootstrapPart.query).map(_.endPartition).orNull
  }

  object JoinOps {
    private val identifierRegex: Pattern = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*")
    def isIdentifier(s: String): Boolean = identifierRegex.matcher(s).matches()
  }

  implicit class JoinOps(val join: Join) extends Serializable {
    def keyNameForKvStore: String = {
      _keyNameForKvStore(join.metaData, JoinFolder)
    }

    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    // all keys as they should appear in left that are being used on right
    def leftKeyCols: Array[String] = {
      join.joinParts.toScala
        .flatMap {
          _.rightToLeft.values
        }
        .toSet
        .toArray
    }

    def isModularMode: Boolean =
      Option(join.metaData)
        .flatMap(md => Option(md.executionInfo))
        .flatMap(ei => Option(ei.conf))
        .flatMap(c => Option(c.common))
        .exists(_.get("modular_execution") == "true")

    def isUnionJoinEligible: Boolean =
      join.left.isSetEvents &&
        Option(join.joinParts).exists(_.size() == 1) &&
        join.getJoinParts.get(0).groupBy.inferredAccuracy == Accuracy.TEMPORAL &&
        !join.isSetBootstrapParts

    def derivedOutputTable: String = s"${join.metaData.outputTable}__derived"

    def hasSeparateDerivedOutput: Boolean =
      join.isModularMode && join.hasDerivations && !isUnionJoinEligible

    def finalOutputTable: String =
      if (hasSeparateDerivedOutput) derivedOutputTable else join.metaData.outputTable

    def partitionSpec(defaultSpec: PartitionSpec): PartitionSpec =
      join.metaData.partitionSpec(defaultSpec)

    def historicalBackfill: Boolean = {
      if (join.metaData.isSetExecutionInfo && join.metaData.executionInfo.isSetHistoricalBackfill) {
        join.metaData.executionInfo.historicalBackfill
      } else {
        true
      }
    }

    def partOutputTable(jp: JoinPart): String =
      (Seq(join.metaData.outputTable) ++ Option(jp.prefix) :+ jp.groupBy.metaData.cleanName).mkString("_")

    private val leftSourceKey: String = "left_source"
    private val derivedKey: String = "derived"

    /*
     * semanticHash contains hashes of left side and each join part, and is used to detect join definition
     * changes and determine whether any intermediate/final tables of the join need to be recomputed.
     */
    def semanticHash: Map[String, String] = {
      // the join's own output grid rides on the left hash: changing partitionInterval or
      // partitionOffset renames every ds in the intermediate/final tables computed off the
      // left, so it must read as a left change
      val baseLeftHash = ThriftJsonCodec.md5Digest(join.left)
      val leftHash = Option(join.metaData).map(_.mixGridToken(baseLeftHash)).getOrElse(baseLeftHash)
      logger.info(s"Join Left Hash: $leftHash")
      logger.info(s"Join Left Object: ${ThriftJsonCodec.toJsonStr(join.left)}")
      val partHashes = join.joinParts.toScala.map { jp => partOutputTable(jp) -> jp.groupBy.semanticHash }.toMap
      val derivedHashMap = Option(join.derivations)
        .map { derivations =>
          val derivedHash =
            derivations.iterator().toScala.map(d => s"${d.expression} as ${d.name}").mkString(", ").hashCode.toHexString
          Map(derivedKey -> derivedHash)
        }
        .getOrElse(Map.empty)
      val bootstrapHash = ThriftJsonCodec.md5Digest(join.bootstrapParts)
      partHashes ++ Map(leftSourceKey -> leftHash, join.metaData.bootstrapTable -> bootstrapHash) ++ derivedHashMap
    }

    def externalPartColumns: Map[String, Array[String]] =
      Option(join.onlineExternalParts) match {
        case Some(parts) =>
          parts.toScala.map { part =>
            val keys = part.source.getKeySchema.params.toScala.map(_.name)
            val values = part.source.getValueSchema.params.toScala.map(_.name)
            part.fullName -> (keys ++ values).toArray
          }.toMap
        case None => Map.empty
      }

    def derivedColumns: Array[String] =
      Option(join.getDerivations) match {
        case Some(derivations) =>
          derivations.toScala.flatMap { derivation =>
            derivation.getName match {
              case "*" => None
              case _   => Some(derivation.getName)
            }
          }.toArray
        case None => Array.empty
      }

    // renamed cols are no longer part of the output
    private def renamedColumns: Set[String] =
      Option(join.derivations)
        .map {
          _.toScala.renameOnlyDerivations.map(_.expression).toSet
        }
        .getOrElse(Set.empty)

    def joinPartColumns: Map[String, Array[String]] =
      Option(join.getJoinParts) match {
        case None => Map.empty
        case Some(parts) =>
          parts.toScala.map { part =>
            val prefix = Option(part.prefix)
            val groupByName = part.getGroupBy.getMetaData.cleanName
            val partName = (prefix.toSeq :+ groupByName).mkString("_")

            val outputColumns = part.getGroupBy.valueColumns
            val cols = outputColumns.map { column =>
              (prefix.toSeq :+ groupByName :+ column).mkString("_")
            }
            partName -> cols
          }.toMap
      }

    def outputColumnsByGroup: Map[String, Array[String]] = {
      val preDeriveCols = (joinPartColumns ++ externalPartColumns)
      val preDerivedWithoutRenamed = preDeriveCols.mapValues(_.filterNot(renamedColumns.contains)).toMap
      val derivedColumns: Array[String] = Option(join.derivations) match {
        case Some(derivations) => derivations.toScala.map { _.getName }.filter(_ == "*").toArray
        case None              => Array.empty
      }
      preDerivedWithoutRenamed ++ Map("derivations" -> derivedColumns)
    }

    def keyColumns: Array[String] = {
      val joinPartKeys = join.joinParts.toScala.flatMap(_.groupBy.keyColumns.toScala).toSet
      val externalKeys = join.onlineExternalParts.toScala.flatMap(_.source.keyNames).toSet
      val bootstrapKeys = join.bootstrapParts.toScala.flatMap(_.keyColumns.toScala).toSet
      (joinPartKeys ++ externalKeys ++ bootstrapKeys).toArray
    }

    def leftChanged(oldSemanticHash: Map[String, String]): Boolean = {
      // Checks for semantic changes in left or bootstrap, because those are saved together
      val bootstrapExistsAndChanged = oldSemanticHash.contains(join.metaData.bootstrapTable) && oldSemanticHash.get(
        join.metaData.bootstrapTable) != semanticHash.get(join.metaData.bootstrapTable)
      logger.info(s"Bootstrap table changed: $bootstrapExistsAndChanged")
      logger.info(s"Old Semantic Hash: $oldSemanticHash")
      logger.info(s"New Semantic Hash: $semanticHash")
      oldSemanticHash.get(leftSourceKey) != semanticHash.get(leftSourceKey) || bootstrapExistsAndChanged
    }

    def tablesToDrop(oldSemanticHash: Map[String, String]): Seq[String] = {
      val newSemanticHash = semanticHash
      // only right join part hashes for convenience
      def partHashes(semanticHashMap: Map[String, String]): Map[String, String] = {
        semanticHashMap.filter { case (name, _) => name != leftSourceKey && name != derivedKey }
      }

      // drop everything if left source changes
      val partsToDrop = if (leftChanged(oldSemanticHash)) {
        val oldPartHashes = partHashes(oldSemanticHash)
        oldPartHashes.keys.toSeq
      } else {
        val newPartHashes = partHashes(newSemanticHash)
        val changed = newPartHashes.flatMap { case (key, newVal) =>
          oldSemanticHash.get(key).filter(_ != newVal).map(_ => key)
        }
        val deleted = partHashes(oldSemanticHash).keys.filterNot(newSemanticHash.contains)
        (changed ++ deleted).toSeq
      }

      val added = newSemanticHash.keys.filter(!oldSemanticHash.contains(_)).filter {
        // introduce boostrapTable as a semantic_hash but skip dropping to avoid recompute if it is empty
        case key if key == join.metaData.bootstrapTable => join.isSetBootstrapParts && !join.bootstrapParts.isEmpty
        case _                                          => true
      }

      val derivedChanges = oldSemanticHash.get(derivedKey) != newSemanticHash.get(derivedKey)
      // TODO: make this incremental, retain the main table and continue joining, dropping etc
      val mainTable = if (partsToDrop.nonEmpty || added.nonEmpty || derivedChanges) {
        Some(join.metaData.outputTable)
      } else None
      partsToDrop ++ mainTable
    }

    def isProduction: Boolean = join.getMetaData.isProduction

    def team: String = join.getMetaData.getTeam

    private def generateSkewFilterSql(key: String, values: Seq[String]): String = {
      val nulls = Seq("null", "Null", "NULL")
      val nonNullFilters = Some(s"$key NOT IN (${values.filterNot(nulls.contains).mkString(", ")})")
      val nullFilters = if (values.exists(nulls.contains)) Some(s"$key IS NOT NULL") else None
      (nonNullFilters ++ nullFilters).mkString(" AND ")
    }

    // TODO: validate that non keys are not specified in - join.skewKeys
    def skewFilter(keys: Option[Seq[String]] = None, joiner: String = " OR "): Option[String] = {
      Option(join.skewKeys).map { jmap =>
        val result = jmap.toScala
          .filterKeys(key =>
            keys.forall {
              _.contains(key)
            })
          .map { case (leftKey, values) =>
            assert(
              leftKeyCols.contains(leftKey),
              s"specified skew filter for $leftKey is not used as a key in any join part. " +
                s"Please specify key columns in skew filters: [${leftKeyCols.mkString(", ")}]"
            )
            generateSkewFilterSql(leftKey, values.toScala.toSeq)
          }
          .filter(_.nonEmpty)
          .mkString(joiner)
        logger.info(s"Generated join left side skew filter:\n    $result")
        result
      }
    }

    def partSkewFilter(joinPart: JoinPart, joiner: String = " OR "): Option[String] = {
      Option(join.skewKeys).flatMap { jmap =>
        val result = jmap.toScala
          .flatMap { case (leftKey, values) =>
            Option(joinPart.keyMapping)
              .map(_.toScala.getOrElse(leftKey, leftKey))
              .orElse(Some(leftKey))
              .filter(joinPart.groupBy.keyColumns.contains(_))
              .map(generateSkewFilterSql(_, values.toScala))
          }
          .filter(_.nonEmpty)
          .mkString(joiner)

        if (result.nonEmpty) {
          logger.info(s"Generated join part skew filter for ${joinPart.groupBy.metaData.name}:\n    $result")
          Some(result)
        } else None
      }
    }

    def setups: Seq[String] =
      (join.left.query.setupsSeq ++ join.joinParts.toScala
        .flatMap(_.groupBy.setups)).distinct

    lazy val joinPartOps: Seq[JoinPartOps] =
      Option(join.joinParts)
        .getOrElse(new util.ArrayList[JoinPart]())
        .toScala
        .toSeq
        .map(new JoinPartOps(_))

    def outputAsSource: Source = {
      val source = new Source()

      val query = new Query()
      query.setStartPartition(join.left.query.getStartPartition)
      query.setEndPartition(join.left.query.getEndPartition)

      join.left.dataModel match {
        case ENTITIES =>
          val src = new EntitySource()
          src.setSnapshotTable(join.metaData.outputTable)
          src.setQuery(query)
        case EVENTS =>
          val src = new EventSource()
          src.setTable(join.metaData.outputTable)
          src.setQuery(query)
      }
      source
    }

    def logFullValues: Boolean = true // TODO: supports opt-out in the future

    def hasDerivations: Boolean = join.isSetDerivations && !join.derivations.isEmpty
    lazy val derivationsScala: List[Derivation] = if (join.hasDerivations) join.derivations.toScala else List.empty
    lazy val derivationsContainStar: Boolean = join.hasDerivations && derivationsScala.iterator.exists(_.name == "*")
    lazy val derivationsWithoutStar: List[Derivation] =
      if (join.hasDerivations) derivationsScala.filterNot(_.name == "*") else List.empty
    lazy val areDerivationsRenameOnly: Boolean = join.hasDerivations && derivationsScala.areDerivationsRenameOnly
    lazy val derivationExpressionSet: Set[String] =
      if (join.hasDerivations) derivationsScala.iterator.map(_.expression).toSet else Set.empty

  }

  implicit class StringsOps(strs: Iterable[String]) {
    def pretty: String = {
      if (strs.nonEmpty)
        "\n    " + strs.mkString(",\n    ") + "\n"
      else
        ""
    }

    def prettyInline: String = strs.mkString("[", ",", "]")
  }

  implicit class TableInfoOps(ti: TableInfo) {
    def partitionSpec(defaultSpec: PartitionSpec): PartitionSpec = {
      val column = Option(ti).flatMap((q) => Option(q.partitionColumn)).getOrElse(defaultSpec.column)
      val format = Option(ti).flatMap((q) => Option(q.partitionFormat)).getOrElse(defaultSpec.format)
      val declaredInterval = Option(ti).flatMap((q) => Option(q.partitionInterval))
      val timePartitioned = Option(ti).exists(t => t.isSetTimePartitioned && t.timePartitioned)
      // a time-partitioned table has no physical grid - its column is a real timestamp, so it
      // is sensed and quantized on the DOWNSTREAM node's grid: inherit the default spec's full
      // partitionInterval+partitionOffset instead of assuming daily (which would quantize an
      // intraday requirement up to a whole day and stall sub-daily readiness until the day closes)
      val interval = declaredInterval.getOrElse(
        if (timePartitioned) WindowUtils.fromMillis(defaultSpec.spanMillis) else WindowUtils.Day)
      // a conf that declares its own interval gets offset 0 unless it also declares one - the
      // default spec's offset must not leak into an explicitly-intervaled table
      val offset = Option(ti)
        .flatMap((q) => Option(q.partitionOffset))
        .map(_.millis)
        .getOrElse(if (declaredInterval.isDefined) 0L else defaultSpec.offsetMillis)
      PartitionSpec(column, format, interval.millis, offset)
    }

    /** Stamps the spec's partition fields onto this TableInfo; the offset is set only when
      * nonzero so existing daily confs serialize byte-identically (absent offset means
      * midnight boundaries).
      */
    def withSpec(spec: PartitionSpec): TableInfo = {
      ti.setPartitionColumn(spec.column)
        .setPartitionFormat(spec.format)
        .setPartitionInterval(WindowUtils.fromMillis(spec.spanMillis))
      if (spec.offsetMillis != 0)
        ti.setPartitionOffset(WindowUtils.fromMillis(spec.offsetMillis))
      else
        ti.unsetPartitionOffset()
      ti
    }
  }

  implicit class QueryOps(query: Query) {
    def setupsSeq: Seq[String] = {
      Option(query.setups)
        .map(
          _.toScala.toSeq
        )
        .getOrElse(Seq.empty)
    }

    def getQuerySelects: Map[String, String] = Option(query.selects).map(_.toScala.toMap).orNull

    def enrichedSelects(mutationInfoOnSnapshot: Boolean = false): Map[String, String] = {
      query.selects.toScala ++
        Option(query.timeColumn).map(timeColumn => Constants.TimeColumn -> timeColumn) ++
        Option(query.mutationTimeColumn).map(mutationTimeExpression =>
          Constants.MutationTimeColumn -> (if (mutationInfoOnSnapshot) "0" else mutationTimeExpression)) ++
        Option(query.reversalColumn).map(isBeforeExpression =>
          Constants.ReversalColumn -> (if (mutationInfoOnSnapshot) "false" else isBeforeExpression))
    }

    def enrichedQuery(mutationInfoOnSnapshot: Boolean = false): Query = {
      val result = query.deepCopy()
      result.setSelects(enrichedSelects(mutationInfoOnSnapshot).toJava)
      result
    }

    def partitionSpec(defaultSpec: PartitionSpec): PartitionSpec = {
      val column = Option(query).flatMap(q => Option(q.partitionColumn)).getOrElse(defaultSpec.column)
      val format = Option(query).flatMap(q => Option(q.partitionFormat)).getOrElse(defaultSpec.format)
      val declaredInterval = Option(query).flatMap(q => Option(q.partitionInterval))
      val timePartitioned = Option(query).exists(q => q.isSetTimePartitioned && q.timePartitioned)
      // a time-partitioned source has no physical grid - its column is a real timestamp, so it
      // is sensed and quantized on the DOWNSTREAM node's grid (see TableInfoOps.partitionSpec)
      val interval = declaredInterval.getOrElse(
        if (timePartitioned) WindowUtils.fromMillis(defaultSpec.spanMillis) else WindowUtils.Day)
      // a conf that declares its own interval gets offset 0 unless it also declares one - the
      // default spec's offset must not leak into an explicitly-intervaled source
      val offset = Option(query)
        .flatMap(q => Option(q.partitionOffset))
        .map(_.millis)
        .getOrElse(if (declaredInterval.isDefined) 0L else defaultSpec.offsetMillis)
      PartitionSpec(column, format, interval.millis, offset)
    }
  }

  implicit class ThrowableOps(throwable: Throwable) {
    def traceString: String = {
      val sw = new StringWriter()
      val pw = new PrintWriter(sw)
      throwable.printStackTrace(pw)
      sw.toString();
    }
  }

  implicit class DerivationOps(derivations: List[Derivation]) {
    lazy val derivationsContainStar: Boolean = derivations.iterator.exists(_.name == "*")
    lazy val derivationsWithoutStar: List[Derivation] = derivations.filterNot(_.name == "*")
    lazy val areDerivationsRenameOnly: Boolean = derivationsWithoutStar.forall(d => JoinOps.isIdentifier(d.expression))
    lazy val derivationExpressionSet: Set[String] = derivations.iterator.map(_.expression).toSet
    lazy val derivationExpressionFlippedMap: Map[String, String] =
      derivationsWithoutStar.map(d => d.expression -> d.name).toMap
    lazy val renameOnlyDerivations: List[Derivation] =
      derivationsWithoutStar.filter(d => JoinOps.isIdentifier(d.expression))

    // Used during offline spark job and this method preserves ordering of derivations
    def derivationProjection(baseColumns: Seq[String], ensureKeys: Seq[String] = Seq.empty): Seq[(String, String)] = {

      val wildcardDerivations = if (derivationsContainStar) {
        // select all baseColumns except renamed ones
        val expressions = derivations.iterator.map(_.expression).toSet
        baseColumns.filterNot(expressions)
      } else {
        Seq.empty
      }

      // expand wildcard derivations
      val expandedDerivations = derivations.iterator.flatMap { d =>
        if (d.name == "*") {
          wildcardDerivations.map(c => c -> c)
        } else {
          Seq(d.name -> d.expression)
        }
      }.toSeq

      val expandedDerivationCols = expandedDerivations.map(_._1).toSet

      val missingKeys = ensureKeys
        .filterNot(expandedDerivationCols.contains)
        .map { key => key -> key }

      missingKeys ++ expandedDerivations
    }

    def finalOutputColumn(baseColumns: Seq[String], ensureKeys: Seq[String] = Seq.empty): Seq[Column] = {
      val projections = derivationProjection(baseColumns, ensureKeys)
      val finalOutputColumns = projections
        .flatMap { case (name, expression) =>
          Some(expr(expression).as(name))
        }
      finalOutputColumns.toSeq
    }

    // Used only during online fetching to reduce latency
    def applyRenameOnlyDerivation(baseColumns: Map[String, Any]): Map[String, Any] = {
      assert(
        areDerivationsRenameOnly,
        s"Derivations contain more complex expressions than simple renames: ${derivations.map(d => (d.name, d.expression))}")
      val wildcardDerivations = if (derivationsContainStar) {
        baseColumns.filterNot(derivationExpressionSet contains _._1)
      } else {
        Map.empty[String, Any]
      }

      wildcardDerivations ++ derivationsWithoutStar.map(d => d.name -> baseColumns.getOrElse(d.expression, null)).toMap
    }
  }

  implicit class JoinSourceOps(joinSource: JoinSource) {
    // convert chained joinSource into event or entity sources
    def toDirectSource(joinOutputTable: String): Source = {
      val result = new Source()
      joinSource.join.left.dataModel match {
        case ENTITIES =>
          val inner = new EntitySource()
          inner.setSnapshotTable(joinOutputTable)
          inner.setQuery(Option(joinSource.getQuery).map(_.deepCopy()).orNull)
          inner.setMutationTopic(joinSource.join.left.topic)
          result.setEntities(inner)

        case EVENTS =>
          val inner = new EventSource()
          inner.setTable(joinOutputTable)
          inner.setQuery(Option(joinSource.getQuery).map(_.deepCopy()).orNull)
          inner.setTopic(joinSource.join.left.topic)
          inner.setIsCumulative(joinSource.join.left.isCumulative)
          result.setEvents(inner)
      }
      result
    }
  }

  implicit class StagingQueryOps(stagingQuery: StagingQuery) {
    def keyNameForKvStore: String = {
      _keyNameForKvStore(stagingQuery.metaData, StagingQueryFolder)
    }

    def partitionSpec(defaultSpec: PartitionSpec): PartitionSpec =
      stagingQuery.metaData.partitionSpec(defaultSpec)
  }

  implicit class ModelOps(model: Model) {
    def keyNameForKvStore: String = {
      _keyNameForKvStore(model.metaData, ModelFolder)
    }

    def partitionSpec(defaultSpec: PartitionSpec): PartitionSpec =
      model.metaData.partitionSpec(defaultSpec)
  }

  implicit class NodeOps(node: ai.chronon.planner.Node) {
    def partitionSpec(defaultSpec: PartitionSpec): PartitionSpec =
      Option(node).flatMap(n => Option(n.metaData)).map(_.partitionSpec(defaultSpec)).getOrElse(defaultSpec)
  }

  implicit class DateRangeOps(dateRange: DateRange) {
    def toPartitionRange(implicit partitionSpec: PartitionSpec): PartitionRange = {
      val start = dateRange.startDate
      val end = dateRange.endDate
      new PartitionRange(start, end)
    }
  }

  implicit class ModelTransformsOps(transforms: ModelTransforms) {
    def keyNameForKvStore: String = {
      _keyNameForKvStore(transforms.metaData, ModelFolder)
    }

    def partitionSpec(defaultSpec: PartitionSpec): PartitionSpec =
      transforms.metaData.partitionSpec(defaultSpec)

    def joinSource: Option[JoinSource] =
      Option(transforms.sources)
        .map(_.toScala)
        .getOrElse(Seq.empty)
        .find(_.isSetJoinSource)
        .map(_.getJoinSource)
  }
}
