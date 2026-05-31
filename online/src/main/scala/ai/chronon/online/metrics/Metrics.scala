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

package ai.chronon.online.metrics

import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import io.opentelemetry.api.OpenTelemetry
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object Metrics {
  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  val MetricsEnabled = "ai.chronon.metrics.enabled"
  val MetricsReporter = "ai.chronon.metrics.reporter"
  val MetricsPrefix = "ai.chronon.metrics.prefix"

  private val globalMetricsPrefix: Option[String] = Option(System.getProperty(MetricsPrefix)).filter(_.nonEmpty)

  object Environment extends Enumeration {
    type Environment = String
    val MetaDataFetching = "metadata.fetch"
    val JoinFetching = "join.fetch"
    val JoinSchemaFetching = "join.schema.fetch"
    val ModelTransformsFetching = "modeltransforms.fetch"
    val ModelPredict = "model.predict"
    val GroupByFetching = "group_by.fetch"
    val GroupByUpload = "group_by.upload"
    val GroupByStreaming = "group_by.streaming"
    val Fetcher = "fetcher"
    val JoinOffline = "join.offline"
    val JoinOOC = "join.ooc"
    val GroupByOffline = "group_by.offline"
    val StagingQueryOffline = "staging_query.offline"

    val JoinLogFlatten = "join.log_flatten"
    val KVStore = "kv_store"
    val Orchestrator = "orchestrator"
  }

  import Environment._

  object Tag {
    val GroupBy = "group_by"
    val Join = "join"
    val Model = "model"
    val ModelTransforms = "model_transforms"
    val JoinPartPrefix = "join_part_prefix"
    val StagingQuery = "staging_query"
    val Environment = "environment"
    val Production = "production"
    val Accuracy = "accuracy"
    val Team = "team"
    val Dataset = "dataset"
    val Feature = "feature"
  }

  object Name {
    val FreshnessMillis = "freshness.millis"
    val FreshnessMinutes = "freshness.minutes"
    val LatencyMillis = "latency.millis"
    val LagMillis: String = "lag.millis"
    val BatchLagMillis: String = "micro_batch_lag.millis"
    val QueryDelaySleepMillis: String = "chain.query_delay_sleep.millis"
    val LatencyMinutes = "latency.minutes"

    val PartitionCount = "partition.count"
    val RowCount = "row.count"
    val RequestCount = "request.count"
    val ColumnBeforeCount = "column.before.count"
    val ColumnAfterCount = "column.after.count"
    val FailureCount = "failure.ratio"

    val Bytes = "bytes"
    val KeyBytes = "key.bytes"
    val ValueBytes = "value.bytes"
    val FetchExceptions = "fetch.exception_count"
    val FetchNulls = "fetch.null_count"
    val FetchCount = "fetch.count"
    val FeatureNulls = "feature.null_count"
    val FeatureCount = "feature.count"
    val UploadNullCount = "upload.null_count"

    val PutKeyNullPercent = "put.key.null_percent"
    val PutValueNullPercent = "put.value.null_percent"

    val Exception = "exception"
    val validationFailure = "validation.failure"
    val validationSuccess = "validation.success"
  }

  object Context {

    def apply(environment: Environment, join: Join): Context = {
      Context(
        environment = environment,
        join = join.metaData.cleanName,
        production = join.metaData.isProduction,
        team = join.metaData.team
      )
    }

    def apply(environment: Environment, groupBy: GroupBy): Context = {
      Context(
        environment = environment,
        groupBy = groupBy.metaData.cleanName,
        production = groupBy.metaData.isProduction,
        accuracy = groupBy.inferredAccuracy,
        team = groupBy.metaData.team,
        join = groupBy.sources.toScala
          .find(_.isSetJoinSource)
          .map(_.getJoinSource.join.metaData.cleanName)
          .orNull
      )
    }

    def apply(joinContext: Context, joinPart: JoinPart): Context = {
      joinContext.copy(groupBy = joinPart.groupBy.metaData.cleanName,
                       accuracy = joinPart.groupBy.inferredAccuracy,
                       joinPartPrefix = joinPart.prefix)
    }

    def apply(environment: Environment, stagingQuery: StagingQuery): Context = {
      Context(
        environment = environment,
        groupBy = stagingQuery.metaData.cleanName,
        production = stagingQuery.metaData.isProduction,
        team = stagingQuery.metaData.team
      )
    }

    private val client: MetricsReporter = {
      // Metrics collection is turned off by default, we explicitly turn on in serving contexts
      val metricsEnabled: Boolean = System.getProperty(MetricsEnabled, "false").toBoolean
      val reporter: String = System.getProperty(MetricsReporter, "otel")
      logger.info(s"create metrics reporter with - enabled: $metricsEnabled, reporter: $reporter")

      reporter.toLowerCase match {
        case "otel" | "opentelemetry" =>
          if (metricsEnabled) {
            val metricReader = OtelMetricsReporter.buildOtelMetricReader()
            val openTelemetry = OtelMetricsReporter.buildOpenTelemetryClient(metricReader)
            new OtelMetricsReporter(openTelemetry)
          } else {
            new OtelMetricsReporter(OpenTelemetry.noop())
          }
        case _ =>
          throw new IllegalArgumentException(s"Unknown metrics reporter: $reporter. Only opentelemetry is supported.")
      }
    }
  }

  case class Context(environment: Environment,
                     join: String = null,
                     groupBy: String = null,
                     stagingQuery: String = null,
                     production: Boolean = false,
                     accuracy: Accuracy = null,
                     team: String = null,
                     joinPartPrefix: String = null,
                     suffix: String = null,
                     dataset: String = null,
                     model: String = null,
                     modelTransforms: String = null)
      extends Serializable {

    def withSuffix(suffixN: String): Context = copy(suffix = (Option(suffix) ++ Seq(suffixN)).mkString("."))

    private val prefixString =
      globalMetricsPrefix.map(_ + ".").getOrElse("") + environment + Option(suffix).map("." + _).getOrElse("")

    private def prefix(s: String): String =
      new java.lang.StringBuilder(prefixString.length + s.length + 1)
        .append(prefixString)
        .append('.')
        .append(s)
        .toString

    def toTags: Map[String, String] = {
      val joinNames: Array[String] = Option(join).map(_.split(",")).getOrElse(Array.empty[String]).map(_.sanitize)
      assert(
        environment != null,
        "Environment needs to be set - group_by.upload, group_by.streaming, join.fetching, group_by.fetching, group_by.offline etc")
      val buffer = mutable.Map[String, String]()

      def addTag(key: String, value: String): Unit = {
        if (value == null) return
        buffer += key -> value
      }

      joinNames.foreach(addTag(Tag.Join, _))

      val groupByName = Option(groupBy).map(_.sanitize)
      groupByName.foreach(addTag(Tag.GroupBy, _))

      addTag(Tag.StagingQuery, stagingQuery)
      addTag(Tag.Production, production.toString)
      addTag(Tag.Team, team)
      addTag(Tag.Environment, environment)
      addTag(Tag.JoinPartPrefix, joinPartPrefix)
      addTag(Tag.Accuracy, if (accuracy != null) accuracy.name() else null)
      addTag(Tag.Dataset, dataset)
      addTag(Tag.ModelTransforms, modelTransforms)
      addTag(Tag.Model, model)
      buffer.toMap
    }

    implicit val context: Context = this

    def increment(metric: String): Unit = Context.client.count(prefix(metric), 1, Map.empty)

    def increment(metric: String, additionalTags: Map[String, String]): Unit =
      Context.client.count(prefix(metric), 1, additionalTags)

    def incrementException(exception: Throwable)(implicit logger: org.slf4j.Logger): Unit = {
      val stackTrace = exception.getStackTrace
      val exceptionSignature = if (stackTrace.isEmpty) {
        exception.getClass.toString
      } else {
        val stackRoot = stackTrace.apply(0)
        val file = stackRoot.getFileName
        val line = stackRoot.getLineNumber
        val method = stackRoot.getMethodName
        s"[$method@$file:$line]${exception.getClass.toString}"
      }
      logger.error(s"Exception Message: ${exception.traceString}")
      Context.client.count(prefix(Name.Exception), 1, Map(Metrics.Name.Exception -> exceptionSignature))
    }

    def distribution(metric: String, value: Long): Unit =
      Context.client.distribution(prefix(metric), value, Map.empty)

    def distribution(metric: String, value: Long, additionalTags: Map[String, String]): Unit =
      Context.client.distribution(prefix(metric), value, additionalTags)

    def count(metric: String, value: Long): Unit = Context.client.count(prefix(metric), value)

    def gauge(metric: String, value: Long): Unit = Context.client.longGauge(prefix(metric), value)

    def gauge(metric: String, value: Double): Unit = Context.client.doubleGauge(prefix(metric), value)
    def gauge(metric: String, value: Double, additionalTags: Map[String, String]): Unit =
      Context.client.doubleGauge(prefix(metric), value, additionalTags)

    /** Block until any buffered metrics have been exported, or `timeoutMillis` elapses.
      * Use from short-lived batch jobs after recording terminal gauges, before the JVM exits.
      */
    def flush(timeoutMillis: Long): Unit = Context.client.flush(timeoutMillis)
  }
}
