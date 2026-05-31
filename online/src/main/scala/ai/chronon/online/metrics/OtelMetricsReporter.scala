package ai.chronon.online.metrics

import ai.chronon.online.metrics.Metrics.Context
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.api.metrics.{DoubleGauge, LongCounter, LongGauge, LongHistogram, Meter}
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.export.{MetricReader, PeriodicMetricReader}
import io.opentelemetry.sdk.resources.Resource

import java.time.Duration
import scala.collection.concurrent.TrieMap

class OtelMetricsReporter(openTelemetry: OpenTelemetry) extends MetricsReporter {

  private val meter: Meter = openTelemetry.getMeterProvider
    .meterBuilder("ai.chronon")
    .setInstrumentationVersion("0.0.0")
    .build()

  // SdkMeterProvider supports forceFlush(); the no-op provider used when metrics are disabled
  // does not. We can't pattern-match on getMeterProvider() because OpenTelemetrySdk hands back
  // an ObfuscatedMeterProvider wrapper that doesn't expose the underlying SdkMeterProvider —
  // unwrap via OpenTelemetrySdk.getSdkMeterProvider() instead.
  private val sdkMeterProvider: Option[SdkMeterProvider] = openTelemetry match {
    case sdk: OpenTelemetrySdk => Some(sdk.getSdkMeterProvider)
    case _                     => None
  }

  val tagCache: TTLCache[Context, Attributes] = new TTLCache[Context, Attributes](
    { ctx =>
      val tagMap = ctx.toTags
      buildAttributes(tagMap)
    },
    { ctx => ctx },
    ttlMillis = 5 * 24 * 60 * 60 * 1000 // 5 days
  )

  private val counters = new TrieMap[String, LongCounter]()
  private val longGauges = new TrieMap[String, LongGauge]()
  private val doubleGauges = new TrieMap[String, DoubleGauge]()
  private val histograms = new TrieMap[String, LongHistogram]()

  private def buildAttributes(tags: Map[String, String]): Attributes = {
    val builder = Attributes.builder()
    tags.foreach { case (k, v) => builder.put(k, v) }
    builder.build()
  }

  private def mergeAttributes(attributes: Attributes, tags: Map[String, String]): Attributes = {
    val builder = attributes.toBuilder
    tags.foreach { case (k, v) => builder.put(k, v) }
    builder.build()
  }

  override def count(metric: String, value: Long, tags: Map[String, String] = Map.empty)(implicit
      context: Context): Unit = {
    val counter = counters.getOrElseUpdate(metric, meter.counterBuilder(metric).build())
    val mergedAttributes = mergeAttributes(tagCache(context), tags)
    counter.add(value, mergedAttributes)
  }

  override def longGauge(metric: String, value: Long, tags: Map[String, String] = Map.empty)(implicit
      context: Context): Unit = {
    val gauge = longGauges.getOrElseUpdate(metric, meter.gaugeBuilder(metric).ofLongs().build())
    val mergedAttributes = mergeAttributes(tagCache(context), tags)
    gauge.set(value, mergedAttributes)
  }

  override def doubleGauge(metric: String, value: Double, tags: Map[String, String] = Map.empty)(implicit
      context: Context): Unit = {
    val gauge = doubleGauges.getOrElseUpdate(metric, meter.gaugeBuilder(metric).build())
    val mergedAttributes = mergeAttributes(tagCache(context), tags)
    gauge.set(value, mergedAttributes)
  }

  override def distribution(metric: String, value: Long, tags: Map[String, String] = Map.empty)(implicit
      context: Context): Unit = {
    val histogram = histograms.getOrElseUpdate(metric, meter.histogramBuilder(metric).ofLongs().build())
    val mergedAttributes = mergeAttributes(tagCache(context), tags)
    histogram.record(value, mergedAttributes)
  }

  override def flush(timeoutMillis: Long): Unit = sdkMeterProvider.foreach { provider =>
    // forceFlush returns a CompletableResultCode; join() blocks up to timeoutMillis. If the
    // export fails or times out we log and move on rather than throwing — losing a metric
    // batch must not fail the underlying job.
    val result = provider.forceFlush().join(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
    if (!result.isSuccess) {
      OtelMetricsReporter.logger.warn(s"OTel metrics forceFlush did not complete within ${timeoutMillis}ms")
    }
  }
}

object OtelMetricsReporter {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[OtelMetricsReporter])

  val MetricsReader = "ai.chronon.metrics.reader"
  val MetricsExporterUrlKey = "ai.chronon.metrics.exporter.url"
  val MetricsExporterPrometheusPortKey = "ai.chronon.metrics.exporter.port"
  val MetricsExporterResourceKey = "ai.chronon.metrics.exporter.resources"
  val MetricsExporterIntervalKey = "ai.chronon.metrics.exporter.interval"

  val MetricsReaderGrpc = "grpc"
  val MetricsReaderHttp = "http"
  val MetricsReaderPrometheus = "prometheus"
  val MetricsExporterInterval = "PT15s"
  val MetricsExporterUrlDefault = "http://localhost:4318"
  val MetricsExporterPrometheusPortDefault = "8905"

  def getExporterUrl: String = {
    System.getProperty(MetricsExporterUrlKey, MetricsExporterUrlDefault)
  }

  def getMetricsReader: String = {
    System.getProperty(MetricsReader, MetricsReaderHttp)
  }

  def getMetricsExporterInterval: String = {
    System.getProperty(MetricsExporterIntervalKey, MetricsExporterInterval)
  }

  def buildOtelMetricReader(): MetricReader = {
    val metricReader = getMetricsReader
    metricReader.toLowerCase match {
      case MetricsReaderHttp =>
        val exporterUrl = getExporterUrl + "/v1/metrics"
        val metricExporter = OtlpHttpMetricExporter.builder.setEndpoint(exporterUrl).build
        // Configure periodic metric reader// Configure periodic metric reader
        PeriodicMetricReader.builder(metricExporter).setInterval(Duration.parse(getMetricsExporterInterval)).build

      case MetricsReaderGrpc =>
        val metricExporter = OtlpGrpcMetricExporter.builder().setEndpoint(getExporterUrl).build
        PeriodicMetricReader.builder(metricExporter).setInterval(Duration.parse(getMetricsExporterInterval)).build

      case MetricsReaderPrometheus =>
        val prometheusPort =
          System.getProperty(MetricsExporterPrometheusPortKey, MetricsExporterPrometheusPortDefault).toInt
        PrometheusHttpServer.builder
          .setPort(prometheusPort)
          .build
      case _ =>
        throw new IllegalArgumentException(s"Unknown metrics reader: $metricReader")
    }
  }

  def buildOpenTelemetryClient(metricReader: MetricReader): OpenTelemetry = {
    // Create resource with service information
    val configuredResourceKVPairs = System
      .getProperty(MetricsExporterResourceKey, "")
      .split(",")
      .map(_.split("="))
      .filter(_.length == 2)
      .map { case Array(k, v) => k.trim -> v.trim }
      .toMap

    val builder = Attributes.builder()
    configuredResourceKVPairs.map { case (k, v) =>
      val key = AttributeKey.stringKey(k)
      builder.put(key, v)
    }

    val resource = Resource.getDefault.merge(Resource.create(builder.build()))

    val meterProvider = SdkMeterProvider.builder
      .setResource(resource)
      .registerMetricReader(metricReader)
      .build

    // Belt-and-suspenders: even with explicit Context.flush() at known exit points, any new
    // caller that records gauges right before main() returns would re-introduce the periodic-
    // reader race. The shutdown hook is best-effort and only adds latency proportional to the
    // outstanding metrics — long-running services that have already drained will see it return
    // immediately.
    Runtime.getRuntime.addShutdownHook(shutdownFlushHook(meterProvider, ShutdownFlushTimeoutMillis))

    OpenTelemetrySdk.builder
      .setMeterProvider(meterProvider)
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance))
      .build
  }

  private[metrics] def shutdownFlushHook(provider: SdkMeterProvider, timeoutMillis: Long): Thread = {
    val t = new Thread(new Runnable {
      override def run(): Unit = {
        try {
          provider.forceFlush().join(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
          provider.shutdown().join(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
        } catch {
          // The JVM is exiting; do not surface anything that could mask the actual exit cause.
          case _: Throwable => ()
        }
      }
    })
    t.setName("otel-metrics-flush-shutdown")
    t
  }

  // Caps how long the shutdown hook will block on outstanding OTLP exports. Picked to match
  // ScrapeWaitSeconds so behavior aligns with the explicit flush at GroupByUpload exit.
  private val ShutdownFlushTimeoutMillis: Long = 15000L
}
