package ai.chronon.online.metrics

import ai.chronon.online.metrics.Metrics.Environment
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest
import io.opentelemetry.proto.metrics.v1.NumberDataPoint
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.export.{MetricReader, PeriodicMetricReader}
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration

import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.zip.GZIPInputStream

/** Black-box test of the production OTel metrics export path.
  *
  * Stands up a real HTTP server bound to a free port, points the production wiring
  * (`buildOtelMetricReader` / `buildOpenTelemetryClient`) at it via the same system properties
  * that production reads, and decodes the incoming OTLP/HTTP protobuf request body. The test
  * verifies what we actually care about end-to-end:
  *
  *   1. Without flush, a `gauge.set()` does not hit the wire (the periodic reader is configured
  *      with a 1-hour interval so no tick can fire during the test). This reproduces the
  *      production race that PR #1774 is fixing — the JVM can exit before the next tick.
  *   2. After `flush(timeoutMillis)`, the receiver has the gauge with the expected value/name.
  *   3. `flush` does not throw when the receiver hangs past the timeout — losing a metric must
  *      never fail the underlying job.
  */
class OtelMetricsReporterFlushTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var server: HttpServer = _
  private var receivedRequests: ConcurrentLinkedQueue[ExportMetricsServiceRequest] = _
  private var unexpectedPaths: ConcurrentLinkedQueue[String] = _
  private var hangLatch: Option[CountDownLatch] = None
  private var savedProps: Map[String, Option[String]] = Map.empty

  private val PropKeys = Seq(
    OtelMetricsReporter.MetricsExporterUrlKey,
    OtelMetricsReporter.MetricsReader,
    OtelMetricsReporter.MetricsExporterIntervalKey
  )

  override def beforeEach(): Unit = {
    receivedRequests = new ConcurrentLinkedQueue[ExportMetricsServiceRequest]()
    unexpectedPaths = new ConcurrentLinkedQueue[String]()
    hangLatch = None
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    // Catch-all root context so we still observe (and log) requests to unexpected paths.
    server.createContext("/", new MetricsHandler)
    server.setExecutor(null)
    server.start()

    savedProps = PropKeys.map(k => k -> Option(System.getProperty(k))).toMap
    System.setProperty(OtelMetricsReporter.MetricsExporterUrlKey, s"http://127.0.0.1:${server.getAddress.getPort}")
    System.setProperty(OtelMetricsReporter.MetricsReader, OtelMetricsReporter.MetricsReaderHttp)
    // 1-hour interval → guarantees no periodic tick fires during the test, so any export we see
    // is unambiguously caused by forceFlush.
    System.setProperty(OtelMetricsReporter.MetricsExporterIntervalKey, "PT1H")
  }

  override def afterEach(): Unit = {
    hangLatch.foreach(_.countDown())
    if (server != null) server.stop(0)
    PropKeys.foreach { k =>
      savedProps.get(k).flatten match {
        case Some(v) => System.setProperty(k, v)
        case None    => System.clearProperty(k)
      }
    }
  }

  it should "not deliver gauges before flush when the periodic tick has not fired" in {
    val (reporter, _) = buildReporterAndContext()
    val ctx = Metrics.Context(Environment.GroupByUpload, groupBy = "test_gb", team = "team1")

    reporter.longGauge("NullCount.feature1.20260423", 7L)(ctx)

    // Generous wait — periodic reader is at 1h, so this should never see traffic.
    Thread.sleep(300)
    receivedRequests shouldBe empty
  }

  it should "deliver buffered gauges synchronously when flush is called" in {
    val (reporter, _) = buildReporterAndContext()
    val ctx = Metrics.Context(Environment.GroupByUpload, groupBy = "test_gb", team = "team1")

    reporter.longGauge("NullCount.feature1.20260423", 7L)(ctx)
    reporter.longGauge("NullCount.feature2.20260423", 0L)(ctx)

    reporter.flush(5000L)

    // forceFlush blocks until the OTLP POST completes, so by the time flush returns the
    // request has already been handled by our server.
    import scala.jdk.CollectionConverters._
    val gauges = collectGauges()
    withClue(
      s"received gauges: $gauges, raw requests: ${dumpRequests()}, unexpected paths: ${unexpectedPaths.asScala.mkString(",")}") {
      receivedRequests should not be empty
      gauges should contain key "NullCount.feature1.20260423"
      gauges should contain key "NullCount.feature2.20260423"
      gauges("NullCount.feature1.20260423") shouldBe 7L
      gauges("NullCount.feature2.20260423") shouldBe 0L
    }
  }

  it should "flush via the JVM shutdown hook" in {
    // Build the SdkMeterProvider directly (mirroring buildOpenTelemetryClient) so we own a
    // hook Thread we can run synchronously, instead of leaving an orphan auto-registered
    // hook in the test JVM.
    val provider = buildIsolatedMeterProvider()
    val otel: OpenTelemetry = OpenTelemetrySdk.builder.setMeterProvider(provider).build
    val reporter = new OtelMetricsReporter(otel)
    val ctx = Metrics.Context(Environment.GroupByUpload, groupBy = "test_gb", team = "team1")

    reporter.longGauge("NullCount.feature1.20260423", 7L)(ctx)
    receivedRequests shouldBe empty

    val hook = OtelMetricsReporter.shutdownFlushHook(provider, 5000L)
    hook.run() // simulate JVM exit firing the registered hook

    val gauges = collectGauges()
    gauges should contain key "NullCount.feature1.20260423"
    gauges("NullCount.feature1.20260423") shouldBe 7L
  }

  it should "return without throwing when flush times out" in {
    val latch = new CountDownLatch(1)
    hangLatch = Some(latch)
    val (reporter, _) = buildReporterAndContext()
    val ctx = Metrics.Context(Environment.GroupByUpload, groupBy = "test_gb", team = "team1")

    reporter.longGauge("NullCount.feature1.20260423", 7L)(ctx)

    val started = System.currentTimeMillis()
    noException should be thrownBy reporter.flush(150L)
    val elapsed = System.currentTimeMillis() - started

    // Should return close to the timeout, not block until the receiver is unblocked.
    elapsed should be < 2000L
    latch.countDown()
  }

  private def buildReporterAndContext(): (OtelMetricsReporter, Unit) = {
    val reader = OtelMetricsReporter.buildOtelMetricReader()
    val otel = OtelMetricsReporter.buildOpenTelemetryClient(reader)
    (new OtelMetricsReporter(otel), ())
  }

  private def buildIsolatedMeterProvider(): SdkMeterProvider = {
    val exporter = OtlpHttpMetricExporter.builder
      .setEndpoint(s"http://127.0.0.1:${server.getAddress.getPort}/v1/metrics")
      .build
    val reader: MetricReader =
      PeriodicMetricReader.builder(exporter).setInterval(Duration.ofHours(1)).build
    SdkMeterProvider.builder.registerMetricReader(reader).build
  }

  private def collectGauges(): Map[String, Long] = {
    import scala.jdk.CollectionConverters._
    val builder = Map.newBuilder[String, Long]
    receivedRequests.asScala.foreach { req =>
      req.getResourceMetricsList.asScala.foreach { rm =>
        rm.getScopeMetricsList.asScala.foreach { sm =>
          sm.getMetricsList.asScala.foreach { metric =>
            if (metric.hasGauge) {
              metric.getGauge.getDataPointsList.asScala.foreach { (dp: NumberDataPoint) =>
                val value: Long =
                  if (dp.hasAsInt) dp.getAsInt
                  else dp.getAsDouble.toLong
                builder += metric.getName -> value
              }
            }
          }
        }
      }
    }
    builder.result()
  }

  private def dumpRequests(): String = {
    import scala.jdk.CollectionConverters._
    receivedRequests.asScala
      .map { req =>
        req.getResourceMetricsList.asScala
          .flatMap(_.getScopeMetricsList.asScala)
          .flatMap(_.getMetricsList.asScala)
          .map(m => s"${m.getName}(hasGauge=${m.hasGauge}, dataCase=${m.getDataCase})")
          .mkString("[", ", ", "]")
      }
      .mkString("\n")
  }

  private class MetricsHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      try {
        val path = exchange.getRequestURI.getPath
        // The OTLP HTTP exporter may gzip; honor Content-Encoding to decode the protobuf.
        val raw = readAll(exchange.getRequestBody)
        val bytes = Option(exchange.getRequestHeaders.getFirst("Content-Encoding")) match {
          case Some(enc) if enc.equalsIgnoreCase("gzip") => gunzip(raw)
          case _                                         => raw
        }
        // Optional hang for the timeout test. Done before recording/responding so flush
        // observes an outstanding response when timing out.
        hangLatch.foreach(_.await(5, TimeUnit.SECONDS))

        if (path == "/v1/metrics") {
          val req = ExportMetricsServiceRequest.parseFrom(bytes)
          receivedRequests.add(req)
        } else {
          // Surface unexpected paths in the failure clue.
          unexpectedPaths.add(s"$path (${bytes.length} bytes)")
        }

        val resp = Array.emptyByteArray
        exchange.getResponseHeaders.set("Content-Type", "application/x-protobuf")
        exchange.sendResponseHeaders(200, resp.length.toLong)
        exchange.getResponseBody.write(resp)
      } finally {
        exchange.close()
      }
    }
  }

  private def readAll(in: java.io.InputStream): Array[Byte] = {
    val buf = new ByteArrayOutputStream()
    val chunk = new Array[Byte](4096)
    var n = in.read(chunk)
    while (n != -1) {
      buf.write(chunk, 0, n)
      n = in.read(chunk)
    }
    buf.toByteArray
  }

  private def gunzip(bytes: Array[Byte]): Array[Byte] = {
    val gz = new GZIPInputStream(new java.io.ByteArrayInputStream(bytes))
    try readAll(gz)
    finally gz.close()
  }
}
