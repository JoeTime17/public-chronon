package ai.chronon.service;

import ai.chronon.online.metrics.Metrics;
import ai.chronon.online.metrics.OtelMetricsReporter;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.vertx.core.Launcher;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.micrometer.Label;
import io.vertx.micrometer.MicrometerMetricsFactory;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Function;

/**
 * Custom launcher to help configure the Chronon vertx feature service
 * to handle things like setting up a otel metrics registry.
 * We use otel here to be consistent with the rest of our project (e.g. fetcher code).
 * This allows us to send Vertx webservice metrics along with fetcher related metrics to allow users
 * to debug performance issues and set alerts etc.
 */
public class ChrononServiceLauncher extends Launcher {

    private static final String VertxPrometheusPort = "ai.chronon.vertx.metrics.exporter.port";
    private static final String DefaultVertxPrometheusPort = "8906";

    private static final Logger logger = LoggerFactory.getLogger(ChrononServiceLauncher.class);

    @Override
    public void beforeStartingVertx(VertxOptions options) {
        boolean enableMetrics = Optional.ofNullable(System.getProperty(Metrics.MetricsEnabled()))
                .map(Boolean::parseBoolean)
                .orElse(false);

        if (enableMetrics) {
            initializeMetrics(options);
        }
    }

    private void initializeMetrics(VertxOptions options) {
        String serviceName = "ai.chronon";
        String configuredMetricsReader = OtelMetricsReporter.getMetricsReader();
        if (configuredMetricsReader.equalsIgnoreCase(OtelMetricsReporter.MetricsReaderHttp())) {
            configureOtlpHttpMetrics(serviceName, options);
        } else if (configuredMetricsReader.equalsIgnoreCase(OtelMetricsReporter.MetricsReaderPrometheus())) {
            // if prometheus is configured, we need to spin up a Prometheus server configured to vend out
            // Vert.x metrics when it is hit with scrape calls
            configurePrometheusMetrics(options);
        } else {
            logger.warn("Unrecognized configured metrics reader: {}. Skipping Vert.x metrics collection", configuredMetricsReader);
        }
    }

    private void configureOtlpHttpMetrics(String serviceName, VertxOptions options) {
        String exporterUrl = OtelMetricsReporter.getExporterUrl() + "/v1/metrics";
        String exportInterval = OtelMetricsReporter.getMetricsExporterInterval();
        String resourceAttributes = buildOtlpResourceAttributes(serviceName);

        // Configure OTLP using Micrometer's built-in registry
        OtlpConfig otlpConfig = key -> {
            switch (key) {
                case "otlp.url":
                    return exporterUrl;
                case "otlp.step":
                    return exportInterval;
                case "otlp.resourceAttributes":
                    return resourceAttributes;
                // Emit exponential histograms so backends can compute server-side percentiles
                case "otlp.histogramFlavor":
                    return "BASE2_EXPONENTIAL";
                default:
                    return null;
            }
        };

        OtlpMeterRegistry registry = new OtlpMeterRegistry(otlpConfig, Clock.SYSTEM);
        // histogramFlavor only takes effect when publishPercentileHistogram is enabled on the meter
        registry.config().meterFilter(new MeterFilter() {
            @Override
            public DistributionStatisticConfig configure(io.micrometer.core.instrument.Meter.Id id,
                                                        DistributionStatisticConfig config) {
                return DistributionStatisticConfig.builder()
                        .percentilesHistogram(true)
                        .build()
                        .merge(config);
            }
        });
        MicrometerMetricsFactory metricsFactory = new MicrometerMetricsFactory((MeterRegistry) registry);

        MicrometerMetricsOptions metricsOptions = new MicrometerMetricsOptions()
                .setEnabled(true)
                .setJvmMetricsEnabled(true)
                .setFactory(metricsFactory)
                .addLabels(Label.HTTP_METHOD, Label.HTTP_CODE, Label.HTTP_PATH);

        options.setMetricsOptions(metricsOptions);
    }

    private void configurePrometheusMetrics(VertxOptions options) {
        // Configure Prometheus using Micrometer's built-in registry
        String prometheusPort = System.getProperty(VertxPrometheusPort, DefaultVertxPrometheusPort);
        VertxPrometheusOptions promOptions =
                new VertxPrometheusOptions()
                        .setEnabled(true)
                        .setStartEmbeddedServer(true)
                        .setPublishQuantiles(true)
                        .setEmbeddedServerOptions(new HttpServerOptions().setPort(Integer.parseInt(prometheusPort)));

        MicrometerMetricsFactory metricsFactory = new MicrometerMetricsFactory(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));
        MicrometerMetricsOptions metricsOptions = new MicrometerMetricsOptions()
                .setEnabled(true)
                .setJvmMetricsEnabled(true)
                .setPrometheusOptions(promOptions)
                .setFactory(metricsFactory)
                .addLabels(Label.HTTP_METHOD, Label.HTTP_CODE, Label.HTTP_PATH);

        options.setMetricsOptions(metricsOptions);
    }

    /**
     * Build the comma-separated resource attributes string for the Vert.x/Micrometer OTLP registry.
     *
     * Unlike the OTel SDK, Micrometer's OtlpMeterRegistry does not auto-read OTEL_SERVICE_NAME or
     * OTEL_RESOURCE_ATTRIBUTES, so we mirror that behavior here. We also honor the Chronon-specific
     * system property used by OtelMetricsReporter so both metrics pipelines accept the same config.
     *
     * Precedence (later entries override earlier ones, since Micrometer parses into a LinkedHashMap
     * where the last value for a given key wins):
     *   1. service.name=&lt;default&gt;
     *   2. OTEL_RESOURCE_ATTRIBUTES env var
     *   3. ai.chronon.metrics.exporter.resources system property
     *   4. OTEL_SERVICE_NAME env var (overrides service.name)
     */
    static String buildOtlpResourceAttributes(String defaultServiceName) {
        return buildOtlpResourceAttributes(defaultServiceName, System::getenv);
    }

    // Overload that takes an env lookup function for testability — System.getenv is immutable in-process.
    static String buildOtlpResourceAttributes(String defaultServiceName, Function<String, String> envLookup) {
        StringBuilder sb = new StringBuilder("service.name=").append(defaultServiceName);
        String envResourceAttrs = envLookup.apply("OTEL_RESOURCE_ATTRIBUTES");
        if (envResourceAttrs != null && !envResourceAttrs.trim().isEmpty()) {
            sb.append(',').append(envResourceAttrs.trim());
        }
        String chrononResourceAttrs = System.getProperty(OtelMetricsReporter.MetricsExporterResourceKey(), "");
        if (!chrononResourceAttrs.trim().isEmpty()) {
            sb.append(',').append(chrononResourceAttrs.trim());
        }
        String envServiceName = envLookup.apply("OTEL_SERVICE_NAME");
        if (envServiceName != null && !envServiceName.trim().isEmpty()) {
            sb.append(',').append("service.name=").append(envServiceName.trim());
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        new ChrononServiceLauncher().dispatch(args);
    }
}
