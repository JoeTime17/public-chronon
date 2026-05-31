package ai.chronon.online.metrics

import ai.chronon.online.metrics.Metrics.Context

/** Generic interface for reporting metrics. Specific implementations of this cater to different metrics systems
  * (e.g., StatsD, OpenTelemetry).
  */
trait MetricsReporter extends Serializable {

  def count(metric: String, value: Long, tags: Map[String, String] = Map.empty)(implicit context: Context): Unit

  def longGauge(metric: String, value: Long, tags: Map[String, String] = Map.empty)(implicit context: Context): Unit

  def doubleGauge(metric: String, value: Double, tags: Map[String, String] = Map.empty)(implicit context: Context): Unit

  def distribution(metric: String, value: Long, tags: Map[String, String] = Map.empty)(implicit context: Context): Unit

  /** Block until any buffered metrics have been exported, or `timeoutMillis` elapses.
    *
    * Required for short-lived batch jobs (e.g. GroupByUpload) where the JVM exits before the
    * periodic exporter's next tick — without this, the last batch of gauges silently drops.
    * Implementations that buffer in-process (OTel `PeriodicMetricReader`) must flush; pull-based
    * or fire-and-forget reporters can no-op.
    */
  def flush(timeoutMillis: Long): Unit = ()
}
