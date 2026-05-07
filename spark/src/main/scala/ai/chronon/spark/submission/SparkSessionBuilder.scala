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

package ai.chronon.spark.submission

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.slf4j.LoggerFactory

import java.io.File
import java.util.logging.Logger
import scala.util.Properties

object SparkSessionBuilder {

  def configureLogging(): Unit = {

    // Force reconfiguration
    LoggerContext.getContext(false).close()

    val builder = ConfigurationBuilderFactory.newConfigurationBuilder()

    // Add status logger to debug logging setup
    // builder.setStatusLevel(Level.DEBUG)

    // Create console appender
    val console = builder
      .newAppender("console", "Console")
      .addAttribute("target", "SYSTEM_OUT")

    val patternLayout = builder
      .newLayout("PatternLayout")
      .addAttribute("pattern", "%F:%L: %m%n")

    console.add(patternLayout)
    builder.add(console)

    // Configure root logger
    val rootLogger = builder.newRootLogger(Level.ERROR)
    rootLogger.add(builder.newAppenderRef("console"))
    builder.add(rootLogger)

    // Configure specific logger for ai.chronon
    val chrononLogger = builder.newLogger("ai.chronon", Level.INFO)
    builder.add(chrononLogger)

    // Spark 3.5.3 RPC noise: org.apache.spark.storage.StorageStatus is not
    // Serializable, so BlockManagerMasterEndpoint replies to GetStorageStatus
    // hit a NotSerializableException over Netty. Inbox.safelyCall catches and
    // logs at ERROR ("Ignoring error", with full stack trace) on every RPC.
    // The job is unaffected. Upstream demoted this log to DEBUG in
    // SPARK-49749 (https://issues.apache.org/jira/browse/SPARK-49749), but
    // we're on 3.5.3 which predates the fix. Drop the Inbox logger until we
    // upgrade.
    val inboxLogger = builder.newLogger("org.apache.spark.rpc.netty.Inbox", Level.OFF)
    builder.add(inboxLogger)

    // Build and apply configuration
    val config = builder.build()
    val context = LoggerContext.getContext(false)
    context.start(config)

    // Add a test log message
    val logger = LogManager.getLogger(getClass)
    logger.info("Chronon logging system initialized. Overrides spark's configuration")

  }

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  private val warehouseId = java.util.UUID.randomUUID().toString.takeRight(6)
  private val DefaultWarehouseDir = new File("/tmp/chronon/spark-warehouse_" + warehouseId)

  def expandUser(path: String): String = path.replaceFirst("~", System.getProperty("user.home"))
  // we would want to share locally generated warehouse during CI testing
  def build(name: String,
            local: Boolean = false,
            localWarehouseLocation: Option[String] = None,
            additionalConfig: Option[Map[String, String]] = None,
            enforceKryoSerializer: Boolean = true): SparkSession = {

    val userName = Properties.userName
    val warehouseDir = localWarehouseLocation.map(expandUser).getOrElse(DefaultWarehouseDir.getAbsolutePath)
    println(s"Using warehouse dir: $warehouseDir")

    // Read existing Spark config (e.g. from spark-submit --conf) so we don't clobber user settings.
    val existingConf = new org.apache.spark.SparkConf()

    val baseConfigs = Map(
      "spark.sql.session.timeZone" -> existingConf.get("spark.sql.session.timeZone", "UTC"),
      // needs to be uppercase until https://github.com/GoogleCloudDataproc/spark-bigquery-connector/pull/1313 is available
      "spark.sql.sources.partitionOverwriteMode" -> "DYNAMIC",
      "hive.exec.dynamic.partition" -> "true",
      "hive.exec.dynamic.partition.mode" -> "nonstrict",
      "spark.hadoop.hive.exec.max.dynamic.partitions" -> "30000",
      "spark.sql.legacy.timeParserPolicy" -> "LEGACY",
      SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true",
      SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key -> "false"
    )

    // Staging queries don't benefit from the KryoSerializer and in fact may fail with buffer underflow in some cases.
    val kryoConfigs =
      if (enforceKryoSerializer)
        Map(
          "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
          "spark.kryo.registrator" -> "ai.chronon.spark.submission.ChrononKryoRegistrator",
          "spark.kryoserializer.buffer.max" -> "2000m",
          "spark.kryo.referenceTracking" -> "false"
        )
      else Map.empty[String, String]

    val localConfigs = if (local) {
      logger.info(s"Building local spark session with warehouse at $warehouseDir")
      val metastoreDb = s"jdbc:derby:;databaseName=$warehouseDir/metastore_db;create=true"
      Map(
        "spark.master" -> "local[*]",
        "spark.kryo.registrationRequired" -> s"${localWarehouseLocation.isEmpty}",
        "spark.local.dir" -> s"/tmp/$userName/${name}_$warehouseId",
        "spark.sql.warehouse.dir" -> s"$warehouseDir/data",
        "spark.hadoop.javax.jdo.option.ConnectionURL" -> metastoreDb,
        "spark.driver.host" -> "127.0.0.1",
        "spark.driver.bindAddress" -> "127.0.0.1",
        "spark.ui.enabled" -> "false",
        "spark.sql.catalogImplementation" -> "hive",
        // Defaults for ModularMonolith's increased resource requirements; overridable via additionalConfig
        "spark.driver.memory" -> "4g",
        "spark.executor.memory" -> "4g",
        "spark.default.parallelism" -> "2",
        "spark.sql.shuffle.partitions" -> "4",
        "spark.network.timeout" -> "600s",
        "spark.executor.heartbeatInterval" -> "60s",
        "spark.storage.memoryFraction" -> "0.5",
        "spark.shuffle.memoryFraction" -> "0.3"
      )
    } else Map.empty[String, String]

    // additionalConfig applied last so callers can override any default
    val allConfigs = baseConfigs ++ kryoConfigs ++ localConfigs ++ additionalConfig.getOrElse(Map.empty)

    val baseBuilder = SparkSession.builder().appName(name).enableHiveSupport()

    val builder = allConfigs.foldLeft(baseBuilder) { case (b, (k, v)) => b.config(k, v) }

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    Logger.getLogger("parquet.hadoop").setLevel(java.util.logging.Level.SEVERE)
    configureLogging()
    spark
  }

  def buildStreaming(local: Boolean): SparkSession = {
    val userName = Properties.userName
    val baseBuilder = SparkSession
      .builder()
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "ai.chronon.spark.submission.ChrononKryoRegistrator")
      .config("spark.kryoserializer.buffer.max", "2000m")
      .config("spark.kryo.referenceTracking", "false")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")

    val builder = if (local) {
      baseBuilder
        // use all threads - or the tests will be slow
        .master("local[*]")
        .config("spark.local.dir", s"/tmp/$userName/chronon-spark-streaming")
        .config("spark.kryo.registrationRequired", "true")
    } else {
      baseBuilder
    }
    val spark = builder.getOrCreate()
    // disable log spam
    spark.sparkContext.setLogLevel("ERROR")
    Logger.getLogger("parquet.hadoop").setLevel(java.util.logging.Level.SEVERE)
    spark
  }
}
