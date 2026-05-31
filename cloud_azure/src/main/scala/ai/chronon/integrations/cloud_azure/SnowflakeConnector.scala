package ai.chronon.integrations.cloud_azure

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.nio.charset.StandardCharsets

/** Shared Snowflake connection utilities used by both [[Snowflake]] (Format reader) and
  * [[SnowflakeImport]] (staging query). Consolidates JDBC URL parsing, PEM extraction,
  * Spark connector options building, and private key lookup.
  */
object SnowflakeConnector {

  /** Snowflake JDBC with `tracing=all` writes verbose driver logs to a file whose default
    * location is `$HOME/snowflake_jdbc*.log`. Process owners without a writable HOME (e.g.
    * `hubuser` created via `useradd -r`, or the `spark` user in driver pods) hit an
    * unrecoverable lockfile failure during driver init — which takes down Snowflake
    * schema fetches and Staging Query imports.
    *
    * Redirect the driver's log output to `java.io.tmpdir` via an SF client config file that
    * we write lazily at first use. Both the ``SF_CLIENT_CONFIG_FILE`` env-var equivalent
    * (set as a system property — the driver reads it at JDBC-init time) and the per-connection
    * ``CLIENT_CONFIG_FILE`` Spark-connector option are set, so the redirect takes effect
    * regardless of which lookup path the driver version in use prefers.
    */
  private lazy val sfClientConfigFile: String = {
    val logDir = Paths.get(System.getProperty("java.io.tmpdir"))
    Files.createDirectories(logDir)
    // Per-process unique filename — avoids a TOCTOU race when multiple JVMs
    // on the same host share java.io.tmpdir. The body is deterministic (same
    // tmpdir produces the same JSON) so a collision wouldn't corrupt data,
    // but createTempFile makes the guarantee unconditional.
    val cfg: Path = Files.createTempFile(logDir, "sf_client_config-", ".json")
    val body =
      s"""{"common":{"log_level":"INFO","log_path":"${logDir.toString.replace("\\", "\\\\")}"}}"""
    // createTempFile produces an empty file, so this unconditionally writes on
    // first init. Keep the compare-and-write shape for resilience if the same
    // Path is somehow reused across unexpected code paths.
    if (
      !Files.exists(cfg) || !java.util.Arrays.equals(Files.readAllBytes(cfg), body.getBytes(StandardCharsets.UTF_8))
    ) {
      Files.write(cfg, body.getBytes(StandardCharsets.UTF_8))
    }
    System.setProperty("SF_CLIENT_CONFIG_FILE", cfg.toString)
    cfg.toString
  }

  def parseJdbcParams(jdbcUrl: String): Map[String, String] = {
    val queryString = new URI(jdbcUrl.replace("jdbc:snowflake://", "http://")).getQuery
    if (queryString != null && queryString.nonEmpty) {
      queryString.split("&").map(_.split("=", 2)).collect { case Array(k, v) => k -> v }.toMap
    } else {
      Map.empty[String, String]
    }
  }

  def extractPemBase64(pemContent: String): String =
    pemContent
      .replaceFirst("-----BEGIN[^-]*-----", "")
      .replaceFirst("-----END[^-]*-----", "")
      .replaceAll("\\s", "")

  def buildSparkConnectorOptions(jdbcUrl: String, pemContent: String): Map[String, String] = {
    val params = parseJdbcParams(jdbcUrl)
    val pemBase64 = extractPemBase64(pemContent)
    def require(key: String, label: String): String =
      params.getOrElse(key, throw new IllegalStateException(s"$label missing in SNOWFLAKE_JDBC_URL"))
    Map(
      "sfURL" -> jdbcUrl.split("\\?").head.replace("jdbc:snowflake://", ""),
      "pem_private_key" -> pemBase64,
      "sfUser" -> require("user", "User"),
      "sfDatabase" -> require("db", "Database"),
      "sfSchema" -> require("schema", "Schema"),
      "sfWarehouse" -> require("warehouse", "Warehouse"),
      "treat_decimal_as_long" -> "true",
      "tracing" -> "all",
      "CLIENT_CONFIG_FILE" -> sfClientConfigFile
    )
  }

  /** Read via Spark Snowflake connector and lowercase all column names. */
  def read(sparkSession: SparkSession, options: Map[String, String], query: String): DataFrame = {
    val df = sparkSession.read
      .format("net.snowflake.spark.snowflake")
      .options(options)
      .option("query", query)
      .load()
    df.toDF(df.columns.map(_.toLowerCase): _*)
  }

  /** Tiered private key lookup: env var -> Azure Key Vault -> throw. */
  def getPrivateKeyPem(env: Map[String, String]): String = {
    env.get("SNOWFLAKE_PRIVATE_KEY") match {
      case Some(key) => key
      case None =>
        env
          .get("SNOWFLAKE_PRIVATE_KEY_VAULT_URI")
          .filter(_.trim.nonEmpty)
          .orElse(env.get("SNOWFLAKE_VAULT_URI").filter(_.trim.nonEmpty)) match {
          case Some(vaultUri) =>
            val (vaultUrl, secretName) = AzureKeyVaultHelper.parseSecretUri(vaultUri)
            AzureKeyVaultHelper.getSecret(vaultUrl, secretName)
          case None =>
            throw new IllegalStateException(
              "Snowflake private key not found. Provide SNOWFLAKE_PRIVATE_KEY, " +
                "SNOWFLAKE_PRIVATE_KEY_VAULT_URI, or SNOWFLAKE_VAULT_URI.")
        }
    }
  }

  def validateJdbcUrl(jdbcUrl: String): String = {
    if (!jdbcUrl.startsWith("jdbc:snowflake://"))
      throw new IllegalStateException(s"SNOWFLAKE_JDBC_URL must start with 'jdbc:snowflake://'. Got: $jdbcUrl")
    jdbcUrl
  }
}
