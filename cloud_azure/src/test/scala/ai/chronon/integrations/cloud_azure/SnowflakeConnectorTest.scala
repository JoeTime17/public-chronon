package ai.chronon.integrations.cloud_azure

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

class SnowflakeConnectorTest extends AnyFlatSpec with Matchers {

  private val sampleJdbcUrl =
    "jdbc:snowflake://account.snowflakecomputing.com/?user=alice&db=MY_DB&schema=PUBLIC&warehouse=WH"

  // --- parseJdbcParams ---

  it should "parse all query params from a JDBC URL" in {
    val params = SnowflakeConnector.parseJdbcParams(sampleJdbcUrl)
    params shouldBe Map("user" -> "alice", "db" -> "MY_DB", "schema" -> "PUBLIC", "warehouse" -> "WH")
  }

  it should "return empty map when JDBC URL has no query string" in {
    SnowflakeConnector.parseJdbcParams("jdbc:snowflake://account.snowflakecomputing.com/") shouldBe empty
  }

  it should "handle params with '=' in the value" in {
    val url = "jdbc:snowflake://host/?user=alice&token=abc=def="
    val params = SnowflakeConnector.parseJdbcParams(url)
    params("token") shouldBe "abc=def="
  }

  // --- extractPemBase64 ---

  it should "strip PEM headers, footers, and whitespace" in {
    val pem =
      """-----BEGIN PRIVATE KEY-----
        |MIIEvQIBADANBg
        |kqhkiG9w0BAQEF
        |-----END PRIVATE KEY-----""".stripMargin
    SnowflakeConnector.extractPemBase64(pem) shouldBe "MIIEvQIBADANBgkqhkiG9w0BAQEF"
  }

  it should "handle PEM content that is already raw base64" in {
    SnowflakeConnector.extractPemBase64("AABBCC") shouldBe "AABBCC"
  }

  // --- buildSparkConnectorOptions ---

  it should "build correct Spark connector options" in {
    val pem =
      """-----BEGIN PRIVATE KEY-----
        |AABB
        |-----END PRIVATE KEY-----""".stripMargin
    val opts = SnowflakeConnector.buildSparkConnectorOptions(sampleJdbcUrl, pem)

    opts("sfURL") shouldBe "account.snowflakecomputing.com/"
    opts("pem_private_key") shouldBe "AABB"
    opts("sfUser") shouldBe "alice"
    opts("sfDatabase") shouldBe "MY_DB"
    opts("sfSchema") shouldBe "PUBLIC"
    opts("sfWarehouse") shouldBe "WH"
    opts("tracing") shouldBe "all"
    // Driver log redirect — CLIENT_CONFIG_FILE points at a writable tmpdir path and
    // the same path is exposed via SF_CLIENT_CONFIG_FILE as a system property for
    // driver versions that prefer that lookup.
    val cfgPath = opts("CLIENT_CONFIG_FILE")
    cfgPath should include(System.getProperty("java.io.tmpdir"))
    System.getProperty("SF_CLIENT_CONFIG_FILE") shouldBe cfgPath
  }

  it should "throw when a required JDBC param is missing" in {
    val urlMissingWarehouse = "jdbc:snowflake://host/?user=alice&db=D&schema=S"
    val ex = the[IllegalStateException] thrownBy {
      SnowflakeConnector.buildSparkConnectorOptions(urlMissingWarehouse, "PEM")
    }
    ex.getMessage should include("Warehouse")
  }

  // --- getPrivateKeyPem ---

  it should "return SNOWFLAKE_PRIVATE_KEY when present" in {
    val env = Map("SNOWFLAKE_PRIVATE_KEY" -> "my-key")
    SnowflakeConnector.getPrivateKeyPem(env) shouldBe "my-key"
  }

  it should "throw when neither key nor vault URI is present" in {
    val ex = the[IllegalStateException] thrownBy {
      SnowflakeConnector.getPrivateKeyPem(Map.empty)
    }
    ex.getMessage should include("SNOWFLAKE_PRIVATE_KEY")
    ex.getMessage should include("SNOWFLAKE_VAULT_URI")
  }

  // --- validateJdbcUrl ---

  it should "pass through a valid JDBC URL" in {
    SnowflakeConnector.validateJdbcUrl(sampleJdbcUrl) shouldBe sampleJdbcUrl
  }

  it should "reject a URL without the jdbc:snowflake:// prefix" in {
    val ex = the[IllegalStateException] thrownBy {
      SnowflakeConnector.validateJdbcUrl("https://account.snowflakecomputing.com/")
    }
    ex.getMessage should include("jdbc:snowflake://")
  }

  // --- SF_CLIENT_CONFIG_FILE log redirect ---

  private def invokeOptions(): Map[String, String] =
    SnowflakeConnector.buildSparkConnectorOptions(sampleJdbcUrl, "-----BEGIN KEY-----\nAA\n-----END KEY-----")

  "SF client config redirect" should "include CLIENT_CONFIG_FILE in connector options" in {
    val opts = invokeOptions()
    opts.keys should contain("CLIENT_CONFIG_FILE")
  }

  it should "write a unique sf_client_config-*.json file under java.io.tmpdir" in {
    val opts = invokeOptions()
    val cfgPath = Paths.get(opts("CLIENT_CONFIG_FILE"))
    // Normalize — java.io.tmpdir can carry a trailing slash that
    // Paths.get(...).toString strips.
    val tmpdir = Paths.get(System.getProperty("java.io.tmpdir")).toString
    cfgPath.toString should startWith(tmpdir)
    // Per-process unique filename: sf_client_config-<random>.json
    cfgPath.getFileName.toString should startWith("sf_client_config-")
    cfgPath.getFileName.toString should endWith(".json")
    Files.exists(cfgPath) shouldBe true
  }

  it should "write valid JSON body pointing log_path at a writable dir" in {
    val opts = invokeOptions()
    val cfgPath = Paths.get(opts("CLIENT_CONFIG_FILE"))
    val body = new String(Files.readAllBytes(cfgPath), StandardCharsets.UTF_8)
    // Minimal schema check — we don't need a full JSON parser to assert the
    // driver-facing contract (log_level + log_path under "common").
    body should include("\"common\"")
    body should include("\"log_level\":\"INFO\"")
    body should include("\"log_path\":")
    // log_path must be a directory the driver can actually write into;
    // java.io.tmpdir is writable for any user, hubuser/spark included.
    // Normalize because java.io.tmpdir can carry a trailing slash that
    // Paths.get(...).toString strips.
    body should include(Paths.get(System.getProperty("java.io.tmpdir")).toString)
  }

  it should "expose the same path via SF_CLIENT_CONFIG_FILE system property" in {
    val opts = invokeOptions()
    // Both lookup paths are wired so the fix works across driver versions:
    // older drivers honor the env-var-like system property, newer ones honor
    // the per-connection CLIENT_CONFIG_FILE.
    System.getProperty("SF_CLIENT_CONFIG_FILE") shouldBe opts("CLIENT_CONFIG_FILE")
  }

  it should "be idempotent across repeated calls — same path, file stays consistent" in {
    val path1 = invokeOptions()("CLIENT_CONFIG_FILE")
    val body1 = new String(Files.readAllBytes(Paths.get(path1)), StandardCharsets.UTF_8)
    val path2 = invokeOptions()("CLIENT_CONFIG_FILE")
    val body2 = new String(Files.readAllBytes(Paths.get(path2)), StandardCharsets.UTF_8)
    path2 shouldBe path1
    body2 shouldBe body1
  }

  // --- Snowflake.buildPartitionQuery ---

  it should "generate a partition query with a WHERE clause when filters are provided" in {
    val query = Snowflake.buildPartitionQuery("MY_DB.PUBLIC.EVENTS", "DS", "DS >= '2023-01-01'", "yyyy-MM-dd")
    query shouldBe "SELECT DISTINCT TO_VARCHAR(DS::DATE, 'YYYY-MM-DD') AS DS FROM MY_DB.PUBLIC.EVENTS WHERE DS >= '2023-01-01'"
  }

  it should "generate a partition query without a WHERE clause when no filters are provided" in {
    val query = Snowflake.buildPartitionQuery("MY_DB.PUBLIC.EVENTS", "DS", "", "yyyy-MM-dd")
    query shouldBe "SELECT DISTINCT TO_VARCHAR(DS::DATE, 'YYYY-MM-DD') AS DS FROM MY_DB.PUBLIC.EVENTS"
  }

  it should "convert Java date format patterns to Snowflake format patterns" in {
    val query = Snowflake.buildPartitionQuery("DB.SCHEMA.T", "EVENT_DATE", "", "yyyy/MM/dd")
    query should include("TO_VARCHAR(EVENT_DATE::DATE, 'YYYY/MM/DD')")
  }
}
