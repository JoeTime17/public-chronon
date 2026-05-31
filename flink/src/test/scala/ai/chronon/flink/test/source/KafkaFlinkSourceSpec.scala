package ai.chronon.flink.test.source

import ai.chronon.flink.source.{FlinkSourceProvider, KafkaFlinkSource}
import ai.chronon.online.TopicInfo
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class KafkaFlinkSourceSpec extends AnyFlatSpec {

  it should "fail on missing kafka bootstrap servers" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)
    assertThrows[Exception] {
      FlinkSourceProvider.build(Map.empty, null, topicInfo)
    }
  }

  it should "read kafka bootstrap server from properties" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)
    val flinkSrc = FlinkSourceProvider.build(Map(KafkaFlinkSource.KafkaBootstrap -> "my-kafka-server"), null, topicInfo)

    flinkSrc.isInstanceOf[KafkaFlinkSource[_]] shouldBe true
    val kafkaSrc = flinkSrc.asInstanceOf[KafkaFlinkSource[_]]
    kafkaSrc.bootstrap shouldBe "my-kafka-server"
  }

  it should "read kafka bootstrap server from topic info" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map(KafkaFlinkSource.KafkaBootstrap -> "my-kafka-server"))
    val flinkSrc = FlinkSourceProvider.build(Map.empty, null, topicInfo)

    flinkSrc.isInstanceOf[KafkaFlinkSource[_]] shouldBe true
    val kafkaSrc = flinkSrc.asInstanceOf[KafkaFlinkSource[_]]
    kafkaSrc.bootstrap shouldBe "my-kafka-server"
  }

  it should "read kafka bootstrap server from host and port" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map("host" -> "my-host", "port" -> "9092"))
    val flinkSrc = FlinkSourceProvider.build(Map.empty, null, topicInfo)

    flinkSrc.isInstanceOf[KafkaFlinkSource[_]] shouldBe true
    val kafkaSrc = flinkSrc.asInstanceOf[KafkaFlinkSource[_]]
    kafkaSrc.bootstrap shouldBe "my-host:9092"
  }

  // --- resolveSaslJaasConfig ---

  "resolveSaslJaasConfig" should "inject sasl.jaas.config from env when security.protocol contains SASL and config is not set" in {
    val params = Map("security.protocol" -> "SASL_SSL", "bootstrap" -> "kafka:9093")
    val resolved = KafkaFlinkSource.resolveSaslJaasConfig(params, {
      case KafkaFlinkSource.SaslJaasConfigEnvVar => Some("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";")
      case _ => None
    })
    resolved("sasl.jaas.config") shouldBe "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";"
  }

  it should "not overwrite sasl.jaas.config if already set in params" in {
    val existingJaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"existing\";"
    val params = Map("security.protocol" -> "SASL_SSL", "sasl.jaas.config" -> existingJaas)
    val resolved = KafkaFlinkSource.resolveSaslJaasConfig(params, _ => Some("should-not-be-used"))
    resolved("sasl.jaas.config") shouldBe existingJaas
  }

  it should "leave params unchanged when security.protocol does not contain SASL" in {
    val params = Map("security.protocol" -> "SSL", "bootstrap" -> "kafka:9093")
    val resolved = KafkaFlinkSource.resolveSaslJaasConfig(params, _ => Some("should-not-be-used"))
    resolved should not contain key("sasl.jaas.config")
  }

  it should "leave params unchanged when security.protocol is not set" in {
    val params = Map("bootstrap" -> "kafka:9092")
    val resolved = KafkaFlinkSource.resolveSaslJaasConfig(params, _ => Some("should-not-be-used"))
    resolved should not contain key("sasl.jaas.config")
  }

  it should "leave params unchanged when SASL_JAAS_CONFIG env var is not set" in {
    val params = Map("security.protocol" -> "SASL_PLAINTEXT")
    val resolved = KafkaFlinkSource.resolveSaslJaasConfig(params, _ => None)
    resolved should not contain key("sasl.jaas.config")
  }

  it should "inject sasl.jaas.config from env when existing value is empty" in {
    val params = Map("security.protocol" -> "SASL_SSL", "sasl.jaas.config" -> "")
    val resolved = KafkaFlinkSource.resolveSaslJaasConfig(params, _ => Some("injected-jaas"))
    resolved("sasl.jaas.config") shouldBe "injected-jaas"
  }

  it should "inject sasl.jaas.config from env when existing value is whitespace-only" in {
    val params = Map("security.protocol" -> "SASL_SSL", "sasl.jaas.config" -> "   ")
    val resolved = KafkaFlinkSource.resolveSaslJaasConfig(params, _ => Some("injected-jaas"))
    resolved("sasl.jaas.config") shouldBe "injected-jaas"
  }
}
