package ai.chronon.flink.test.deser

import ai.chronon.flink.deser.FlinkSerDeProvider
import ai.chronon.flink.deser.SchemaRegistrySerDe
import ai.chronon.flink.deser.SchemaRegistrySerDe.RegistryHostKey
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.SerDe
import org.scalatest.flatspec.AnyFlatSpec

class FlinkSerDeProviderSpec extends AnyFlatSpec {

  "FlinkSerDeProvider.build" should "instantiate SchemaRegistrySerDe via reflection when serde=schema_registry" in {
    val topicInfo = TopicInfo(
      "test-topic",
      "kafka",
      Map("serde" -> "schema_registry", RegistryHostKey -> "localhost")
    )
    val serDe: SerDe = FlinkSerDeProvider.build(topicInfo)
    assert(serDe.isInstanceOf[SchemaRegistrySerDe])
  }

  it should "instantiate SchemaRegistrySerDe via reflection when registry_host is present (legacy fallback)" in {
    val topicInfo = TopicInfo(
      "test-topic",
      "kafka",
      Map(RegistryHostKey -> "localhost")
    )
    val serDe: SerDe = FlinkSerDeProvider.build(topicInfo)
    assert(serDe.isInstanceOf[SchemaRegistrySerDe])
  }

  it should "throw for unsupported serde type" in {
    val topicInfo = TopicInfo(
      "test-topic",
      "kafka",
      Map("serde" -> "unknown")
    )
    assertThrows[IllegalArgumentException] {
      FlinkSerDeProvider.build(topicInfo)
    }
  }
}
