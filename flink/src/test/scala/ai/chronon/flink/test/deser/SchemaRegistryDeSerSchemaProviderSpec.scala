package ai.chronon.flink.test.deser

import ai.chronon.api.{IntType, StringType, StructField}
import ai.chronon.flink.deser.SchemaRegistrySerDe
import ai.chronon.flink.deser.SchemaRegistrySerDe.{Proto3DefaultAsNullKey, RegistryHostKey, SchemaRegistryWireFormat}
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.AvroCodec
import com.google.protobuf.DynamicMessage
import io.confluent.kafka.schemaregistry.SchemaProvider
import io.confluent.kafka.schemaregistry.avro.{AvroSchema, AvroSchemaProvider}
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.protobuf.{ProtobufSchema, ProtobufSchemaProvider}
import org.apache.avro.generic.GenericData
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._

class MockSchemaRegistrySerDe(topicInfo: TopicInfo, mockSchemaRegistryClient: MockSchemaRegistryClient)
    extends SchemaRegistrySerDe(topicInfo) {
  override def buildSchemaRegistryClient(schemeString: String,
                                         registryHost: String,
                                         maybePortString: Option[String]): MockSchemaRegistryClient =
    mockSchemaRegistryClient
}

class SchemaRegistrySerDeSpec extends AnyFlatSpec {

  private val avroSchemaProvider: SchemaProvider = new AvroSchemaProvider
  private val protoSchemaProvider: SchemaProvider = new ProtobufSchemaProvider
  val schemaRegistryClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider, protoSchemaProvider).asJava)

  it should "fail if the schema subject is not found" in {
    val topicInfo = TopicInfo("test-topic-avro", "kafka", Map(RegistryHostKey -> "localhost"))
    val schemaRegistrySchemaProvider =
      new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)
    assertThrows[IllegalArgumentException] {
      schemaRegistrySchemaProvider.schema
    }
  }

  it should "succeed if we look up an avro schema that is present" in {
    val topicInfo = TopicInfo("test-topic-avro", "kafka", Map(RegistryHostKey -> "localhost"))
    val schemaRegistrySchemaProvider =
      new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    schemaRegistryClient.register("test-topic-avro-value", new AvroSchema(avroSchemaStr))
    val deserSchema = schemaRegistrySchemaProvider.schema
    assert(deserSchema != null)
  }

  it should "succeed if we look up an avro schema using injected subject" in {
    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    schemaRegistryClient.register("my-subject", new AvroSchema(avroSchemaStr))

    val topicInfo = TopicInfo("another-topic", "kafka", Map(RegistryHostKey -> "localhost", "subject" -> "my-subject"))
    val schemaRegistrySchemaProvider =
      new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val deserSchema = schemaRegistrySchemaProvider.schema
    assert(deserSchema != null)
  }

  // ============== Proto3 Tests ==============

  it should "succeed if we look up a proto3 schema" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message TestProto3 {
        |  string name = 1;
        |  int32 age = 2;
        |}""".stripMargin
    schemaRegistryClient.register("test-topic-proto3-value", new ProtobufSchema(proto3SchemaStr))

    val topicInfo = TopicInfo("test-topic-proto3", "kafka", Map(RegistryHostKey -> "localhost"))
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val schema = serDe.schema
    assert(schema != null)
    assert(schema.fields.length == 2)
    assert(schema.fields.exists(f => f.name == "name" && f.fieldType == StringType))
    assert(schema.fields.exists(f => f.name == "age" && f.fieldType == IntType))
  }

  it should "deserialize proto3 messages" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message User {
        |  string username = 1;
        |  int32 user_id = 2;
        |}""".stripMargin
    val protobufSchema = new ProtobufSchema(proto3SchemaStr)
    schemaRegistryClient.register("test-topic-proto3-deser-value", protobufSchema)

    val topicInfo = TopicInfo(
      "test-topic-proto3-deser",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "false")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val descriptor = protobufSchema.toDescriptor()
    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("username"), "alice")
      .setField(descriptor.findFieldByName("user_id"), 42)
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)
    assert(mutation.after != null)
    assert(mutation.after(0) == "alice")
    assert(mutation.after(1) == 42)
  }

  it should "handle proto3DefaultAsNull parameter for proto3 schemas" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message TestDefaults {
        |  string text = 1;
        |  int32 number = 2;
        |}""".stripMargin
    val protobufSchema = new ProtobufSchema(proto3SchemaStr)
    schemaRegistryClient.register("test-proto3-defaults-value", protobufSchema)

    val topicInfoWithNull = TopicInfo(
      "test-proto3-defaults",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "false", Proto3DefaultAsNullKey -> "true")
    )
    val serDeWithNull = new MockSchemaRegistrySerDe(topicInfoWithNull, schemaRegistryClient)

    val descriptor = protobufSchema.toDescriptor()
    val emptyMessage = DynamicMessage.newBuilder(descriptor).build()

    val mutationWithNull = serDeWithNull.fromBytes(emptyMessage.toByteArray)
    assert(mutationWithNull.after(0) == null)
    assert(mutationWithNull.after(1) == null)

    val topicInfoWithoutNull = TopicInfo(
      "test-proto3-defaults",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "false", Proto3DefaultAsNullKey -> "false")
    )
    val serDeWithoutNull = new MockSchemaRegistrySerDe(topicInfoWithoutNull, schemaRegistryClient)

    val mutationWithoutNull = serDeWithoutNull.fromBytes(emptyMessage.toByteArray)
    assert(mutationWithoutNull.after(0) == "")
    assert(mutationWithoutNull.after(1) == 0)
  }

  it should "handle wire format with 5-byte header for proto3" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message WireFormatTest {
        |  string value = 1;
        |}""".stripMargin
    val protobufSchema = new ProtobufSchema(proto3SchemaStr)
    schemaRegistryClient.register("test-wire-format-proto3-value", protobufSchema)

    val topicInfo = TopicInfo(
      "test-wire-format-proto3",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "true")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val descriptor = protobufSchema.toDescriptor()
    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("value"), "test")
      .build()

    val wireFormatBytes = Array[Byte](0x00, 0x00, 0x00, 0x00, 0x01) ++ message.toByteArray

    val mutation = serDe.fromBytes(wireFormatBytes)
    assert(mutation.after != null)
    assert(mutation.after(0) == "test")
  }

  // ============== Proto2 Tests ==============

  it should "succeed if we look up a proto2 schema" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message TestProto2 {
        |  required string name = 1;
        |  optional int32 age = 2;
        |}""".stripMargin
    schemaRegistryClient.register("test-topic-proto2-value", new ProtobufSchema(proto2SchemaStr))

    val topicInfo = TopicInfo("test-topic-proto2", "kafka", Map(RegistryHostKey -> "localhost"))
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val schema = serDe.schema
    assert(schema != null)
    assert(schema.fields.length == 2)
    assert(schema.fields.exists(f => f.name == "name" && f.fieldType == StringType))
    assert(schema.fields.exists(f => f.name == "age" && f.fieldType == IntType))
  }

  it should "deserialize proto2 messages with required and optional fields" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message Person {
        |  required string name = 1;
        |  optional int32 id = 2;
        |}""".stripMargin
    val protobufSchema = new ProtobufSchema(proto2SchemaStr)
    schemaRegistryClient.register("test-topic-proto2-deser-value", protobufSchema)

    val topicInfo = TopicInfo(
      "test-topic-proto2-deser",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "false")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val descriptor = protobufSchema.toDescriptor()
    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("name"), "bob")
      .setField(descriptor.findFieldByName("id"), 123)
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)
    assert(mutation.after != null)
    assert(mutation.after(0) == "bob")
    assert(mutation.after(1) == 123)
  }

  it should "handle proto2 unset optional fields as null" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message OptionalTest {
        |  required string name = 1;
        |  optional int32 value = 2;
        |}""".stripMargin
    val protobufSchema = new ProtobufSchema(proto2SchemaStr)
    schemaRegistryClient.register("test-proto2-optional-value", protobufSchema)

    val topicInfo = TopicInfo(
      "test-proto2-optional",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "false")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val descriptor = protobufSchema.toDescriptor()
    val messageWithOnlyRequired = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("name"), "test")
      .build()

    val mutation = serDe.fromBytes(messageWithOnlyRequired.toByteArray)
    assert(mutation.after != null)
    assert(mutation.after(0) == "test")
    assert(mutation.after(1) == null)
  }

  // ============== Schema Evolution Bug Tests ==============

  /**
    * Helper to build a Confluent wire format message:
    * [0x00 magic byte] [4-byte schema ID big-endian] [avro payload]
    */
  private def buildWireFormatMessage(schemaId: Int, avroPayload: Array[Byte]): Array[Byte] = {
    val header = ByteBuffer.allocate(5)
    header.put(0x00.toByte) // magic byte
    header.putInt(schemaId)
    header.array() ++ avroPayload
  }

  // Schema evolution scenario 1: Reading historical data
  // Topic has records written with schema 1 (old) and schema 2 (new).
  // SchemaRegistrySerDe fetches schema 2 (latest) as the reader schema but reads the writer
  // schema ID from the wire header to correctly decode schema-1 messages.
  it should "correctly decode old data (schema 1) when latest schema (schema 2) adds a nullable field" in {
    val schema1Str =
      """{ "type": "record", "name": "User", "fields": [
        |  { "name": "name", "type": "string" },
        |  { "name": "age", "type": "int" }
        |]}""".stripMargin

    val schema2Str =
      """{ "type": "record", "name": "User", "fields": [
        |  { "name": "name", "type": "string" },
        |  { "name": "age", "type": "int" },
        |  { "name": "email", "type": ["null", "string"], "default": null }
        |]}""".stripMargin

    val freshClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider).asJava)
    val subject = "evolution-test-1-value"

    val schema1Id = freshClient.register(subject, new AvroSchema(schema1Str))
    val schema2Id = freshClient.register(subject, new AvroSchema(schema2Str))
    assert(schema1Id != schema2Id, "Schema IDs should differ")

    val codec1 = new AvroCodec(schema1Str)
    val record1 = new GenericData.Record(codec1.schema)
    record1.put("name", "John")
    record1.put("age", 30)
    val wireMessage = buildWireFormatMessage(schema1Id, codec1.encodeBinary(record1))

    val topicInfo = TopicInfo(
      "evolution-test-1",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "true")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, freshClient)

    // Avro resolution: schema-1 bytes decoded with schema-2 as reader.
    // The missing "email" field is filled with its default (null).
    val mutation = serDe.fromBytes(wireMessage)
    assert(mutation.after(0) == "John")
    assert(mutation.after(1) == 30)
    assert(mutation.after(2) == null, "email should be null (default from schema 2)")
  }

  // Schema evolution scenario 2: Flink started before schema upgrade
  // Flink starts and caches schema 1 as the reader (latest at startup).
  // Later, producers upgrade to schema 2. The wire header carries schema 2's ID,
  // so the SerDe fetches schema 2 as the writer schema and decodes correctly.
  it should "correctly decode new data (schema 2) when Flink started with schema 1 as reader" in {
    val schema1Str =
      """{ "type": "record", "name": "Person", "fields": [
        |  { "name": "name", "type": "string" },
        |  { "name": "age", "type": "int" }
        |]}""".stripMargin

    val schema2Str =
      """{ "type": "record", "name": "Person", "fields": [
        |  { "name": "name", "type": "string" },
        |  { "name": "age", "type": "int" },
        |  { "name": "email", "type": ["null", "string"], "default": null }
        |]}""".stripMargin

    val freshClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider).asJava)
    val subject = "evolution-test-2-value"
    val schema1Id = freshClient.register(subject, new AvroSchema(schema1Str))

    val topicInfo = TopicInfo(
      "evolution-test-2",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "true")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, freshClient)

    // Force initialization — SerDe caches schema 1 as the reader schema
    assert(serDe.schema != null)

    // Producer upgrades to schema 2 and writes a new record
    val codec2 = new AvroCodec(schema2Str)
    val record2 = new GenericData.Record(codec2.schema)
    record2.put("name", "Alice")
    record2.put("age", 25)
    record2.put("email", "alice@test.com")
    val schema2Id = freshClient.register(subject, new AvroSchema(schema2Str))
    val wireMessage = buildWireFormatMessage(schema2Id, codec2.encodeBinary(record2))

    // Avro resolution: schema-2 bytes decoded with schema-1 as reader.
    // The extra "email" field in the writer schema is ignored (not in reader schema).
    val mutation = serDe.fromBytes(wireMessage)
    assert(mutation.after(0) == "Alice")
    assert(mutation.after(1) == 25)
  }

  // Schema evolution scenario 3: new field inserted in the MIDDLE (incompatible change)
  // Flink starts and caches schema 1 as the reader. Producer registers schema 3 which
  // inserts "email" between "name" and "age". Because the wire header carries schema 3's ID,
  // the SerDe fetches schema 3 as the writer and uses Avro resolution to map fields by name,
  // correctly extracting "name" and "age" despite the positional shift.
  it should "correctly decode data when a new field is inserted in the middle (schema 3) and Flink has schema 1 as reader" in {
    val schema1Str =
      """{ "type": "record", "name": "Employee", "fields": [
        |  { "name": "name", "type": "string" },
        |  { "name": "age", "type": "int" }
        |]}""".stripMargin

    val schema3Str =
      """{ "type": "record", "name": "Employee", "fields": [
        |  { "name": "name", "type": "string" },
        |  { "name": "email", "type": "string" },
        |  { "name": "age", "type": "int" }
        |]}""".stripMargin

    val freshClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider).asJava)
    val subject = "evolution-test-3-value"
    freshClient.register(subject, new AvroSchema(schema1Str))

    val topicInfo = TopicInfo(
      "evolution-test-3",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "true")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, freshClient)

    assert(serDe.schema != null)

    val codec3 = new AvroCodec(schema3Str)
    val record3 = new GenericData.Record(codec3.schema)
    record3.put("name", "Bob")
    record3.put("email", "bob@test.com")
    record3.put("age", 35)

    // MockSchemaRegistryClient doesn't enforce compatibility by default
    val schema3Id = freshClient.register(subject, new AvroSchema(schema3Str))
    val wireMessage = buildWireFormatMessage(schema3Id, codec3.encodeBinary(record3))

    // Avro resolution maps fields by name: "name" and "age" are matched correctly,
    // "email" (present in writer, absent in reader) is skipped.
    val mutation = serDe.fromBytes(wireMessage)
    assert(mutation.after(0) == "Bob")
    assert(mutation.after(1) == 35, "age should be 35 — Avro resolution matches fields by name, not position")
  }
}
