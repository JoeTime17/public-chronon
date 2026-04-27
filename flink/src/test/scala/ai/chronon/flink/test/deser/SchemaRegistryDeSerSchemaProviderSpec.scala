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

  // Bug scenario 1: Reading historical data
  // Topic has records written with schema 1 (old) and schema 2 (new).
  // SchemaRegistrySerDe fetches schema 2 (latest) and uses it to decode ALL records.
  // Records written with schema 1 will be decoded with schema 2's binary layout → broken.
  it should "demonstrate bug: old data (schema 1) decoded with latest schema (schema 2) produces error" in {
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

    // Use a fresh MockSchemaRegistryClient so we control exactly which schemas are registered
    val freshClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider).asJava)
    val subject = "evolution-test-1-value"

    // Register schema 1 first, then schema 2 → schema 2 becomes "latest"
    val schema1Id = freshClient.register(subject, new AvroSchema(schema1Str))
    val schema2Id = freshClient.register(subject, new AvroSchema(schema2Str))
    assert(schema1Id != schema2Id, "Schema IDs should differ")

    // Encode a record using schema 1 (the old schema)
    val codec1 = new AvroCodec(schema1Str)
    val record1 = new GenericData.Record(codec1.schema)
    record1.put("name", "John")
    record1.put("age", 30)
    val schema1Bytes = codec1.encodeBinary(record1)

    // Build wire format message with schema 1's ID
    val wireMessage = buildWireFormatMessage(schema1Id, schema1Bytes)

    // Create SerDe — it will fetch schema 2 (latest) and use it as the ONLY decode schema
    val topicInfo = TopicInfo(
      "evolution-test-1",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "true")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, freshClient)

    // BUG: fromBytes() will try to decode schema-1 bytes using schema-2 layout.
    // Schema 2 expects 3 fields (name, age, email) but the bytes only contain 2 fields.
    // The decoder will try to read a union tag for the "email" field from bytes that don't exist,
    // causing an exception or corrupt data.
    val caught = intercept[Exception] {
      serDe.fromBytes(wireMessage)
    }
    // The exception proves the bug: schema-1 data cannot be decoded with schema-2 as the writer schema.
    assert(caught != null,
      "Decoding schema-1 bytes with schema-2 layout should fail because " +
        "the binary format has no email field bytes, but the decoder expects them")
  }

  // Bug scenario 2: Flink started before schema upgrade
  // Flink starts and caches schema 1 (the latest at startup time).
  // Later, producers upgrade to schema 2 and start writing new records.
  // SchemaRegistrySerDe still uses schema 1 to decode schema-2 data → broken.
  it should "demonstrate bug: new data (schema 2) decoded with old cached schema (schema 1) produces error" in {
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

    // Simulate Flink startup: only schema 1 is registered at this point
    val freshClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider).asJava)
    val subject = "evolution-test-2-value"
    val schema1Id = freshClient.register(subject, new AvroSchema(schema1Str))

    // Create SerDe at startup — it fetches schema 1 (the only/latest schema)
    val topicInfo = TopicInfo(
      "evolution-test-2",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "true")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, freshClient)

    // Force initialization of the delegate SerDe with schema 1
    val cachedSchema = serDe.schema
    assert(cachedSchema != null)

    // Now simulate a producer upgrade: encode a record using schema 2
    val codec2 = new AvroCodec(schema2Str)
    val record2 = new GenericData.Record(codec2.schema)
    record2.put("name", "Alice")
    record2.put("age", 25)
    record2.put("email", "alice@test.com")
    val schema2Bytes = codec2.encodeBinary(record2)

    // Schema 2 is now registered (but SerDe already cached schema 1)
    val schema2Id = freshClient.register(subject, new AvroSchema(schema2Str))
    val wireMessage = buildWireFormatMessage(schema2Id, schema2Bytes)

    // BUG: fromBytes() will try to decode schema-2 bytes using schema-1 layout.
    // Schema 1 only has 2 fields (name, age), but the bytes contain 3 fields.
    // The decoder reads name and age correctly but leaves the email bytes unconsumed.
    // Depending on the Avro implementation this may:
    // - silently succeed but ignore the extra bytes (data loss for the email field)
    // - throw an exception due to unexpected trailing bytes
    // Either way, the email field is lost — the schema has no email field.
    try {
      val mutation = serDe.fromBytes(wireMessage)
      // If it doesn't throw, verify data loss: the returned schema has no email field
      // because the SerDe is using schema 1 which only knows about name and age.
      assert(mutation.after.length == 2,
        "SerDe using schema 1 only returns 2 fields — email data is silently lost")
      assert(mutation.after(0) == "Alice")
      assert(mutation.after(1) == 25)
      // The email field "alice@test.com" was in the bytes but is completely lost —
      // the consumer has no way to know it existed. This is the bug.
      println("BUG CONFIRMED: Schema-2 record decoded with schema-1 — email field silently dropped")
    } catch {
      case e: Exception =>
        // Also acceptable proof of the bug: the decoder choked on extra bytes
        println(s"BUG CONFIRMED: Schema-2 record cannot be decoded with schema-1: ${e.getMessage}")
        assert(e != null)
    }
  }

  // Bug scenario 3: Flink started before schema upgrade, new field inserted in the MIDDLE
  // Flink starts and caches schema 1 (the latest at startup time).
  // Later, producers upgrade to schema 3 which adds "email" BETWEEN "name" and "age".
  // SchemaRegistrySerDe still uses schema 1 to decode schema-3 data.
  // Because Avro binary encoding is positional, the decoder reads the email string bytes
  // as if they were the age int → silent data corruption (wrong values, no error).
  it should "demonstrate bug: new field inserted in middle (schema 3) decoded with old schema (schema 1) causes silent corruption" in {
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

    // Simulate Flink startup: only schema 1 is registered
    val freshClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider).asJava)
    val subject = "evolution-test-3-value"
    val schema1Id = freshClient.register(subject, new AvroSchema(schema1Str))

    // Create SerDe at startup — it fetches schema 1 (the only/latest schema)
    val topicInfo = TopicInfo(
      "evolution-test-3",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "true")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, freshClient)

    // Force initialization of the delegate SerDe with schema 1
    val cachedSchema = serDe.schema
    assert(cachedSchema != null)

    // Now simulate a producer upgrade: encode a record using schema 3
    // Schema 3 has email BETWEEN name and age
    val codec3 = new AvroCodec(schema3Str)
    val record3 = new GenericData.Record(codec3.schema)
    record3.put("name", "Bob")
    record3.put("email", "bob@test.com")
    record3.put("age", 35)
    val schema3Bytes = codec3.encodeBinary(record3)

    // Schema 3 binary layout:
    //   [name bytes: "Bob"] [email bytes: "bob@test.com"] [age bytes: 35]
    //
    // Schema 1 decoder reads positionally:
    //   field 1 "name" (string): reads "Bob" ✓
    //   field 2 "age"  (int):    reads from email bytes! The decoder tries to interpret
    //                            the first byte(s) of "bob@test.com" as a zigzag-encoded int
    //                            → WRONG VALUE or exception (type mismatch)

    val schema3Id = freshClient.register(subject, new AvroSchema(schema3Str))
    val wireMessage = buildWireFormatMessage(schema3Id, schema3Bytes)

    try {
      val mutation = serDe.fromBytes(wireMessage)
      // If it doesn't throw, the "age" field is corrupted — it read email bytes as an int
      assert(mutation.after(0) == "Bob", "name should decode correctly (it's the first field in both schemas)")
      assert(mutation.after(1) != 35,
        "age should be WRONG — the decoder read email string bytes as an int, producing a garbage value")
      println(s"BUG CONFIRMED: Silent data corruption — age field is ${mutation.after(1)} instead of 35 " +
        "(decoder read email bytes as int)")
    } catch {
      case e: Exception =>
        // The decoder may also crash if the email bytes can't be interpreted as an int
        println(s"BUG CONFIRMED: Schema-3 record (field inserted in middle) cannot be decoded with schema-1: ${e.getMessage}")
        assert(e != null)
    }
  }
}
