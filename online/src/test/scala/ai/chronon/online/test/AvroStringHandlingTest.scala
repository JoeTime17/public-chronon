package ai.chronon.online.test

import ai.chronon.api._
import ai.chronon.online.serde.AvroConversions
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Test string handling in AvroConversions to ensure both java.lang.String
 * and org.apache.avro.util.Utf8 types are properly handled.
 *
 * This addresses the ClassCastException that occurs when Avro schemas use
 * "avro.java.string": "String" and produce String objects instead of Utf8.
 */
class AvroStringHandlingTest extends AnyFlatSpec with Matchers {

  "AvroConversions" should "handle String objects from avro.java.string schemas" in {
    // Create a schema with string fields
    val schema = StructType(
      "StringTestSchema",
      Array(
        StructField("stringField", StringType)
      )
    )

    // Create Avro schema with String type preference
    val avroSchemaStr = """{
      "type": "record",
      "name": "StringTestSchema",
      "namespace": "ai.chronon.data",
      "fields": [
        {
          "name": "stringField",
          "type": ["null", {
            "type": "string",
            "avro.java.string": "String"
          }],
          "default": null
        }
      ]
    }"""

    val avroSchema = new Schema.Parser().parse(avroSchemaStr)

    // Create test data with java.lang.String objects (not Utf8)
    val record = new GenericData.Record(avroSchema)
    record.put("stringField", "test_value")  // This will be java.lang.String

    // Test the conversion using our fixed toChrononRowCached
    val converter = AvroConversions.genericRecordToChrononRowConverter(schema)
    val result = converter(record)

    // Verify the conversion worked without ClassCastException
    result should not be null
    result.length shouldBe 1
    result(0) shouldBe "test_value"
  }

  it should "handle Utf8 objects from standard Avro schemas" in {
    // Standard Avro schema (without avro.java.string property)
    val schema = StructType(
      "Utf8TestSchema",
      Array(
        StructField("utf8Field", StringType)
      )
    )

    val avroSchemaStr = """{
      "type": "record",
      "name": "Utf8TestSchema",
      "namespace": "ai.chronon.data",
      "fields": [
        {
          "name": "utf8Field",
          "type": ["null", "string"],
          "default": null
        }
      ]
    }"""

    val avroSchema = new Schema.Parser().parse(avroSchemaStr)

    // Create record with Utf8 object
    val record = new GenericData.Record(avroSchema)
    record.put(0, new Utf8("utf8_test_value"))

    // Test conversion
    val converter = AvroConversions.genericRecordToChrononRowConverter(schema)
    val result = converter(record)

    // Verify it works with Utf8 too
    result should not be null
    result.length shouldBe 1
    result(0) shouldBe "utf8_test_value"
  }

  it should "handle mixed String and Utf8 objects in the same schema" in {
    val schema = StructType(
      "MixedStringSchema",
      Array(
        StructField("stringField", StringType),
        StructField("utf8Field", StringType)
      )
    )

    val avroSchemaStr = """{
      "type": "record",
      "name": "MixedStringSchema",
      "namespace": "ai.chronon.data",
      "fields": [
        {
          "name": "stringField",
          "type": ["null", {
            "type": "string",
            "avro.java.string": "String"
          }],
          "default": null
        },
        {
          "name": "utf8Field",
          "type": ["null", "string"],
          "default": null
        }
      ]
    }"""

    val avroSchema = new Schema.Parser().parse(avroSchemaStr)
    val record = new GenericData.Record(avroSchema)

    // Mix String and Utf8 objects
    record.put(0, "java_string")           // java.lang.String
    record.put(1, new Utf8("utf8_string")) // Utf8

    val converter = AvroConversions.genericRecordToChrononRowConverter(schema)
    val result = converter(record)

    result should not be null
    result.length shouldBe 2
    result(0) shouldBe "java_string"
    result(1) shouldBe "utf8_string"
  }

  it should "treat avro enums as strings" in {
    val avroSchemaStr = """{
      "type": "record",
      "name": "EnumTestSchema",
      "namespace": "ai.chronon.data",
      "fields": [
        {
          "name": "status",
          "type": ["null", {
            "type": "enum",
            "name": "Status",
            "symbols": ["ACTIVE", "INACTIVE"]
          }],
          "default": null
        }
      ]
    }"""

    val avroSchema = new Schema.Parser().parse(avroSchemaStr)

    val chrononSchema = AvroConversions.toChrononSchema(avroSchema).asInstanceOf[StructType]
    chrononSchema.fields(0).fieldType shouldBe StringType

    val statusSchema = avroSchema.getField("status").schema().getTypes.get(1)
    val record = new GenericData.Record(avroSchema)
    record.put("status", new GenericData.EnumSymbol(statusSchema, "ACTIVE"))

    val converter = AvroConversions.genericRecordToChrononRowConverter(chrononSchema)
    val result = converter(record)

    result(0) shouldBe "ACTIVE"
  }
}
