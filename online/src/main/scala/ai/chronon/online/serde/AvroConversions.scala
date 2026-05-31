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

package ai.chronon.online.serde

import ai.chronon.api._
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8

import java.nio.ByteBuffer
import java.util
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.{AbstractIterator, mutable}
import com.linkedin.avro.fastserde.{primitive => fastavro}
import com.linkedin.avro.fastserde.BufferBackedPrimitiveFloatList

object AvroConversions {

  def toAvroValue(value: AnyRef, schema: Schema): Object =
    schema.getType match {
      case Schema.Type.UNION => toAvroValue(value, schema.getTypes.get(1))
      case Schema.Type.LONG
          if Option(schema.getLogicalType).map(_.getName).getOrElse("") == LogicalTypes.timestampMillis().getName =>
        // because we're setting spark.sql.datetime.java8API.enabled to True https://github.com/zipline-ai/chronon/blob/main/spark/src/main/scala/ai/chronon/spark/submission/SparkSessionBuilder.scala#L132,
        // we'll convert to java.time.Instant
        value.asInstanceOf[java.time.Instant].asInstanceOf[Object]
      case Schema.Type.LONG => value.asInstanceOf[Long].asInstanceOf[Object]
      case Schema.Type.INT
          if Option(schema.getLogicalType).map(_.getName).getOrElse("") == LogicalTypes.date().getName =>
        // Avro represents as java.time.LocalDate: https://github.com/apache/avro/blob/fe0261deecf22234bbd09251764152d4bf9a9c4a/lang/java/avro/src/main/java/org/apache/avro/data/TimeConversions.java#L38
        value.asInstanceOf[java.time.LocalDate].asInstanceOf[Object]
      case Schema.Type.INT    => value.asInstanceOf[Int].asInstanceOf[Object]
      case Schema.Type.FLOAT  => value.asInstanceOf[Float].asInstanceOf[Object]
      case Schema.Type.DOUBLE => value.asInstanceOf[Double].asInstanceOf[Object]
      case Schema.Type.BYTES =>
        Option(schema.getLogicalType) match {
          case Some(decimalType: LogicalTypes.Decimal) =>
            // Convert BigDecimal to ByteBuffer for Avro decimal logical type
            val bigDecimal = value.asInstanceOf[java.math.BigDecimal]
            val scale = decimalType.getScale
            // Set scale to match the schema's scale
            val scaledDecimal = bigDecimal.setScale(scale, java.math.RoundingMode.HALF_UP)
            val unscaledValue = scaledDecimal.unscaledValue()
            ByteBuffer.wrap(unscaledValue.toByteArray).asInstanceOf[Object]
          case _ => value
        }
      // Recursively process arrays and maps to handle nested values requiring transformation.
      // Avro natively supports collections of primitives (Int, Long, String, etc.), but types
      // requiring explicit conversion (e.g., BigDecimal → ByteBuffer for decimal logical type)
      // must be transformed recursively within nested structures.
      case Schema.Type.ARRAY =>
        val elementSchema = schema.getElementType
        val list = value.asInstanceOf[util.ArrayList[AnyRef]]
        val result = new util.ArrayList[Any](list.size())
        val it = list.iterator()
        while (it.hasNext) {
          val elem = it.next()
          result.add(if (elem != null) toAvroValue(elem, elementSchema) else null)
        }
        result.asInstanceOf[Object]
      case Schema.Type.MAP =>
        val valueSchema = schema.getValueType
        val map = value.asInstanceOf[util.Map[Any, AnyRef]]
        val result = new util.HashMap[Any, Any](map.size())
        val it = map.entrySet().iterator()
        while (it.hasNext) {
          val entry = it.next()
          val convertedValue = if (entry.getValue != null) toAvroValue(entry.getValue, valueSchema) else null
          result.put(entry.getKey, convertedValue)
        }
        result.asInstanceOf[Object]
      case _ => value
    }

  def toChrononSchema(schema: Schema): DataType = {
    schema.getType match {
      case Schema.Type.RECORD =>
        StructType(schema.getName,
                   schema.getFields.asScala.toArray.map { field =>
                     StructField(field.name(), toChrononSchema(field.schema()))
                   })
      case Schema.Type.ARRAY  => ListType(toChrononSchema(schema.getElementType))
      case Schema.Type.MAP    => MapType(StringType, toChrononSchema(schema.getValueType))
      case Schema.Type.STRING => StringType
      case Schema.Type.ENUM   => StringType
      case Schema.Type.INT
          if Option(schema.getLogicalType).map(_.getName).getOrElse("") == LogicalTypes.date().getName =>
        DateType
      case Schema.Type.INT => IntType
      case Schema.Type.LONG
          if Option(schema.getLogicalType).map(_.getName).getOrElse("") == LogicalTypes.timestampMillis().getName =>
        TimestampType
      case Schema.Type.LONG   => LongType
      case Schema.Type.FLOAT  => FloatType
      case Schema.Type.DOUBLE => DoubleType
      case Schema.Type.BYTES => {
        Option(schema.getLogicalType) match {
          case Some(decimalType: LogicalTypes.Decimal) =>
            DecimalType(decimalType.getPrecision, decimalType.getScale)
          case _ => BinaryType
        }
      }
      case Schema.Type.BOOLEAN => BooleanType
      case Schema.Type.UNION => toChrononSchema(schema.getTypes.get(1)) // unions are only used to represent nullability
      case _ => throw new UnsupportedOperationException(s"Cannot convert avro type ${schema.getType.toString}")
    }
  }

  def fromChrononSchema(dataType: DataType,
                        nameSet: mutable.Set[String] = new mutable.HashSet[String],
                        fieldPath: String = ""): Schema = {
    dataType match {
      case StructType(name, fields) =>
        assert(name != null)

        // Generate unique name based on field path
        val uniqueName = if (fieldPath.isEmpty) {
          name
        } else {
          s"${fieldPath.replace(".", "_")}_$name"
        }

        Schema.createRecord(
          uniqueName,
          "", // doc
          "ai.chronon.data", // namespace
          false, // isError
          fields
            .map { chrononField =>
              val defaultValue: AnyRef = null
              val childPath = if (fieldPath.isEmpty) chrononField.name else s"$fieldPath.${chrononField.name}"
              new Field(chrononField.name,
                        Schema.createUnion(Schema.create(Schema.Type.NULL),
                                           fromChrononSchema(chrononField.fieldType, nameSet, childPath)),
                        "",
                        defaultValue)
            }
            .toList
            .asJava
        )
      case ListType(elementType) => Schema.createArray(fromChrononSchema(elementType, nameSet, fieldPath))
      case MapType(keyType, valueType) => {
        assert(keyType == StringType, "Avro only supports string keys for a map")
        Schema.createMap(fromChrononSchema(valueType, nameSet, fieldPath))
      }
      case StringType    => Schema.create(Schema.Type.STRING)
      case IntType       => Schema.create(Schema.Type.INT)
      case LongType      => Schema.create(Schema.Type.LONG)
      case FloatType     => Schema.create(Schema.Type.FLOAT)
      case DoubleType    => Schema.create(Schema.Type.DOUBLE)
      case BinaryType    => Schema.create(Schema.Type.BYTES)
      case BooleanType   => Schema.create(Schema.Type.BOOLEAN)
      case TimestampType => LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
      case DateType =>
        LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))
      case DecimalType(precision, scale) =>
        LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES))
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot convert chronon type $dataType to avro type. Cast it to string please")
    }
  }

  def fromChrononRow(value: Any,
                     dataType: DataType,
                     topLevelSchema: Schema,
                     extraneousRecord: Any => Array[Any] = null): Any = {
    // But this also has to happen at the recursive depth - data type and schema inside the compositor need to
    Row.to[GenericRecord, ByteBuffer, util.ArrayList[Any], util.Map[Any, Any], Schema](
      value,
      dataType,
      { (data: Array[Any], elemDataType: DataType, providedSchema: Option[Schema]) =>
        val schema = providedSchema.getOrElse(AvroConversions.fromChrononSchema(elemDataType))
        val record = new GenericData.Record(schema)
        // For StructType, apply toAvroValue to each field to handle types requiring transformation.
        // This ensures fields like DecimalType (BigDecimal → ByteBuffer) are properly converted
        // before being added to the GenericRecord, as Avro doesn't natively support BigDecimal.
        elemDataType match {
          case StructType(_, fields) =>
            data.zipWithIndex.foreach { case (value1, idx) =>
              val fieldSchema = schema.getFields.get(idx).schema()
              val convertedValue = if (value1 != null) toAvroValue(value1.asInstanceOf[AnyRef], fieldSchema) else null
              record.put(idx, convertedValue)
            }
          case _ =>
            data.zipWithIndex.foreach { case (value1, idx) =>
              record.put(idx, value1)
            }
        }
        record
      },
      ByteBuffer.wrap,
      { (elems: Array[Any], size: Int) =>
        val result = new util.ArrayList[Any](size)
        elems.foreach(result.add)
        result
      },
      { m: util.Map[Any, Any] => m },
      extraneousRecord,
      Some(AvroSchemaTraverser(topLevelSchema))
    )
  }

  def genericRecordToChrononRowConverter(schema: StructType): Any => Array[Any] = {
    val cachedFunc = toChrononRowCached(schema)

    { value: Any =>
      if (value == null) null
      else {
        cachedFunc(value).asInstanceOf[Array[Any]]
      }
    }
  }

  def buildArray(size: Int, iterator: util.Iterator[Any]): util.ArrayList[Any] = {
    val arr = new util.ArrayList[Any](size)
    while (iterator.hasNext) {
      arr.add(iterator.next())
    }
    arr
  }

  private def toChrononRowCached(dataType: DataType): Any => Any = {
    Row.fromCached[GenericRecord, ByteBuffer, Any, Any](
      dataType,
      { (record: GenericRecord, recordLength: Int) =>
        new AbstractIterator[Any]() {
          var idx = 0
          override def next(): Any = {
            val res = record.get(idx)
            idx += 1
            res
          }
          override def hasNext: Boolean = idx < recordLength
        }
      },
      { (byteBuffer: ByteBuffer) => byteBuffer.array() },
      { // cases are ordered by most frequent use
        // TODO: Leverage type info if this case match proves to be expensive
        case doubles: fastavro.PrimitiveDoubleArrayList =>
          val arr = new util.ArrayList[Any](doubles.size)
          val iterator = doubles.iterator()
          while (iterator.hasNext) {
            arr.add(iterator.next())
          }
          arr

        case longs: fastavro.PrimitiveLongArrayList =>
          val arr = new util.ArrayList[Any](longs.size)
          val iterator = longs.iterator()
          while (iterator.hasNext) {
            arr.add(iterator.next())
          }
          arr

        case genericArray: GenericData.Array[_] =>
          val arr = new util.ArrayList[Any](genericArray.size)
          val iterator = genericArray.iterator()
          while (iterator.hasNext) {
            arr.add(iterator.next())
          }
          arr

        case ints: fastavro.PrimitiveIntArrayList =>
          val arr = new util.ArrayList[Any](ints.size)
          val iterator = ints.iterator()
          while (iterator.hasNext) {
            arr.add(iterator.next())
          }
          arr

        case floats: fastavro.PrimitiveFloatArrayList =>
          val arr = new util.ArrayList[Any](floats.size)
          val iterator = floats.iterator()
          while (iterator.hasNext) {
            arr.add(iterator.next())
          }
          arr

        case floats: BufferBackedPrimitiveFloatList =>
          val arr = new util.ArrayList[Any](floats.size)
          val iterator = floats.iterator()
          while (iterator.hasNext) {
            arr.add(iterator.next())
          }
          arr

        case bools: fastavro.PrimitiveBooleanArrayList =>
          val arr = new util.ArrayList[Any](bools.size)
          val iterator = bools.iterator()
          while (iterator.hasNext) {
            arr.add(iterator.next())
          }
          arr

        case valueOfUnknownType =>
          throw new RuntimeException(s"Found unknown list type in avro record: ${valueOfUnknownType.getClass.getName}")
      },
      {
        case avString: Utf8                  => avString.toString
        case enumVal: GenericData.EnumSymbol => enumVal.toString
        case str: String                     => str
        case other =>
          throw new IllegalArgumentException(
            s"Unexpected string type: ${other.getClass.getName}. Expected String or Utf8, got: $other"
          )
      }
    )
  }

  def encodeBytes(schema: StructType, extraneousRecord: Any => Array[Any] = null): Any => Array[Byte] = {
    val codec: AvroCodec = new AvroCodec(fromChrononSchema(schema).toString(true));
    { data: Any =>
      val record =
        fromChrononRow(data, codec.chrononSchema, codec.schema, extraneousRecord).asInstanceOf[GenericData.Record]
      val bytes = codec.encodeBinary(record)
      bytes
    }
  }

  def encodeJson(schema: StructType, extraneousRecord: Any => Array[Any] = null): Any => String = {
    val codec: AvroCodec = new AvroCodec(fromChrononSchema(schema).toString(true));
    { data: Any =>
      val record =
        fromChrononRow(data, codec.chrononSchema, codec.schema, extraneousRecord).asInstanceOf[GenericData.Record]
      val json = codec.encodeJson(record)
      json
    }
  }
}

case class AvroSchemaTraverser(currentNode: Schema) extends SchemaTraverser[Schema] {

  // We only use union types for nullable fields, and always
  // unbox them when writing the actual schema out.
  private def unboxUnion(maybeUnion: Schema): Schema =
    if (maybeUnion.getType == Schema.Type.UNION) {
      maybeUnion.getTypes.get(1)
    } else {
      maybeUnion
    }

  override def getField(field: StructField): SchemaTraverser[Schema] =
    copy(
      unboxUnion(currentNode.getField(field.name).schema())
    )

  override def getCollectionType: SchemaTraverser[Schema] =
    copy(
      unboxUnion(currentNode.getElementType)
    )

  // Avro map keys are always strings.
  override def getMapKeyType: SchemaTraverser[Schema] =
    if (currentNode.getType == Schema.Type.MAP) {
      copy(
        Schema.create(Schema.Type.STRING)
      )
    } else {
      throw new UnsupportedOperationException(
        s"Current node ${currentNode.getName} is a ${currentNode.getType}, not a ${Schema.Type.MAP}"
      )
    }

  override def getMapValueType: SchemaTraverser[Schema] =
    copy(
      unboxUnion(currentNode.getValueType)
    )
}
