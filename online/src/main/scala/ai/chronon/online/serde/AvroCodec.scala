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

import ai.chronon.api.{DataType, Row, StructType}
import ai.chronon.api.ScalaJavaConversions._
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io._
import com.linkedin.avro.fastserde.FastGenericDatumReader
import com.linkedin.avro.fastserde.FastGenericDatumWriter
import java.util.concurrent.ConcurrentHashMap

import java.io.ByteArrayOutputStream

class AvroCodec(val schemaStr: String, val writerSchemaStr: Option[String] = None) extends Serializable {
  @transient private lazy val parser = new Schema.Parser()
  @transient lazy val schema: Schema = parser.parse(schemaStr)

  // we reuse a lot of intermediate
  // lazy vals so that spark can serialize & ship the codec to executors
  @transient private lazy val datumWriter = new FastGenericDatumWriter[GenericRecord](schema)
  @transient private lazy val datumReader = writerSchemaStr match {
    case Some(wStr) =>
      // Writer schema differs from reader schema — use Avro's resolution logic to handle
      // schema evolution (fills defaults for missing fields, ignores unknown fields)
      new FastGenericDatumReader[GenericRecord](new Schema.Parser().parse(wStr), schema)
    case None =>
      new FastGenericDatumReader[GenericRecord](schema)
  }

  @transient private lazy val outputStream = new ByteArrayOutputStream()
  @transient private var jsonEncoder: JsonEncoder = null
  val fieldNames: Array[String] = schema.getFields.toScala.map(_.name()).toArray
  @transient lazy val chrononSchema: DataType = AvroConversions.toChrononSchema(schema)

  @transient private var binaryEncoder: BinaryEncoder = null
  @transient private var decoder: BinaryDecoder = null
  @transient lazy val schemaElems: Array[Field] = schema.getFields.toScala.toArray
  @transient lazy val toChrononRowFunc: Any => Array[Any] =
    AvroConversions.genericRecordToChrononRowConverter(chrononSchema.asInstanceOf[StructType])

  def encode(valueMap: Map[String, AnyRef]): Array[Byte] = {
    val record = new GenericData.Record(schema)
    schemaElems.foreach { field =>
      record.put(field.name(), AvroConversions.toAvroValue(valueMap.get(field.name()).orNull, field.schema()))
    }
    encodeBinary(record)
  }

  def encode(row: Row): Array[Byte] = {
    val record = new GenericData.Record(schema)
    for (i <- 0 until row.length) {
      record.put(i, row.get(i))
    }
    encodeBinary(record)
  }

  def encodeBinary(record: GenericRecord): Array[Byte] = {
    binaryEncoder = EncoderFactory.get.binaryEncoder(outputStream, binaryEncoder)
    encodeRecord(record, binaryEncoder)
  }

  def encodeRecord(record: GenericRecord, reusableEncoder: Encoder): Array[Byte] = {
    outputStream.reset()
    datumWriter.write(record, reusableEncoder)
    reusableEncoder.flush()
    outputStream.flush()
    outputStream.toByteArray
  }

  def encodeJson(record: GenericRecord): String = {
    jsonEncoder = EncoderFactory.get.jsonEncoder(schema, outputStream)
    new String(encodeRecord(record, jsonEncoder))
  }

  def decode(bytes: Array[Byte]): GenericRecord = {

    if (bytes == null) return null
    val inputStream = new SeekableByteArrayInput(bytes)
    inputStream.reset()
    decoder = DecoderFactory.get.directBinaryDecoder(inputStream, decoder)
    datumReader.read(null, decoder)
  }

  def decodeRow(bytes: Array[Byte]): Array[Any] = toChrononRowFunc(decode(bytes))

  def decodeRow(bytes: Array[Byte], millis: Long, mutation: Boolean = false): ArrayRow =
    new ArrayRow(decodeRow(bytes), millis, mutation)

  def decodeArray(bytes: Array[Byte]): Array[Any] = {
    if (bytes == null) return null
    toChrononRowFunc(decode(bytes))
  }

  def decodeMap(bytes: Array[Byte]): Map[String, AnyRef] = {
    if (bytes == null) return null

    val decoded = decodeArray(bytes)
    fieldNames.iterator.zip(decoded.iterator.map(_.asInstanceOf[AnyRef])).toMap
  }
}

/** Consumed by row aggregator after decoding.
  * Mutations follow the same schema as input for value indices. However there are two main differences.
  *  * ts and reversal columns are required for computation
  *  * Mutation ts takes on the role of ts.
  * Since the schema is the same with the sole difference of the added columns, we add these columns on the tail
  * of the Array and extract them accordingly.
  * i.e. for mutations: reversal index = ArrayRow.length - (Constants.MutationAvroColumns.length - (index of reversal in Constants.MutationAvroColumns)
  */
class ArrayRow(values: Array[Any], millis: Long, mutation: Boolean = false) extends Row {
  override def get(index: Int): Any = values(index)

  override def ts: Long = if (mutation) values(values.length - 2).asInstanceOf[Long] else millis

  override def isBefore: Boolean = if (mutation) values(values.length - 1).asInstanceOf[Boolean] else false

  override def mutationTs: Long = millis

  override val length: Int = values.length
}

object AvroCodec {
  // creating new codecs is expensive - so we want to do it once per process
  // but at the same-time we want to avoid contention across threads - hence thread-local
  private val codecMap: ConcurrentHashMap[String, ThreadLocal[AvroCodec]] =
    new ConcurrentHashMap[String, ThreadLocal[AvroCodec]]

  def ofThreaded(readerSchemaStr: String, writerSchemaStr: Option[String] = None): ThreadLocal[AvroCodec] = {
    val cacheKey = writerSchemaStr.fold(readerSchemaStr)(w => s"$w|$readerSchemaStr")
    codecMap.computeIfAbsent(cacheKey,
                             _ =>
                               new ThreadLocal[AvroCodec] {
                                 override def initialValue(): AvroCodec =
                                   new AvroCodec(readerSchemaStr, writerSchemaStr)
                               })
  }

  def of(schemaStr: String): AvroCodec = ofThreaded(schemaStr).get()
}
