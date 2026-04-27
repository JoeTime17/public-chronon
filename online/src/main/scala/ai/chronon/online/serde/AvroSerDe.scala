package ai.chronon.online.serde

import ai.chronon.api.{Constants, StructType}
import org.apache.avro.Schema

class AvroSerDe(val avroSchema: Schema) extends SerDe {

  lazy val chrononSchema: StructType = AvroConversions.toChrononSchema(avroSchema).asInstanceOf[StructType]

  @transient private lazy val avroToRowConverter = AvroConversions.genericRecordToChrononRowConverter(chrononSchema)

  lazy val schemaString: String = avroSchema.toString()

  def avroCodec: ThreadLocal[AvroCodec] = AvroCodec.ofThreaded(schemaString)

  override def schema: StructType = chrononSchema

  private def doDeserialize(bytes: Array[Byte], writerSchemaStr: Option[String] = None): Mutation = {
    val avroRecord = AvroCodec.ofThreaded(schemaString, writerSchemaStr).get().decode(bytes)
    val row: Array[Any] = avroToRowConverter(avroRecord)
    val reversalIndex = schema.indexWhere(_.name == Constants.ReversalColumn)
    if (reversalIndex >= 0 && row(reversalIndex).asInstanceOf[Boolean]) {
      Mutation(schema, row, null)
    } else {
      Mutation(schema, null, row)
    }
  }

  override def fromBytes(bytes: Array[Byte]): Mutation = doDeserialize(bytes)

  def fromBytes(bytes: Array[Byte], writerSchema: Schema): Mutation =
    doDeserialize(bytes, Some(writerSchema.toString))

  def fromBytes(bytes: Array[Byte], writerSchemaStr: String): Mutation =
    doDeserialize(bytes, Some(writerSchemaStr))
}
