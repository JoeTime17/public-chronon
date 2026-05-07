package ai.chronon.flink.deser

import ai.chronon.api.StructType
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{AvroConversions, AvroSerDe, Mutation, ProtobufSerDe, SerDe}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import java.nio.ByteBuffer

/** Schema Provider / SerDe implementation that uses the Confluent Schema Registry to fetch schemas for topics.
  * Supports both Avro and Protobuf schemas.
  *
  * Can be configured as: topic = "kafka://topic-name/registry_host=host/[registry_port=port]/[registry_scheme=http]/[subject=subject]/[proto3_default_as_null=false]"
  * Port, scheme and subject are optional. If port is missing, we assume the host is pointing to a LB address / such that
  * forwards to the right host + port. Scheme defaults to http. Subject defaults to the topic name + "-value" (based on schema
  * registry conventions).
  */
class SchemaRegistrySerDe(topicInfo: TopicInfo) extends SerDe {
  import SchemaRegistrySerDe._

  private val schemaRegistryHost: String =
    topicInfo.params.getOrElse(RegistryHostKey, throw new IllegalArgumentException(s"$RegistryHostKey not set"))

  // port is optional as many folks configure just the host as it's behind an LB
  private val schemaRegistryPortString: Option[String] = topicInfo.params.get(RegistryPortKey)

  // default to http if not set
  private val schemaRegistrySchemeString: String = topicInfo.params.getOrElse(RegistrySchemeKey, "http")

  private val CacheCapacity: Int = 10

  private val schemaRegistryWireFormat: Boolean =
    topicInfo.params.getOrElse(SchemaRegistryWireFormat, "true").toBoolean

  private val proto3DefaultAsNull: Boolean =
    topicInfo.params.getOrElse(Proto3DefaultAsNullKey, "false").toBoolean

  protected[flink] def buildSchemaRegistryClient(schemeString: String,
                                                 registryHost: String,
                                                 maybePortString: Option[String]): SchemaRegistryClient = {
    maybePortString match {
      case Some(portString) =>
        val registryUrl = s"$schemeString://$registryHost:$portString"
        new CachedSchemaRegistryClient(registryUrl, CacheCapacity)
      case None =>
        val registryUrl = s"$schemeString://$registryHost"
        new CachedSchemaRegistryClient(registryUrl, CacheCapacity)
    }
  }

  @transient private lazy val schemaRegistryClient: SchemaRegistryClient =
    buildSchemaRegistryClient(schemaRegistrySchemeString, schemaRegistryHost, schemaRegistryPortString)

  @transient private lazy val delegate: SerDe = buildSerDe(topicInfo)

  private def buildSerDe(topicInfo: TopicInfo): SerDe = {
    val subject = topicInfo.params.getOrElse(RegistrySubjectKey, s"${topicInfo.name}-value")
    val parsedSchema =
      try {
        val metadata = schemaRegistryClient.getLatestSchemaMetadata(subject)
        schemaRegistryClient.getSchemaById(metadata.getId)
      } catch {
        case e: RestClientException =>
          throw new IllegalArgumentException(
            s"Failed to retrieve schema details from the registry. Status: ${e.getStatus}; Error code: ${e.getErrorCode}",
            e)
        case e: Exception =>
          throw new IllegalArgumentException("Error connecting to and requesting schema details from the registry", e)
      }

    parsedSchema.schemaType() match {
      case AvroSchema.TYPE =>
        val avroSchema = parsedSchema.asInstanceOf[AvroSchema].rawSchema()
        new AvroSerDe(avroSchema)
      case ProtobufSchema.TYPE =>
        val protobufSchema = parsedSchema.asInstanceOf[ProtobufSchema]
        val descriptor = protobufSchema.toDescriptor()
        new ProtobufSerDe(descriptor, proto3DefaultAsNull)
      case other =>
        throw new IllegalArgumentException(s"Unsupported schema type: $other. Supported types are Avro and Protobuf.")
    }
  }

  override def schema: StructType = delegate.schema

  override def fromBytes(message: Array[Byte]): Mutation = {
    if (schemaRegistryWireFormat) {
      // Wire format: [0x00 magic byte][4-byte schema ID big-endian][payload]
      // https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#messages-wire-format
      val writerSchemaId = ByteBuffer.wrap(message, 1, 4).getInt
      val messageBytes = message.drop(5)
      delegate match {
        case avroSerDe: AvroSerDe =>
          val writerAvroSchema = schemaRegistryClient
            .getSchemaById(writerSchemaId)
            .asInstanceOf[AvroSchema]
            .rawSchema()
          avroSerDe.fromBytes(messageBytes, writerAvroSchema)
        case _ =>
          // Protobuf is self-describing (field tags in every message) — no schema resolution needed
          delegate.fromBytes(messageBytes)
      }
    } else {
      delegate.fromBytes(message)
    }
  }
}

object SchemaRegistrySerDe {
  val RegistryHostKey = "registry_host"
  val RegistryPortKey = "registry_port"
  val RegistrySchemeKey = "registry_scheme"
  val RegistrySubjectKey = "subject"
  val SchemaRegistryWireFormat = "schema_registry_wire_format"
  val Proto3DefaultAsNullKey = "proto3_default_as_null"
}
