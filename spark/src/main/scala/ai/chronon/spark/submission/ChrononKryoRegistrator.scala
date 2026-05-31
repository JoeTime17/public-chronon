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
package ai.chronon.spark.submission

import ai.chronon.aggregator.base.FrequentItemType.{DoubleItemType, LongItemType, StringItemType}
import ai.chronon.aggregator.base.FrequentItemsFriendly._
import ai.chronon.aggregator.base.{FrequentItemType, FrequentItemsFriendly, ItemsSketchIR}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.{ClosureSerializer, JavaSerializer}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.datasketches.common.{ArrayOfItemsSerDe, ArrayOfStringsSerDe}
import org.apache.datasketches.cpc.CpcSketch
import org.apache.datasketches.frequencies.ItemsSketch
import org.apache.datasketches.memory.Memory
import org.apache.spark.serializer.KryoRegistrator

import java.lang.invoke.SerializedLambda

class CpcSketchKryoSerializer extends Serializer[CpcSketch] {
  override def write(kryo: Kryo, output: Output, sketch: CpcSketch): Unit = {
    val bytes = sketch.toByteArray
    output.writeInt(bytes.size)
    output.writeBytes(bytes)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[CpcSketch]): CpcSketch = {
    val size = input.readInt()
    val bytes = input.readBytes(size)
    CpcSketch.heapify(bytes)
  }
}

//@SerialVersionUID(3457890987L)
//class ItemSketchSerializable(var mapSize: Int) extends ItemsSketch[String](mapSize) with Serializable {}

class ItemSketchSerializable extends Serializable {
  var sketch: ItemsSketch[String] = null
  def init(mapSize: Int): ItemSketchSerializable = {
    sketch = new ItemsSketch[String](mapSize)
    this
  }

  // necessary for serialization
  private def writeObject(out: java.io.ObjectOutputStream): Unit = {
    val serDe = new ArrayOfStringsSerDe
    val bytes = sketch.toByteArray(serDe)
    out.writeInt(bytes.size)
    out.writeBytes(new String(bytes))
  }

  private def readObject(input: java.io.ObjectInputStream): Unit = {
    val size = input.readInt()
    val bytes = new Array[Byte](size)
    input.read(bytes)
    val serDe = new ArrayOfStringsSerDe
    sketch = ItemsSketch.getInstance[String](Memory.wrap(bytes), serDe)
  }
}

class ItemsSketchKryoSerializer[T] extends Serializer[ItemsSketchIR[T]] {
  def getSerializer(sketchType: FrequentItemType.Value): ArrayOfItemsSerDe[T] = {
    val serializer = sketchType match {
      case StringItemType => implicitly[FrequentItemsFriendly[String]].serializer
      case LongItemType   => implicitly[FrequentItemsFriendly[java.lang.Long]].serializer
      case DoubleItemType => implicitly[FrequentItemsFriendly[java.lang.Double]].serializer
      case _              => throw new IllegalArgumentException(s"No serializer for sketch type $sketchType")
    }
    serializer.asInstanceOf[ArrayOfItemsSerDe[T]]
  }

  override def write(kryo: Kryo, output: Output, sketch: ItemsSketchIR[T]): Unit = {
    val serializer = getSerializer(sketch.sketchType)
    val bytes = sketch.sketch.toByteArray(serializer)
    output.writeInt(sketch.sketchType.id)
    output.writeInt(bytes.size)
    output.writeBytes(bytes)
  }
  override def read(kryo: Kryo, input: Input, `type`: Class[ItemsSketchIR[T]]): ItemsSketchIR[T] = {
    val sketchType = FrequentItemType(input.readInt())
    val size = input.readInt()
    val bytes = input.readBytes(size)
    val serializer = getSerializer(sketchType)
    val sketch = ItemsSketch.getInstance[T](Memory.wrap(bytes), serializer)
    ItemsSketchIR(sketch, sketchType)
  }
}

class ChrononKryoRegistrator extends KryoRegistrator {
  // registering classes tells kryo to not send schema on the wire
  // helps shuffles and spilling to disk
  override def registerClasses(kryo: Kryo): Unit = {
    // kryo.setWarnUnregisteredClasses(true)
    val names = Seq(
      "ai.chronon.aggregator.base.ApproxHistogramIr",
      "ai.chronon.aggregator.base.MomentsIR",
      "ai.chronon.aggregator.base.UniqueOrderByLimit$State",
      "ai.chronon.aggregator.windowing.BatchIr",
      "ai.chronon.aggregator.windowing.FinalBatchIr",
      "ai.chronon.api.Row",
      "ai.chronon.online.LoggableResponse",
      "ai.chronon.online.LoggableResponseBase64",
      "ai.chronon.online.serde.RowWrapper",
      "ai.chronon.online.fetcher.Fetcher$Request",
      "ai.chronon.spark.KeyWithHash",
      "java.math.BigDecimal",
      "java.math.BigInteger",
      "java.time.Instant",
      "java.time.LocalDate",
      "java.time.LocalDateTime",
      "java.util.ArrayList",
      "java.util.Collections$EmptySet",
      "java.util.Collections$EmptyList",
      "java.util.HashMap",
      "java.util.HashSet",
      "java.util.concurrent.ConcurrentHashMap",
      "java.util.concurrent.atomic.AtomicBoolean",
      "org.apache.iceberg.BaseFile$1",
      "org.apache.iceberg.DataFile",
      "org.apache.iceberg.FileContent",
      "org.apache.iceberg.FileFormat",
      "org.apache.iceberg.GenericDataFile",
      "org.apache.iceberg.PartitionData",
      "org.apache.iceberg.SerializableByteBufferMap",
      "org.apache.iceberg.SerializableTable$SerializableConfSupplier",
      "org.apache.iceberg.SnapshotRef",
      "org.apache.iceberg.SnapshotRefType",
      "org.apache.iceberg.encryption.PlaintextEncryptionManager",
      "org.apache.iceberg.gcp.GCPProperties",
      "org.apache.iceberg.hadoop.HadoopFileIO",
      "org.apache.iceberg.hadoop.HadoopMetricsContext",
      "org.apache.iceberg.MetadataTableType",
      "org.apache.iceberg.io.ResolvingFileIO",
      "org.apache.iceberg.spark.source.SerializableTableWithSize",
      "org.apache.iceberg.spark.source.SerializableTableWithSize$SerializableMetadataTableWithSize",
      "org.apache.iceberg.spark.source.SparkWrite$TaskCommit",
      "org.apache.iceberg.types.Types$DateType",
      "org.apache.iceberg.types.Types$NestedField",
      "org.apache.iceberg.types.Types$StringType",
      "org.apache.iceberg.types.Types$StructType",
      "org.apache.iceberg.util.SerializableMap",
      "org.apache.datasketches.kll.KllFloatsSketch",
      "org.apache.datasketches.kll.KllHeapFloatsSketch",
      "org.apache.datasketches.kll.KllSketch$SketchStructure",
      "org.apache.datasketches.kll.KllSketch$SketchType",
      "org.apache.hadoop.fs.BlockLocation",
      "org.apache.hadoop.fs.FileStatus",
      "org.apache.hadoop.fs.FileUtil$CopyFiles",
      "org.apache.hadoop.fs.FileUtil$CopyListingFileStatus",
      "org.apache.hadoop.fs.FileUtil$CopyMapper",
      "org.apache.hadoop.fs.FileUtil$CopyReducer",
      "org.apache.hadoop.fs.LocatedFileStatus",
      "org.apache.hadoop.fs.Path",
      "org.apache.hadoop.fs.StorageType",
      "org.apache.hadoop.fs.permission.FsAction",
      "org.apache.hadoop.fs.permission.FsPermission",
      "org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage",
      "org.apache.spark.storage.BlockId",
      "org.apache.spark.storage.BlockManagerId",
      "org.apache.spark.storage.BlockManagerMessages$GetStorageStatus$",
      "org.apache.spark.storage.BlockStatus",
      "org.apache.spark.storage.RDDBlockId",
      "org.apache.spark.storage.StorageLevel",
      "org.apache.spark.storage.StorageStatus",
      "org.apache.spark.storage.StorageStatus$NonRddStorageInfo",
      "org.apache.spark.storage.StorageStatus$NonRddStorageInfo$",
      "org.apache.spark.storage.StorageStatus$RddStorageInfo",
      "org.apache.spark.storage.StorageStatus$RddStorageInfo$",
      "org.apache.spark.sql.Row",
      "org.apache.spark.sql.types.ArrayType",
      "org.apache.spark.sql.catalyst.InternalRow",
      "org.apache.spark.sql.catalyst.InternalRow$$anonfun$getAccessor$5",
      "org.apache.spark.sql.catalyst.InternalRow$$anonfun$getAccessor$8",
      "org.apache.spark.sql.catalyst.expressions.Ascending$",
      "org.apache.spark.sql.catalyst.expressions.BoundReference",
      "org.apache.spark.sql.catalyst.expressions.Descending$",
      "org.apache.spark.sql.catalyst.expressions.GenericInternalRow",
      "org.apache.spark.sql.catalyst.expressions.GenericRow",
      "org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema",
      "org.apache.spark.sql.catalyst.expressions.NullsFirst$",
      "org.apache.spark.sql.catalyst.expressions.NullsLast$",
      "org.apache.spark.sql.catalyst.expressions.SortOrder",
      "org.apache.spark.sql.catalyst.expressions.UnsafeRow",
      "org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering",
      "org.apache.spark.sql.catalyst.trees.Origin",
      "org.apache.spark.sql.execution.datasources.BasicWriteTaskStats",
      "org.apache.spark.sql.execution.datasources.ExecutedWriteSummary",
      "org.apache.spark.sql.execution.datasources.InMemoryFileIndex",
      "org.apache.spark.sql.execution.datasources.InMemoryFileIndex$SerializableBlockLocation",
      "org.apache.spark.sql.execution.datasources.InMemoryFileIndex$SerializableFileStatus",
      "org.apache.spark.sql.execution.datasources.WriteTaskResult",
      "org.apache.spark.sql.execution.datasources.v2.DataWritingSparkTaskResult",
      "org.apache.spark.sql.execution.joins.EmptyHashedRelation",
      "org.apache.spark.sql.execution.joins.EmptyHashedRelation$",
      "org.apache.spark.sql.execution.joins.LongHashedRelation",
      "org.apache.spark.sql.execution.joins.LongToUnsafeRowMap",
      "org.apache.spark.sql.execution.joins.UnsafeHashedRelation",
      "org.apache.spark.sql.execution.streaming.sources.ForeachWriterCommitMessage$",
      "org.apache.spark.sql.types.BinaryType",
      "org.apache.spark.sql.types.BinaryType$",
      "org.apache.spark.sql.types.BooleanType$",
      "org.apache.spark.sql.types.DataType",
      "org.apache.spark.sql.types.DateType$",
      "org.apache.spark.sql.types.DoubleType$",
      "org.apache.spark.sql.types.IntegerType$",
      "org.apache.spark.sql.types.LongType$",
      "org.apache.spark.sql.types.MapType",
      "org.apache.spark.sql.types.Metadata",
      "org.apache.spark.sql.types.NullType$",
      "org.apache.spark.sql.types.StringType",
      "org.apache.spark.sql.types.StringType$",
      "org.apache.spark.sql.types.StructField",
      "org.apache.spark.sql.types.StructType",
      "org.apache.spark.sql.types.TimestampType$",
      "org.apache.spark.unsafe.types.UTF8String",
      "org.apache.spark.util.HadoopFSUtils$SerializableBlockLocation",
      "org.apache.spark.util.HadoopFSUtils$SerializableFileStatus",
      "org.apache.spark.util.collection.CompactBuffer",
      "org.apache.spark.util.sketch.BitArray",
      "org.apache.spark.util.sketch.BloomFilterImpl",
      "scala.collection.IndexedSeqLike$Elements",
      "scala.collection.immutable.ArraySeq$ofRef",
      "scala.math.Ordering$$anon$4",
      "scala.reflect.ClassTag$$anon$1",
      "scala.reflect.ClassTag$GenericClassTag",
      "scala.reflect.ClassTag$GenericClassTag",
      "scala.reflect.ManifestFactory$$anon$1",
      "scala.reflect.ManifestFactory$$anon$10",
      "scala.reflect.ManifestFactory$LongManifest",
      "scala.reflect.ManifestFactory$LongManifest",
      "scala.collection.immutable.ArraySeq$ofInt",
      // Delta Lake
      "org.apache.spark.sql.delta.stats.DeltaFileStatistics",
      "org.apache.spark.sql.delta.actions.AddFile",
      // Hudi
      "org.apache.hudi.storage.StoragePath",
      "org.apache.hudi.metadata.HoodieBackedTableMetadataWriter$DirectoryInfo",
      "org.apache.hudi.common.model.HoodieAvroRecord",
      "org.apache.hudi.common.model.HoodieKey",
      "org.apache.hudi.common.model.HoodieOperation",
      "org.apache.hudi.metadata.HoodieMetadataPayload",
      "org.apache.hudi.common.model.HoodieRecordLocation",
      "org.apache.hudi.client.FailOnFirstErrorWriteStatus",
      "org.apache.hudi.client.WriteStatus",
      "org.apache.hudi.common.model.HoodieWriteStat",
      "org.apache.hudi.common.model.HoodieWriteStat$RuntimeStats",
      "org.apache.hudi.avro.model.HoodieMetadataFileInfo",
      "org.apache.hudi.common.model.HoodieDeltaWriteStat",
      "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
      "org.apache.hudi.common.util.collection.ImmutablePair",
      "org.apache.hudi.common.util.Option",
      "org.apache.hudi.storage.StoragePathInfo",
      "org.apache.hudi.metadata.HoodieTableMetadataUtil$DirectoryInfo",
      "org.apache.spark.sql.execution.datasources.parquet.Spark35ParquetReader",
      // GCP
      "org.apache.iceberg.gcp.gcs.GCSFileIO"
    )
    names.foreach(name => doRegister(name, kryo))

    kryo.register(classOf[Array[java.lang.Double]])
    kryo.register(classOf[Array[java.lang.Float]])
    kryo.register(classOf[Array[java.lang.Long]])
    kryo.register(classOf[Array[java.lang.Integer]])
    kryo.register(classOf[Array[java.lang.Boolean]])
    kryo.register(classOf[Array[java.lang.String]])

    // GCSFileIO needs JavaSerializer due to closure serialization issues in the GCSFileIO class
    try {
      kryo.register(Class.forName("org.apache.iceberg.gcp.gcs.GCSFileIO"), new JavaSerializer)
    } catch {
      case _: ClassNotFoundException => // Optional GCP class missing
      case _: LinkageError           => // Optional GCP dependency missing
    }

    kryo.register(classOf[Array[Array[Array[AnyRef]]]])
    kryo.register(classOf[Array[Array[AnyRef]]])
    kryo.register(classOf[CpcSketch], new CpcSketchKryoSerializer())
    kryo.register(classOf[Array[ItemSketchSerializable]])
    kryo.register(classOf[ItemsSketchIR[AnyRef]], new ItemsSketchKryoSerializer[AnyRef])
    kryo.register(classOf[SerializedLambda])
    kryo.register(classOf[ClosureSerializer.Closure], new ClosureSerializer)
  }

  def doRegister(name: String, kryo: Kryo): Unit = {
    try {
      kryo.register(Class.forName(name))
      kryo.register(Class.forName(s"[L$name;")) // represents array of a type to jvm
    } catch {
      case _: ClassNotFoundException => // do nothing
      case _: LinkageError           => // do nothing
    }
  }
}
