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

package ai.chronon.aggregator.base

import ai.chronon.aggregator.base.FrequentItemType.ItemType
import ai.chronon.api._
import org.apache.datasketches.common.ArrayOfDoublesSerDe
import org.apache.datasketches.common.ArrayOfItemsSerDe
import org.apache.datasketches.common.ArrayOfLongsSerDe
import org.apache.datasketches.common.ArrayOfStringsSerDe
import org.apache.datasketches.cpc.CpcSketch
import org.apache.datasketches.cpc.CpcUnion
import org.apache.datasketches.frequencies.ErrorType
import org.apache.datasketches.frequencies.ItemsSketch
import org.apache.datasketches.kll.KllFloatsSketch
import org.apache.datasketches.memory.Memory

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import org.apache.spark.sql.Row

class Sum[I: Numeric](inputType: DataType) extends SimpleAggregator[I, I, I] {
  private val numericImpl = implicitly[Numeric[I]]

  override def outputType: DataType = inputType

  override def irType: DataType = inputType

  override def prepare(input: I): I = input

  override def update(ir: I, input: I): I = numericImpl.plus(ir, input)

  override def merge(ir1: I, ir2: I): I = numericImpl.plus(ir1, ir2)

  override def finalize(ir: I): I = ir

  override def delete(ir: I, input: I): I = numericImpl.minus(ir, input)

  override def isDeletable: Boolean = true

  override def clone(ir: I): I = ir
}

class Count extends SimpleAggregator[Any, Long, Long] {
  override def outputType: DataType = LongType

  override def irType: DataType = LongType

  override def prepare(input: Any): Long = 1

  override def update(ir: Long, input: Any): Long = ir + 1

  override def merge(ir1: Long, ir2: Long): Long = ir1 + ir2

  override def finalize(ir: Long): Long = ir

  override def delete(ir: Long, input: Any): Long = ir - 1

  override def isDeletable: Boolean = true

  override def clone(ir: Long): Long = ir

  override def emptyOutputValue: Any = 0L
}

class UniqueCount[T](inputType: DataType) extends SimpleAggregator[T, util.HashSet[T], Long] {
  override def outputType: DataType = LongType

  override def irType: DataType = ListType(inputType)

  override def prepare(input: T): util.HashSet[T] = {
    val result = new util.HashSet[T]()
    result.add(input)
    result
  }

  override def update(ir: util.HashSet[T], input: T): util.HashSet[T] = {
    if (!ir.contains(input)) {
      ir.add(input)
    }
    ir
  }

  override def merge(ir1: util.HashSet[T], ir2: util.HashSet[T]): util.HashSet[T] = {
    ir1.addAll(ir2)
    ir1
  }

  override def finalize(ir: util.HashSet[T]): Long = ir.size()

  override def clone(ir: util.HashSet[T]): util.HashSet[T] = {
    val cloned = new util.HashSet[T]()
    cloned.addAll(ir)
    cloned
  }

  override def normalize(ir: util.HashSet[T]): Any = {
    val arr = new util.ArrayList[T](ir.size())
    arr.addAll(ir)
    arr
  }

  override def denormalize(ir: Any): util.HashSet[T] = {
    val set = new util.HashSet[T]()
    set.addAll(ir.asInstanceOf[util.ArrayList[T]])
    set
  }

  override def emptyOutputValue: Any = 0L
}

class Average extends SimpleAggregator[Double, Array[Any], Double] {
  override def outputType: DataType = DoubleType

  override def irType: DataType =
    StructType(
      "AvgIr",
      Array(StructField("sum", DoubleType), StructField("count", LongType))
    )

  override def prepare(input: Double): Array[Any] = Array(input, 1L)

  // mutating
  override def update(ir: Array[Any], input: Double): Array[Any] = {
    ir.update(0, ir(0).asInstanceOf[Double] + input)
    ir.update(1, ir(1).asInstanceOf[Long] + 1)
    ir
  }

  // mutating
  override def merge(ir1: Array[Any], ir2: Array[Any]): Array[Any] = {
    ir1.update(0, ir1(0).asInstanceOf[Double] + ir2(0).asInstanceOf[Double])
    ir1.update(1, ir1(1).asInstanceOf[Long] + ir2(1).asInstanceOf[Long])
    ir1
  }

  override def finalize(ir: Array[Any]): Double =
    ir(0).asInstanceOf[Double] / ir(1).asInstanceOf[Long].toDouble

  override def delete(ir: Array[Any], input: Double): Array[Any] = {
    ir.update(0, ir(0).asInstanceOf[Double] - input)
    ir.update(1, ir(1).asInstanceOf[Long] - 1)
    ir
  }

  override def clone(ir: Array[Any]): Array[Any] = {
    val arr = new Array[Any](ir.length)
    ir.copyToArray(arr)
    arr
  }

  override def isDeletable: Boolean = true
}

// Welford algo for computing variance
// Traditional sum of squares based formula has serious numerical stability problems
class WelfordState(ir: Array[Any]) {
  private def count: Long = try { ir(0).asInstanceOf[Long] }
  catch {
    // cristian-zlai: used to be an Integer. Adding exception handling to patch inconsistencies.
    case e: ClassCastException => ir(0).asInstanceOf[Int].toLong
    case e                     => throw e
  }
  private def setCount(c: Long): Unit = ir.update(0, c)
  private def mean: Double = ir(1).asInstanceOf[Double]
  private def setMean(m: Double): Unit = ir.update(1, m)
  private def m2: Double = ir(2).asInstanceOf[Double]
  private def setM2(m: Double): Unit = ir.update(2, m)

  private def getCombinedMeanDouble(weightN: Double, an: Double, weightK: Double, ak: Double): Double =
    if (weightN < weightK) getCombinedMeanDouble(weightK, ak, weightN, an)
    else
      (weightN + weightK) match {
        case 0.0                             => 0.0
        case newCount if newCount == weightN => an
        case newCount =>
          val scaling = weightK / newCount
          if (scaling < 0.1) (an + (ak - an) * scaling)
          else (weightN * an + weightK * ak) / newCount
      }

  // See http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
  def update(input: Double): Unit = {
    setCount(count + 1)
    val delta = input - mean
    setMean(mean + (delta / count))
    val delta2 = input - mean
    setM2(m2 + (delta * delta2))
  }

  def finalizeImpl(): Double = m2 / count

  // See http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
  def merge(other: WelfordState): Unit = {
    val countCombined = count + other.count
    val delta = other.mean - mean
    val delta_n = delta / countCombined
    val meanCombined = getCombinedMeanDouble(count, mean, other.count, other.mean)
    val m2Combined = m2 + other.m2 + (delta * delta_n * count * other.count)
    setCount(countCombined)
    setMean(meanCombined)
    setM2(m2Combined)
  }
}

object WelfordState {
  def init: Array[Any] = Array(0L, 0.0, 0.0)
}

class Variance extends SimpleAggregator[Double, Array[Any], Double] {
  override def outputType: DataType = DoubleType

  override def irType: DataType =
    StructType(
      "VarianceIr",
      Array(StructField("count", LongType), StructField("mean", DoubleType), StructField("m2", DoubleType))
    )

  override def prepare(input: Double): Array[Any] = {
    val ir = WelfordState.init
    new WelfordState(ir).update(input)
    ir
  }

  override def update(ir: Array[Any], input: Double): Array[Any] = {
    new WelfordState(ir).update(input)
    ir
  }

  override def merge(ir1: Array[Any], ir2: Array[Any]): Array[Any] = {
    new WelfordState(ir1).merge(new WelfordState(ir2))
    ir1
  }

  override def finalize(ir: Array[Any]): Double =
    new WelfordState(ir).finalizeImpl()

  override def clone(ir: Array[Any]): Array[Any] = {
    val arr = new Array[Any](ir.length)
    ir.copyToArray(arr)
    arr
  }

  override def isDeletable: Boolean = false
}

class Histogram(k: Int = 0) extends SimpleAggregator[String, util.Map[String, Int], util.Map[String, Int]] {
  type IrMap = util.Map[String, Int]
  override def outputType: DataType = MapType(StringType, IntType)

  override def irType: DataType = outputType

  override def prepare(input: String): IrMap = {
    val result = new util.HashMap[String, Int]()
    result.put(input, 1)
    result
  }

  def incrementInMap(ir: IrMap, input: String, count: Int = 1): IrMap = {
    val old = ir.get(input)
    val newVal = if (old == null) count else old + count
    if (newVal == 0) {
      ir.remove(input)
    } else {
      ir.put(input, newVal)
    }
    ir
  }

  // mutating
  override def update(ir: IrMap, input: String): IrMap = {
    incrementInMap(ir, input)
  }

  // mutating
  override def merge(ir1: IrMap, ir2: IrMap): IrMap = {
    val it = ir2.entrySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      val key = entry.getKey
      val value = entry.getValue
      incrementInMap(ir1, key, value)
    }
    ir1
  }

  override def finalize(ir: IrMap): IrMap = {
    if (k > 0 && ir.size() > k) { // keep only top k values
      val pq = new MinHeap[Int](k, Ordering[Int].reverse)
      val it = ir.entrySet().iterator()
      val heap = new util.ArrayList[Int]()
      while (it.hasNext) { pq.insert(heap, it.next().getValue) }
      val cutOff = pq.sort(heap).get(k - 1)
      val newResult = new util.HashMap[String, Int]()
      val itNew = ir.entrySet().iterator()
      while (itNew.hasNext && newResult.size() < k) {
        val entry = itNew.next()
        if (entry.getValue >= cutOff) {
          newResult.put(entry.getKey, entry.getValue)
        }
      }
      newResult
    } else {
      ir
    }
  }

  override def delete(ir: IrMap, input: String): IrMap = {
    incrementInMap(ir, input, -1)
  }

  override def clone(ir: IrMap): IrMap = {
    val result = new util.HashMap[String, Int]();
    result.putAll(ir);
    result
  }

  override def isDeletable: Boolean = true
}

trait CpcFriendly[Input] extends Serializable {
  def update(sketch: CpcSketch, input: Input): Unit
}

object CpcFriendly {
  implicit val stringIsCpcFriendly: CpcFriendly[String] = new CpcFriendly[String] {
    override def update(sketch: CpcSketch, input: String): Unit = sketch.update(input)
  }
  implicit val intIsCpcFriendly: CpcFriendly[Int] = new CpcFriendly[Int] {
    override def update(sketch: CpcSketch, input: Int): Unit = sketch.update(input.toLong)
  }

  implicit val floatIsCpcFriendly: CpcFriendly[Float] = new CpcFriendly[Float] {
    override def update(sketch: CpcSketch, input: Float): Unit = sketch.update(input.toDouble)
  }

  implicit val longIsCpcFriendly: CpcFriendly[Long] = new CpcFriendly[Long] {
    override def update(sketch: CpcSketch, input: Long): Unit = sketch.update(input)
  }
  implicit val doubleIsCpcFriendly: CpcFriendly[Double] = new CpcFriendly[Double] {
    override def update(sketch: CpcSketch, input: Double): Unit = sketch.update(input)
  }

  implicit val decimalIsCpcFriendly: CpcFriendly[java.math.BigDecimal] = new CpcFriendly[java.math.BigDecimal] {
    override def update(sketch: CpcSketch, input: java.math.BigDecimal): Unit = sketch.update(input.toPlainString)
  }

  implicit val BinaryIsCpcFriendly: CpcFriendly[Array[Byte]] = new CpcFriendly[Array[Byte]] {
    override def update(sketch: CpcSketch, input: Array[Byte]): Unit = sketch.update(input)
  }
}

object FrequentItemType extends Enumeration {
  type ItemType = Value
  val StringItemType, LongItemType, DoubleItemType = Value
}

case class ItemsSketchIR[T](sketch: ItemsSketch[T], sketchType: ItemType)

trait FrequentItemsFriendly[Input] {
  def serializer: ArrayOfItemsSerDe[Input]
  def sketchType: FrequentItemType.ItemType
}

object FrequentItemsFriendly {
  implicit val stringIsFrequentItemsFriendly: FrequentItemsFriendly[String] = new FrequentItemsFriendly[String] {
    override def serializer: ArrayOfItemsSerDe[String] = new ArrayOfStringsSerDe
    override def sketchType: ItemType = FrequentItemType.StringItemType
  }

  implicit val longIsFrequentItemsFriendly: FrequentItemsFriendly[java.lang.Long] =
    new FrequentItemsFriendly[java.lang.Long] {
      override def serializer: ArrayOfItemsSerDe[java.lang.Long] = new ArrayOfLongsSerDe
      override def sketchType: ItemType = FrequentItemType.LongItemType
    }

  implicit val doubleIsFrequentItemsFriendly: FrequentItemsFriendly[java.lang.Double] =
    new FrequentItemsFriendly[java.lang.Double] {
      override def serializer: ArrayOfItemsSerDe[java.lang.Double] = new ArrayOfDoublesSerDe
      override def sketchType: ItemType = FrequentItemType.DoubleItemType
    }
}

class FrequentItems[T: FrequentItemsFriendly](val mapSize: Int, val errorType: ErrorType = ErrorType.NO_FALSE_NEGATIVES)
    extends SimpleAggregator[T, ItemsSketchIR[T], util.Map[String, Long]] {
  private type Sketch = ItemsSketchIR[T]

  val sketchSize: Int = {
    // during the purge of internal map this removes more half the elements
    // and internal map is 0.75x of k - so to keep k at all times - we need to set mapSize = k / (0.5*0.75)
    val effectiveMapSize = math.ceil(mapSize.toDouble / (0.75 * 0.5)).toInt

    // The ItemsSketch implementation requires a size with a positive power of 2
    // Initialize the sketch with the next closest power of 2
    if (effectiveMapSize > 1) Integer.highestOneBit(effectiveMapSize - 1) << 1 else 2
  }

  override def outputType: DataType = MapType(StringType, LongType)

  override def irType: DataType = BinaryType

  override def prepare(input: T): ItemsSketchIR[T] = {
    val sketch = new ItemsSketch[T](sketchSize)
    val sketchType = implicitly[FrequentItemsFriendly[T]].sketchType
    sketch.update(input)
    ItemsSketchIR(sketch, sketchType)
  }

  override def update(ir: Sketch, input: T): Sketch = {
    ir.sketch.update(input)
    ir
  }
  override def merge(ir1: Sketch, ir2: Sketch): Sketch = {
    ir1.sketch.merge(ir2.sketch)
    ir1
  }

  // ItemsSketch doesn't have a proper copy method. So we serialize and deserialize.
  override def clone(ir: Sketch): Sketch = {
    val serializer = implicitly[FrequentItemsFriendly[T]].serializer
    val bytes = ir.sketch.toByteArray(serializer)
    val clonedSketch = ItemsSketch.getInstance(Memory.wrap(bytes), serializer)
    ItemsSketchIR(clonedSketch, ir.sketchType)
  }

  override def finalize(ir: Sketch): util.Map[String, Long] = {
    if (mapSize <= 0) {
      return new util.HashMap[String, Long]()
    }

    // useful with debugger on - keep around
    // val outputSketchSize = ir.sketch.getNumActiveItems
    // val serializer = implicitly[FrequentItemsFriendly[T]].serializer
    // val outputSketchBytes = ir.sketch.toByteArray(serializer).length

    val items = ir.sketch.getFrequentItems(errorType).map(sk => sk.getItem -> sk.getEstimate)
    val heap = mutable.PriorityQueue[(T, Long)]()(Ordering.by(_._2))

    items.foreach({ case (key, value) =>
      if (heap.size < mapSize) {
        heap.enqueue((key, value))
      } else if (heap.head._2 < value) {
        heap.dequeue()
        heap.enqueue((key, value))
      }
    })

    val result = new util.HashMap[String, Long]()
    val entries = heap.dequeueAll.toList
    entries.foreach({ case (k, v) => result.put(String.valueOf(k), v) })
    result
  }

  override def normalize(ir: Sketch): Array[Byte] = {
    val serializer = implicitly[FrequentItemsFriendly[T]].serializer
    (Seq(ir.sketchType.id.byteValue()) ++ ir.sketch.toByteArray(serializer)).toArray
  }

  override def denormalize(normalized: Any): Sketch = {
    val bytes = normalized.asInstanceOf[Array[Byte]]
    val sketchType = FrequentItemType(bytes.head)
    val serializer = implicitly[FrequentItemsFriendly[T]].serializer
    val sketch = ItemsSketch.getInstance[T](Memory.wrap(bytes.tail), serializer)
    ItemsSketchIR(sketch, sketchType)
  }

  def toSketch(values: util.Map[T, Long]): Sketch = {
    val sketch = new ItemsSketch[T](sketchSize)
    val sketchType = implicitly[FrequentItemsFriendly[T]].sketchType

    values.asScala.foreach({ case (k, v) => sketch.update(k, v) })

    ItemsSketchIR(sketch, sketchType)
  }
}

// Based on CPC sketch (a faster, smaller and more accurate version of HLL)
// See: Back to the future: an even more nearly optimal cardinality estimation algorithm, 2017
// https://arxiv.org/abs/1708.06839
// refer to the chart here to tune your sketch size with lgK
// https://github.com/apache/incubator-datasketches-java/blob/master/src/main/java/org/apache/datasketches/cpc/CpcSketch.java#L180
// default is about 1200 bytes
class ApproxDistinctCount[Input: CpcFriendly](lgK: Int = 8) extends SimpleAggregator[Input, CpcSketch, Long] {
  override def outputType: DataType = LongType

  override def irType: DataType = BinaryType

  override def prepare(input: Input): CpcSketch = {
    val sketch = new CpcSketch(lgK)
    implicitly[CpcFriendly[Input]].update(sketch, input)
    sketch
  }

  override def update(ir: CpcSketch, input: Input): CpcSketch = {
    implicitly[CpcFriendly[Input]].update(ir, input)
    ir
  }

  override def merge(ir1: CpcSketch, ir2: CpcSketch): CpcSketch = {
    val merger = new CpcUnion(lgK)
    merger.update(ir1)
    merger.update(ir2)
    merger.getResult
  }

  override def bulkMerge(irs: Iterator[CpcSketch]): CpcSketch = {
    val merger = new CpcUnion(lgK)
    irs.foreach(merger.update)
    merger.getResult
  }

  // CPC sketch has a non-public copy() method, which is what we want
  // CPCUnion has access to that copy() method - so we kind of hack that
  // mechanism to get a proper copy without any overhead.
  override def clone(ir: CpcSketch): CpcSketch = {
    val merger = new CpcUnion(lgK)
    merger.update(ir)
    merger.getResult
  }

  override def finalize(ir: CpcSketch): Long = ir.getEstimate.toLong

  override def normalize(ir: CpcSketch): Array[Byte] = ir.toByteArray

  override def denormalize(normalized: Any): CpcSketch =
    CpcSketch.heapify(normalized.asInstanceOf[Array[Byte]])

  override def emptyOutputValue: Any = 0L
}

class ApproxPercentiles(k: Int = 128, percentiles: Array[Double] = Array(0.5))
    extends SimpleAggregator[Float, KllFloatsSketch, Array[Float]] {
  override def outputType: DataType = ListType(FloatType)

  override def irType: DataType = BinaryType

  override def prepare(input: Float): KllFloatsSketch = {
    val sketch = KllFloatsSketch.newHeapInstance(k)
    sketch.update(input)
    sketch
  }

  override def update(ir: KllFloatsSketch, input: Float): KllFloatsSketch = {
    ir.update(input)
    ir
  }

  override def merge(ir1: KllFloatsSketch, ir2: KllFloatsSketch): KllFloatsSketch = {
    ir1.merge(ir2)
    ir1
  }

  override def bulkMerge(irs: Iterator[KllFloatsSketch]): KllFloatsSketch = {
    val result = KllFloatsSketch.newHeapInstance(k)
    irs.foreach(result.merge)
    result
  }

  // KLLFloatsketch doesn't have a proper copy method. So we serialize and deserialize.
  override def clone(ir: KllFloatsSketch): KllFloatsSketch = {
    KllFloatsSketch.heapify(Memory.wrap(ir.toByteArray))
  }

  // Produce percentile values for the specified percentiles ex: [0.1, 0.5, 0.95]
  override def finalize(ir: KllFloatsSketch): Array[Float] = ir.getQuantiles(percentiles)

  override def normalize(ir: KllFloatsSketch): Array[Byte] = ir.toByteArray

  override def denormalize(normalized: Any): KllFloatsSketch =
    KllFloatsSketch.heapify(Memory.wrap(normalized.asInstanceOf[Array[Byte]]))
}

abstract class Order[I](inputType: DataType) extends SimpleAggregator[I, I, I] {
  override def outputType: DataType = inputType

  override def irType: DataType = inputType

  override def prepare(input: I): I = input

  override def finalize(ir: I): I = ir

  override def clone(ir: I): I = ir
}

class Max[I: Ordering](inputType: DataType) extends Order[I](inputType) {
  override def update(ir: I, input: I): I =
    implicitly[Ordering[I]].max(ir, input)

  override def merge(ir1: I, ir2: I): I = implicitly[Ordering[I]].max(ir1, ir2)
}

class Min[I: Ordering](inputType: DataType) extends Order[I](inputType) {
  override def update(ir: I, input: I): I =
    implicitly[Ordering[I]].min(ir, input)

  override def merge(ir1: I, ir2: I): I = implicitly[Ordering[I]].min(ir1, ir2)
}

// generalization of topK and bottomK
class OrderByLimit[I: ClassTag](
    inputType: DataType,
    limit: Int,
    ordering: Ordering[I]
) extends SimpleAggregator[I, util.ArrayList[I], util.ArrayList[I]] {
  private val minHeap = new MinHeap[I](limit, ordering)

  override def outputType: DataType = ListType(inputType)

  override def irType: DataType = ListType(inputType)

  type Container = util.ArrayList[I]

  override final def prepare(input: I): Container = {
    val arr = new util.ArrayList[I]()
    arr.add(input)
    arr
  }

  // mutating
  override final def update(state: Container, input: I): Container =
    minHeap.insert(state, input)

  // mutating 1
  override final def merge(state1: Container, state2: Container): Container =
    minHeap.merge(state1, state2)

  override def finalize(state: Container): Container = minHeap.sort(state)

  override def clone(ir: Container): Container = {
    val cloned = new util.ArrayList[I](ir.size())
    cloned.addAll(ir)
    cloned
  }
}

class TopK[T: Ordering: ClassTag](inputType: DataType, k: Int)
    extends OrderByLimit[T](inputType, k, Ordering[T].reverse)

class BottomK[T: Ordering: ClassTag](inputType: DataType, k: Int) extends OrderByLimit[T](inputType, k, Ordering[T])

case class MomentsIR(
    n: Double,
    m1: Double,
    m2: Double,
    m3: Double,
    m4: Double
)

// Uses Welford/Knuth method as the traditional sum of squares based formula has serious numerical stability problems
trait MomentAggregator extends SimpleAggregator[Double, MomentsIR, Double] {
  override def prepare(input: Double): MomentsIR = {
    val ir = MomentsIR(
      n = 0,
      m1 = 0,
      m2 = 0,
      m3 = 0,
      m4 = 0
    )

    update(ir, input)
  }

  // Implementation is similar to the variance calculation above, but is extended to calculate the 3rd and 4th moments.
  // References for the approach are here:
  // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
  // https://www.johndcook.com/blog/skewness_kurtosis/
  override def update(ir: MomentsIR, x: Double): MomentsIR = {
    val n1 = ir.n
    val n = ir.n + 1
    val delta = x - ir.m1
    val deltaN = delta / n
    val deltaN2 = deltaN * deltaN
    val term1 = delta * deltaN * n1
    val m1 = ir.m1 + deltaN
    val m4 = ir.m4 + term1 * deltaN2 * (n * n - 3 * n + 3) + 6 * deltaN2 * ir.m2 - 4 * deltaN * ir.m3
    val m3 = ir.m3 + term1 * deltaN * (n - 2) - 3 * deltaN * ir.m2
    val m2 = ir.m2 + term1

    MomentsIR(
      n = n,
      m1 = m1,
      m2 = m2,
      m3 = m3,
      m4 = m4
    )
  }

  override def outputType: DataType = DoubleType

  override def irType: DataType = ListType(DoubleType)

  // Implementation is similar to the variance calculation above, but is extended to calculate the 3rd and 4th moments.
  // References for the approach are here:
  // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
  // https://www.johndcook.com/blog/skewness_kurtosis/
  override def merge(a: MomentsIR, b: MomentsIR): MomentsIR = {
    val n = a.n + b.n
    val delta = b.m1 - a.m1
    val delta2 = delta * delta
    val delta3 = delta * delta2
    val delta4 = delta2 * delta2

    val m1 = (a.n * a.m1 + b.n * b.m1) / n
    val m2 = a.m2 + b.m2 + delta2 * a.n * b.n / n
    val m3 = a.m3 + b.m3 + delta3 * a.n * b.n * (a.n - b.n) / (n * n) +
      3.0 * delta * (a.n * b.m2 - b.n * a.m2) / n
    val m4 = a.m4 + b.m4 + delta4 * a.n * b.n * (a.n * a.n - a.n * b.n + b.n * b.n) / (n * n * n) +
      6.0 * delta2 * (a.n * a.n * b.m2 + b.n * b.n * a.m2) / (n * n) + 4.0 * delta * (a.n * b.m3 - b.n * a.m3) / n

    MomentsIR(
      n = n,
      m1 = m1,
      m2 = m2,
      m3 = m3,
      m4 = m4
    )
  }

  override def finalize(ir: MomentsIR): Double

  override def clone(ir: MomentsIR): MomentsIR = {
    MomentsIR(
      n = ir.n,
      m1 = ir.m1,
      m2 = ir.m2,
      m3 = ir.m3,
      m4 = ir.m4
    )
  }

  override def normalize(ir: MomentsIR): util.ArrayList[Double] = {
    val values = List(ir.n, ir.m1, ir.m2, ir.m3, ir.m4)
    new util.ArrayList[Double](values.asJava)
  }

  override def denormalize(normalized: Any): MomentsIR = {
    val values = normalized.asInstanceOf[util.ArrayList[Double]].asScala
    MomentsIR(values(0), values(1), values(2), values(3), values(4))
  }

  override def isDeletable = false
}

class Skew extends MomentAggregator {
  override def finalize(ir: MomentsIR): Double =
    if (ir.n < 3 || ir.m2 == 0) Double.NaN else Math.sqrt(ir.n) * ir.m3 / Math.pow(ir.m2, 1.5)
}

class Kurtosis extends MomentAggregator {
  override def finalize(ir: MomentsIR): Double =
    if (ir.n < 4 || ir.m2 == 0) Double.NaN else ir.n * ir.m4 / (ir.m2 * ir.m2) - 3
}

class UniqueTopKHelper[T](inputType: DataType, k: Int, maxSizeOpt: Option[Int] = None) {

  private val maxSize: Int = maxSizeOpt.getOrElse(2 * k)

  // Define the order type and create operator based on input type
  val (operator, stateProvider) = inputType match {
    case IntType =>
      val getOrderKeyInt = (x: T) => x.asInstanceOf[Int]
      val getIdInt = (x: T) => x.asInstanceOf[Int].toLong
      val op = UniqueOrderByLimit.Operator[T, Int](getOrderKeyInt, getIdInt, k, maxSize, topK = true)
      val provider = new StateProvider[T, Int]
      (op, provider)

    case LongType =>
      val getOrderKeyLong = (x: T) => x.asInstanceOf[Long]
      val getIdLong = (x: T) => x.asInstanceOf[Long]
      val op = UniqueOrderByLimit.Operator[T, Long](getOrderKeyLong, getIdLong, k, maxSize, topK = true)
      val provider = new StateProvider[T, Long]
      (op, provider)

    case StringType =>
      val getOrderKeyString = (x: T) => x.asInstanceOf[String]
      val getIdString = (x: T) => x.asInstanceOf[String].hashCode.toLong
      val op = UniqueOrderByLimit.Operator[T, String](getOrderKeyString, getIdString, k, maxSize, topK = true)
      val provider = new StateProvider[T, String]
      (op, provider)

    case structType: StructType =>
      val sortKeyField = structType.fields.find(_.name == "sort_key")
      val uniqueIdField = structType.fields.find(_.name == "unique_id")

      require(sortKeyField.isDefined && sortKeyField.get.fieldType == StringType,
              "Struct must have 'sort_key' field of type String")
      require(uniqueIdField.isDefined && uniqueIdField.get.fieldType == LongType,
              "Struct must have 'unique_id' field of type Long")

      val sortKeyIndex = structType.fields.indexWhere(_.name == "sort_key")
      val uniqueIdIndex = structType.fields.indexWhere(_.name == "unique_id")

      val getOrderKeyStruct = (x: T) =>
        x match {
          case arr: Array[Any]               => arr(sortKeyIndex).asInstanceOf[String]
          case row: org.apache.spark.sql.Row => row.getString(sortKeyIndex)
          case map: Map[String, AnyRef]      => map("sort_key").asInstanceOf[String]
        }

      val getIdStruct = (x: T) =>
        x match {
          case arr: Array[Any]               => arr(uniqueIdIndex).asInstanceOf[Long]
          case row: org.apache.spark.sql.Row => row.getLong(uniqueIdIndex)
          case map: Map[String, AnyRef]      => map("unique_id").asInstanceOf[Long]
        }

      val op = UniqueOrderByLimit.Operator[T, String](getOrderKeyStruct, getIdStruct, k, maxSize, topK = true)
      val provider = new StateProvider[T, String]
      (op, provider)

    case _ =>
      throw new IllegalArgumentException(
        s"Unsupported input type for UniqueTopK: $inputType. " +
          s"Use one of Int, Long, String or Struct(... sort_key: String, unique_id: Long)."
      )
  }

  // Helper class to handle type erasure
  class StateProvider[T, OrderType] {
    def initState(): Any = UniqueOrderByLimit.initState[T, OrderType]

    def insert(elem: T, state: Any): Unit =
      operator
        .asInstanceOf[UniqueOrderByLimit.Operator[T, OrderType]]
        .insert(elem, state.asInstanceOf[UniqueOrderByLimit.State[T, OrderType]])

    def merge(state1: Any, state2: Any): Any = {

      val s1 = state1.asInstanceOf[UniqueOrderByLimit.State[T, OrderType]]
      val s2 = state2.asInstanceOf[UniqueOrderByLimit.State[T, OrderType]]

      val it = s2.elems.iterator()
      while (it.hasNext) {
        val elem = it.next()
        operator.asInstanceOf[UniqueOrderByLimit.Operator[T, OrderType]].insert(elem, s1)
      }

      s1
    }

    def finalize(state: Any): util.ArrayList[T] = {
      val s = state.asInstanceOf[UniqueOrderByLimit.State[T, OrderType]]
      operator.asInstanceOf[UniqueOrderByLimit.Operator[T, OrderType]].sortAndPrune(s)
      s.elems
    }

    def clone(state: Any): Any = {
      val s = state.asInstanceOf[UniqueOrderByLimit.State[T, OrderType]]
      val clonedElems = new util.ArrayList[T](s.elems)
      val clonedIds = new util.HashSet[Long](s.ids)
      UniqueOrderByLimit.State[T, OrderType](clonedElems, clonedIds, s.orderWaterMark)
    }
    def normalize(state: Any): Any = {
      state.asInstanceOf[UniqueOrderByLimit.State[T, OrderType]].elems
    }
    def denormalize(elems: Any): Any = {
      operator
        .asInstanceOf[UniqueOrderByLimit.Operator[T, OrderType]]
        .buildStateFromElems(elems.asInstanceOf[util.ArrayList[T]])
    }
  }
}

class UniqueTopKAggregator[T](inputType: DataType, k: Int, maxSizeOpt: Option[Int] = None)
    extends SimpleAggregator[T, Any, util.ArrayList[T]] {

  private val uniqueTopK = new UniqueTopKHelper[T](inputType, k, maxSizeOpt)

  override def outputType: DataType = ListType(inputType)

  override def irType: DataType = ListType(inputType)

  override def prepare(input: T): Any = {
    val state = uniqueTopK.stateProvider.initState()
    uniqueTopK.stateProvider.insert(input, state)
    state
  }

  override def update(ir: Any, input: T): Any = {
    uniqueTopK.stateProvider.insert(input, ir)
    ir
  }

  override def merge(ir1: Any, ir2: Any): Any = {
    uniqueTopK.stateProvider.merge(ir1, ir2)
  }

  override def finalize(ir: Any): util.ArrayList[T] = {
    uniqueTopK.stateProvider.finalize(ir)
  }

  override def clone(ir: Any): Any = {
    uniqueTopK.stateProvider.clone(ir)
  }

  override def normalize(ir: Any): Any = {
    uniqueTopK.stateProvider.normalize(ir)
  }

  override def denormalize(normalized: Any): Any = {
    uniqueTopK.stateProvider.denormalize(normalized)
  }
}
