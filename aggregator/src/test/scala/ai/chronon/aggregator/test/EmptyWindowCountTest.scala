package ai.chronon.aggregator.test

import ai.chronon.aggregator.windowing.FiveMinuteResolution
import ai.chronon.aggregator.windowing.SawtoothAggregator
import ai.chronon.api._
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class EmptyWindowCountTest extends AnyFlatSpec {
  private val schema = Seq("ts" -> LongType, "amount" -> LongType)
  private val aggregations = Seq(
    Operation.COUNT,
    Operation.UNIQUE_COUNT,
    Operation.APPROX_UNIQUE_COUNT,
    Operation.SUM,
    Operation.AVERAGE,
    Operation.MIN,
    Operation.MAX
  ).map(operation => Builders.Aggregation(operation, "amount", Seq(new Window(1, TimeUnit.DAYS))))

  private def assertEmpty(result: Array[Any]): Unit = {
    assertEquals(7, result.length)
    result.take(3).foreach(value => assertEquals(0L, value))
    result.drop(3).foreach(assertNull)
  }

  private def iteratorResults(events: Seq[Row], queries: Seq[Long]): Array[Array[Any]] = {
    val aggregator = new SawtoothAggregator(aggregations, schema, FiveMinuteResolution)
    val queryRows = mutable.Buffer(queries.map(timestamp => new TestRow(timestamp)(0): Row): _*)
    aggregator
      .cumulateAndFinalizeSortedIterator(mutable.Buffer(events: _*), queryRows, aggregator.windowedAggregator.init)
      .map(_._2)
      .toArray
  }

  it should "return zero counts and null non-counts with no inputs" in {
    val results = iteratorResults(Seq.empty, Seq(1000L, 2000L))
    assertEquals(2, results.length)
    results.foreach(assertEmpty)
  }

  it should "return zero counts when only equal-timestamp or future events exist" in {
    val events = Seq(new TestRow(1000L, 10L)(0), new TestRow(2000L, 20L)(0))
    val results = iteratorResults(events, Seq(500L, 1000L))
    assertEquals(2, results.length)
    results.foreach(assertEmpty)
  }

  it should "return zero counts when qualifying inputs are all null" in {
    val events = Seq(new TestRow(500L, null)(0), new TestRow(1500L, null)(0))
    val results = iteratorResults(events, Seq(1000L, 2000L))
    assertEquals(2, results.length)
    results.foreach(assertEmpty)
  }

  it should "preserve earlier results when later queries include events" in {
    val events = Seq(new TestRow(1000L, 10L)(0), new TestRow(2000L, 20L)(0))
    val results = iteratorResults(events, Seq(1000L, 1001L, 2001L))
    assertEquals(3, results.length)
    assertEmpty(results(0))
    results(1).take(3).foreach(value => assertEquals(1L, value))
    assertEquals(10L, results(1)(3))
    assertEquals(10.0, results(1)(4))
    assertEquals(10L, results(1)(5))
    assertEquals(10L, results(1)(6))
    results(2).take(3).foreach(value => assertEquals(2L, value))
    assertEquals(30L, results(2)(3))
    assertEquals(15.0, results(2)(4))
    assertEquals(10L, results(2)(5))
    assertEquals(20L, results(2)(6))
  }
}
