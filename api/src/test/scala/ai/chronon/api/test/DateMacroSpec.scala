package ai.chronon.api.test

import ai.chronon.api.ParametricMacro.{adjustDate, applyBasicDateMacros, removeQuotesIfPresent}
import ai.chronon.api.PartitionSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DateMacroSpec extends AnyFlatSpec with Matchers {

  private val partitionSpec = PartitionSpec.daily

  // Tests for remoteQuotesIfPresent
  "remoteQuotesIfPresent" should "remove single quotes from the beginning and end of a string" in {
    removeQuotesIfPresent("'test'") shouldBe "test"
  }

  it should "remove double quotes from the beginning and end of a string" in {
    removeQuotesIfPresent("\"test\"") shouldBe "test"
  }

  it should "not modify strings without quotes at the beginning and end" in {
    removeQuotesIfPresent("test") shouldBe "test"
  }

  it should "only remove quotes if they are at both the beginning and end" in {
    removeQuotesIfPresent("'test") shouldBe "'test"
    removeQuotesIfPresent("test'") shouldBe "test'"
    removeQuotesIfPresent("\"test") shouldBe "\"test"
    removeQuotesIfPresent("test\"") shouldBe "test\""
  }

  it should "handle empty strings" in {
    removeQuotesIfPresent("") shouldBe ""
    removeQuotesIfPresent("''") shouldBe ""
    removeQuotesIfPresent("\"\"") shouldBe ""
  }

  it should "handle strings with quotes in the middle" in {
    removeQuotesIfPresent("'test'test'") shouldBe "test'test"
    removeQuotesIfPresent("\"test\"test\"") shouldBe "test\"test"
  }

  // Tests for adjustDate
  "adjustDate" should "return the input date when args is empty" in {
    adjustDate("2023-01-01", partitionSpec)(Map.empty) shouldBe "2023-01-01"
  }

  it should "shift the date forward by the offset when offset is positive" in {
    adjustDate("2023-01-01", partitionSpec)(Map("offset" -> "5")) shouldBe "2023-01-06"
  }

  it should "shift the date backward by the offset when offset is negative" in {
    adjustDate("2023-01-10", partitionSpec)(Map("offset" -> "-3")) shouldBe "2023-01-07"
  }

  it should "throw an exception when offset is not an integer" in {
    an[IllegalArgumentException] should be thrownBy {
      adjustDate("2023-01-01", partitionSpec)(Map("offset" -> "not-an-int"))
    }
  }

  it should "return the lower_bound when it's greater than the input date" in {
    adjustDate("2023-01-01", partitionSpec)(Map("lower_bound" -> "2023-01-05")) shouldBe "2023-01-05"
  }

  it should "return the input date when it's greater than the lower_bound" in {
    adjustDate("2023-01-10", partitionSpec)(Map("lower_bound" -> "2023-01-05")) shouldBe "2023-01-10"
  }

  it should "return the upper_bound when it's less than the input date" in {
    adjustDate("2023-01-10", partitionSpec)(Map("upper_bound" -> "2023-01-05")) shouldBe "2023-01-05"
  }

  it should "return the input date when it's less than the upper_bound" in {
    adjustDate("2023-01-01", partitionSpec)(Map("upper_bound" -> "2023-01-05")) shouldBe "2023-01-01"
  }

  it should "handle quoted bounds correctly" in {
    adjustDate("2023-01-05", partitionSpec)(Map("lower_bound" -> "'2023-01-10'")) shouldBe "2023-01-10"
    adjustDate("2023-01-15", partitionSpec)(Map("upper_bound" -> "\"2023-01-10\"")) shouldBe "2023-01-10"
  }

  it should "apply offset before bounds" in {
    // Offset +5 moves from 01-01 to 01-06, then lower_bound moves it to 01-10
    adjustDate("2023-01-01", partitionSpec)(Map("offset" -> "5", "lower_bound" -> "2023-01-10")) shouldBe "2023-01-10"

    // Offset +15 moves from 01-01 to 01-16, then upper_bound restricts it to 01-10
    adjustDate("2023-01-01", partitionSpec)(Map("offset" -> "15", "upper_bound" -> "2023-01-10")) shouldBe "2023-01-10"
  }

  it should "apply both bounds correctly" in {
    // Input 01-05, lower_bound 01-10, upper_bound 01-15 => 01-10
    adjustDate("2023-01-05", partitionSpec)(
      Map("lower_bound" -> "2023-01-10", "upper_bound" -> "2023-01-15")) shouldBe "2023-01-10"

    // Input 01-20, lower_bound 01-10, upper_bound 01-15 => 01-15
    adjustDate("2023-01-20", partitionSpec)(
      Map("lower_bound" -> "2023-01-10", "upper_bound" -> "2023-01-15")) shouldBe "2023-01-15"

    // Input 01-12, lower_bound 01-10, upper_bound 01-15 => 01-12 (within bounds)
    adjustDate("2023-01-12", partitionSpec)(
      Map("lower_bound" -> "2023-01-10", "upper_bound" -> "2023-01-15")) shouldBe "2023-01-12"
  }

  it should "handle all three parameters correctly" in {
    // Input 01-01, offset +5 (to 01-06), lower_bound 01-10, upper_bound 01-15 => 01-10
    adjustDate("2023-01-01", partitionSpec)(
      Map("offset" -> "5", "lower_bound" -> "2023-01-10", "upper_bound" -> "2023-01-15")) shouldBe "2023-01-10"

    // Input 01-01, offset +15 (to 01-16), lower_bound 01-10, upper_bound 01-15 => 01-15
    adjustDate("2023-01-01", partitionSpec)(
      Map("offset" -> "15", "lower_bound" -> "2023-01-10", "upper_bound" -> "2023-01-15")) shouldBe "2023-01-15"

    // Input 01-01, offset +12 (to 01-13), lower_bound 01-10, upper_bound 01-15 => 01-13 (within bounds)
    adjustDate("2023-01-01", partitionSpec)(
      Map("offset" -> "12", "lower_bound" -> "2023-01-10", "upper_bound" -> "2023-01-15")) shouldBe "2023-01-13"
  }

  it should "throw an exception when lower_bound > upper_bound" in {
    an[IllegalArgumentException] should be thrownBy {
      adjustDate("2023-01-10", partitionSpec)(Map("lower_bound" -> "2023-01-15", "upper_bound" -> "2023-01-05"))
    }
  }

  // Tests for applyBasicDateMacros
  "applyBasicDateMacros" should "replace start_date, end_date, and latest_date macros with their respective values" in {
    val query = """SELECT * FROM table WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}"""
    val result = applyBasicDateMacros("2023-01-01", "2023-01-31", "2023-02-01", partitionSpec)(query)

    result shouldBe """SELECT * FROM table WHERE ds BETWEEN '2023-01-01' AND '2023-01-31'"""
  }

  it should "apply offset adjustments to dates" in {
    val query = """SELECT * FROM table WHERE ds BETWEEN {{ start_date(offset=5) }} AND {{ end_date(offset=-2) }}"""
    val result = applyBasicDateMacros("2023-01-01", "2023-01-31", "2023-02-01", partitionSpec)(query)

    result shouldBe """SELECT * FROM table WHERE ds BETWEEN '2023-01-06' AND '2023-01-29'"""
  }

  it should "apply lower_bound constraints to dates" in {
    val query = """SELECT * FROM table WHERE ds BETWEEN {{ start_date(lower_bound='2023-01-10') }} AND {{ end_date }}"""
    val result = applyBasicDateMacros("2023-01-01", "2023-01-31", "2023-02-01", partitionSpec)(query)

    result shouldBe """SELECT * FROM table WHERE ds BETWEEN '2023-01-10' AND '2023-01-31'"""
  }

  it should "apply upper_bound constraints to dates" in {
    val query = """SELECT * FROM table WHERE ds BETWEEN {{ start_date }} AND {{ end_date(upper_bound='2023-01-25') }}"""
    val result = applyBasicDateMacros("2023-01-01", "2023-01-31", "2023-02-01", partitionSpec)(query)

    result shouldBe """SELECT * FROM table WHERE ds BETWEEN '2023-01-01' AND '2023-01-25'"""
  }

  it should "handle latest_date macro" in {
    val query = """SELECT * FROM table WHERE ds = {{ latest_date }}"""
    val result = applyBasicDateMacros("2023-01-01", "2023-01-31", "2023-02-01", partitionSpec)(query)

    result shouldBe """SELECT * FROM table WHERE ds = '2023-02-01'"""
  }

  it should "handle complex combinations of macros and parameters" in {
    val query = """
      SELECT * FROM table
      WHERE
        ds BETWEEN {{ start_date(offset=-3, lower_bound='2023-01-05') }} AND
                 {{ end_date(offset=2, upper_bound='2023-02-05') }} AND
        latest_partition = {{ latest_date(offset=-1) }}
    """
    val result = applyBasicDateMacros("2023-01-10", "2023-01-31", "2023-02-01", partitionSpec)(query)

    // start_date: 2023-01-10 offset by -3 = 2023-01-07, but lower_bound = 2023-01-05, so result is 2023-01-07
    // end_date: 2023-01-31 offset by 2 = 2023-02-02, constrained by upper_bound = 2023-02-05, so result is 2023-02-02
    // latest_date: 2023-02-01 offset by -1 = 2023-01-31
    result shouldBe """
      SELECT * FROM table
      WHERE
        ds BETWEEN '2023-01-07' AND
                 '2023-02-02' AND
        latest_partition = '2023-01-31'
    """
  }

  it should "handle queries with no macros" in {
    val query = """SELECT * FROM table WHERE ds = '2023-01-01'"""
    val result = applyBasicDateMacros("2023-01-01", "2023-01-31", "2023-02-01", partitionSpec)(query)

    result shouldBe """SELECT * FROM table WHERE ds = '2023-01-01'"""
  }

  it should "handle multiple occurrences of the same macro" in {
    val query = """SELECT * FROM table WHERE ds >= {{ start_date }} AND ds <= {{ start_date(offset=7) }}"""
    val result = applyBasicDateMacros("2023-01-01", "2023-01-31", "2023-02-01", partitionSpec)(query)

    result shouldBe """SELECT * FROM table WHERE ds >= '2023-01-01' AND ds <= '2023-01-08'"""
  }

  it should "handle whitespace variations in macro syntax" in {
    val query = """SELECT * FROM table WHERE ds BETWEEN {{start_date}} AND {{    end_date   }}"""
    val result = applyBasicDateMacros("2023-01-01", "2023-01-31", "2023-02-01", partitionSpec)(query)

    result shouldBe """SELECT * FROM table WHERE ds BETWEEN '2023-01-01' AND '2023-01-31'"""
  }

  it should "handle empty parameter lists" in {
    val query = """SELECT * FROM table WHERE ds BETWEEN {{ start_date() }} AND {{ end_date() }}"""
    val result = applyBasicDateMacros("2023-01-01", "2023-01-31", "2023-02-01", partitionSpec)(query)

    result shouldBe """SELECT * FROM table WHERE ds BETWEEN '2023-01-01' AND '2023-01-31'"""
  }

  // sub-daily specs: offset means N partition steps; ds values render in the spec's own format
  it should "render sub-daily ds values and step-denominated offsets" in {
    val threeHourlyAt1 = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000L, 60 * 60 * 1000L)
    val query =
      """SELECT * FROM table WHERE ds BETWEEN {{ start_date(offset=-2) }} AND {{ end_date }} AND latest = {{ latest_date }}"""
    val result =
      applyBasicDateMacros("2023-01-10-01-00", "2023-01-10-07-00", "2023-01-10-22-00", threeHourlyAt1)(query)

    // offset=-2 steps back two 3h partitions, wrapping through the 01:00 boundary
    result shouldBe
      """SELECT * FROM table WHERE ds BETWEEN '2023-01-09-19-00' AND '2023-01-10-07-00' AND latest = '2023-01-10-22-00'"""
  }
}
