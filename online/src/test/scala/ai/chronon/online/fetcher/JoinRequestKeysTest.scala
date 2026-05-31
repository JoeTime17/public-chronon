package ai.chronon.online.fetcher

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.online.serde.AvroConversions
import org.junit.Assert.{assertEquals, assertNotEquals}
import org.scalatest.flatspec.AnyFlatSpec

class JoinRequestKeysTest extends AnyFlatSpec {

  private val groupBy = Builders.GroupBy(
    metaData = Builders.MetaData(name = "unit_test.query_group_by"),
    keyColumns = Seq("query_normalized")
  )

  private def join(selectExpr: String, setups: Seq[String] = Seq.empty): Join =
    Builders.Join(
      metaData = Builders.MetaData(name = "unit_test.query_join"),
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Map("query_normalized" -> selectExpr),
          setups = setups
        ),
        table = "unit_test.queries"
      ),
      joinParts = Seq(
        Builders.JoinPart(
          groupBy = groupBy,
          keyMapping = Map("query_normalized" -> "query_normalized")
        ))
    )

  private def servingInfo(inputSchema: StructType): GroupByServingInfoParsed = {
    val groupByServingInfo = new GroupByServingInfo()
    groupByServingInfo.setGroupBy(groupBy)
    groupByServingInfo.setKeyAvroSchema(
      AvroConversions.fromChrononSchema(StructType("Key", Array(StructField("query_normalized", StringType)))).toString)
    groupByServingInfo.setInputAvroSchema(AvroConversions.fromChrononSchema(inputSchema).toString)
    new GroupByServingInfoParsed(groupByServingInfo)
  }

  it should "include join left selects in cached key mapping keys" in {
    val firstJoin = join("lower(query)")
    val secondJoin = join("trim(lower(query))")

    assertNotEquals(
      JoinRequestKeys.partKey(firstJoin, firstJoin.joinPartOps.head),
      JoinRequestKeys.partKey(secondJoin, secondJoin.joinPartOps.head)
    )
  }

  it should "include join left setups in part keys" in {
    val firstJoin = join("normalize(query)", Seq("CREATE TEMPORARY FUNCTION normalize AS 'first.Normalize'"))
    val secondJoin = join("normalize(query)", Seq("CREATE TEMPORARY FUNCTION normalize AS 'second.Normalize'"))

    assertNotEquals(
      JoinRequestKeys.partKey(firstJoin, firstJoin.joinPartOps.head),
      JoinRequestKeys.partKey(secondJoin, secondJoin.joinPartOps.head)
    )
  }

  it should "include GroupBy serving schema in schema-qualified part keys" in {
    val queryJoin = join("lower(query)")
    val firstServingInfo = servingInfo(StructType("Input", Array(StructField("query", StringType))))
    val secondServingInfo =
      servingInfo(StructType("Input", Array(StructField("query", StringType), StructField("locale", StringType))))

    assertNotEquals(
      JoinRequestKeys.partKey(queryJoin, queryJoin.joinPartOps.head, firstServingInfo),
      JoinRequestKeys.partKey(queryJoin, queryJoin.joinPartOps.head, secondServingInfo)
    )
  }

  it should "keep part keys stable for equivalent select ordering" in {
    def orderedJoin(selects: Map[String, String]): Join =
      Builders.Join(
        left = Builders.Source.events(
          query = Builders.Query(selects = selects),
          table = "unit_test.queries"
        ),
        joinParts = Seq(
          Builders.JoinPart(
            groupBy = groupBy,
            keyMapping = Map("query_normalized" -> "query_normalized")
          ))
      )

    val firstJoin = orderedJoin(Map("query_normalized" -> "lower(query)", "unused" -> "unused"))
    val secondJoin = orderedJoin(Map("unused" -> "unused", "query_normalized" -> "lower(query)"))

    assertEquals(
      JoinRequestKeys.partKey(firstJoin, firstJoin.joinPartOps.head),
      JoinRequestKeys.partKey(secondJoin, secondJoin.joinPartOps.head)
    )
  }

  it should "validate missing request keys against raw selected inputs" in {
    val queryJoin = join("lower(query)")
    val joinPart = queryJoin.joinPartOps.head

    assertEquals(
      Seq("query"),
      JoinRequestKeys.missingRequestKeys(Request(queryJoin.metaData.name, Map.empty), queryJoin, joinPart)
    )
    assertEquals(
      Seq.empty,
      JoinRequestKeys.missingRequestKeys(Request(queryJoin.metaData.name, Map("query" -> "SHOES")), queryJoin, joinPart)
    )
    assertEquals(
      Seq.empty,
      JoinRequestKeys.missingRequestKeys(
        Request(queryJoin.metaData.name, Map("query_normalized" -> "shoes")),
        queryJoin,
        joinPart)
    )
  }

  it should "derive GroupBy keys from raw request keys" in {
    val queryJoin = join("lower(query)")
    val joinPart = queryJoin.joinPartOps.head
    val request = Request(queryJoin.metaData.name, Map("query" -> "SHOES"))
    val keyServingInfo = servingInfo(StructType("Input", Array(StructField("query", StringType))))

    assertEquals(true, JoinRequestKeys.needsDerivation(request, queryJoin, joinPart))
    assertEquals(
      Map("query_normalized" -> "shoes"),
      JoinRequestKeys.deriveLeftKeys(request, queryJoin, joinPart, keyServingInfo)
    )
  }
}
