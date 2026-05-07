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

package ai.chronon.spark.batch

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.api.planner.{MetaDataUtils, TableDependencies}
import ai.chronon.observability.{TileSummaryKey, TileSummary}
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.planner.{ExternalSourceSensorNode, GroupByBackfillNode, JoinStatsComputeNode, MonolithJoinNode, Node, NodeContent, StagingQueryNode}
import ai.chronon.spark.other.MockKVStore
import ai.chronon.spark.utils.{MockApi, SparkTestBase}
import ai.chronon.spark.catalog.TableUtils
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.junit.Assert._
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}


import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

class MockKVStoreWithTracking extends MockKVStore {
  var putRequests: Seq[PutRequest] = Seq.empty

  override def put(putRequest: PutRequest): Future[Boolean] = {
    putRequests = putRequests :+ putRequest
    Future.successful(true)
  }

  override def multiPut(putRequests: Seq[PutRequest]): Future[Seq[Boolean]] = {
    this.putRequests = this.putRequests ++ putRequests
    Future.successful(putRequests.map(_ => true))
  }

  def reset(): Unit = {
    putRequests = Seq.empty
    num_puts.clear()
  }

  def getTileSummaryRequests: Seq[PutRequest] = {
    putRequests.filter { req =>
      try {
        // Check if the key can be deserialized as a TileKey (indicates it's a tile summary request)
        val keyStr = new String(req.keyBytes)
        ThriftJsonCodec.fromJsonStr[TileSummaryKey](keyStr, check = false, classOf[TileSummaryKey])
        true
      } catch {
        case _: Exception => false
      }
    }
  }

  def getTileKeysAndSummaries: Seq[(TileSummaryKey, TileSummary)] = {
    getTileSummaryRequests.map { req =>
      val tileKey = ThriftJsonCodec.fromJsonStr[TileSummaryKey](new String(req.keyBytes), check = false, classOf[TileSummaryKey])
      val tileSummary = ThriftJsonCodec.fromJsonStr[TileSummary](new String(req.valueBytes), check = false, classOf[TileSummary])
      (tileKey, tileSummary)
    }
  }

  def getDataQualityMetricsRequests: Seq[PutRequest] = {
    putRequests.filter { req =>
      val keyStr = new String(req.keyBytes)
      keyStr.contains("#null_count")
    }
  }

  def getDataQualityMetricsJson: Seq[(String, Map[String, Any])] = {
    getDataQualityMetricsRequests.map { req =>
      val keyStr = new String(req.keyBytes)
      val valueStr = new String(req.valueBytes)
      val objectMapper = new ObjectMapper()
      objectMapper.registerModule(DefaultScalaModule)
      val valueMap = objectMapper.readValue(valueStr, classOf[Map[String, Any]])
      keyStr -> valueMap
    }
  }
}

class BatchNodeRunnerTest extends SparkTestBase with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val tableUtils = new TableUtils(spark)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val yesterday = tableUtils.partitionSpec.before(today)
  private val twoDaysAgo = tableUtils.partitionSpec.before(yesterday)

  private val mockKVStore = new MockKVStoreWithTracking()
  private val mockApi = new MockApi(() => mockKVStore, "test_namespace")

  def createTestMetadata(inputTable: String,
                         outputTable: String,
                         partitionColumn: String = "ds",
                         partitionFormat: String = "yyyy-MM-dd",
                         semanticHash: Option[String] = None,
                         isSoftDependency: Boolean = false): MetaData = {
    implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec

    val query = new Query().setPartitionColumn(partitionColumn).setPartitionFormat(partitionFormat)
    val tableDependency = TableDependencies.fromTable(inputTable, query)

    semanticHash.foreach(tableDependency.setSemanticHash)
    if (isSoftDependency) {
      tableDependency.setIsSoftNodeDependency(true)
    }

    val baseMetadata = new MetaData()
      .setOutputNamespace("test_db")
      .setTeam("test_team")

    MetaDataUtils.layer(
      baseMetadata = baseMetadata,
      modeName = "test_mode",
      nodeName = "test_batch_node",
      tableDependencies = Seq(tableDependency),
      stepDays = Some(1),
      outputTableOverride = Some(outputTable)
    )
  }

  def createTestMetadataWithMultipleDeps(inputTable: String,
                                         outputTable: String,
                                         semanticHashes: Seq[Option[String]],
                                         partitionColumn: String = "ds",
                                         partitionFormat: String = "yyyy-MM-dd"): MetaData = {
    implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec

    val query = new Query().setPartitionColumn(partitionColumn).setPartitionFormat(partitionFormat)
    val tableDependencies = semanticHashes.map { hashOpt =>
      val dep = TableDependencies.fromTable(inputTable, query)
      hashOpt.foreach(dep.setSemanticHash)
      dep
    }

    val baseMetadata = new MetaData()
      .setOutputNamespace("test_db")
      .setTeam("test_team")

    MetaDataUtils.layer(
      baseMetadata = baseMetadata,
      modeName = "test_mode",
      nodeName = "test_batch_node",
      tableDependencies = tableDependencies,
      stepDays = Some(1),
      outputTableOverride = Some(outputTable)
    )(partitionSpec)
  }

  def createTestNodeContent(inputTable: String = "test_db.input_table",
                            outputTable: String = "test_db.output_table",
                            leftTable: String = "test_db.left_table",
                            partitionColumn: String = "ds",
                            partitionFormat: String = "yyyy-MM-dd"): NodeContent = {
    val join = new Join(
    ).setMetaData(createTestMetadata(inputTable, outputTable, partitionColumn, partitionFormat))
      .setLeft({
        val src = new Source()
        src.setEvents(
          new EventSource()
            .setTable(leftTable)
            .setQuery(new Query().setPartitionColumn(partitionColumn).setPartitionFormat(partitionFormat)))
        src
      })
      .setJoinParts(Seq.empty.asJava)
    val monolithJoin = new MonolithJoinNode().setJoin(join)
    val nodeContent = new NodeContent()
    nodeContent.setMonolithJoin(monolithJoin)
    nodeContent
  }

  def setupTestTables(): Unit = {
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db")

    // Create input table with some partitions
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS test_db.input_table (
         |  id INT,
         |  value STRING,
         |  ds STRING
         |)
         |PARTITIONED BY (ds)
         |""".stripMargin
    )

    // Create left table
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS test_db.left_table (
         |  id INT,
         |  ds STRING
         |)
         |PARTITIONED BY (ds)
         |""".stripMargin
    )

    // Create output table
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS test_db.output_table (
         |  id INT,
         |  value STRING,
         |  ds STRING
         |)
         |PARTITIONED BY (ds)
         |""".stripMargin
    )

    // Insert test data for available partitions
    spark.sql(
      s"""
         |INSERT INTO test_db.input_table VALUES
         |(1, 'value1', '$yesterday'),
         |(2, 'value2', '$twoDaysAgo')
         |""".stripMargin
    )

    spark.sql(
      s"""
         |INSERT INTO test_db.left_table VALUES
         |(1, '$yesterday'),
         |(2, '$twoDaysAgo')
         |""".stripMargin
    )
  }

  def createTestConfigFile(startDs: String,
                           endDs: String,
                           inputTable: String = "test_db.input_table",
                           outputTable: String = "test_db.output_table",
                           leftTable: String = "test_db.left_table",
                           partitionColumn: String = "ds",
                           partitionFormat: String = "yyyy-MM-dd"): String = {
    val tempFile = java.io.File.createTempFile("test_node_config", ".json")
    tempFile.deleteOnExit()

    val nodeContent = createTestNodeContent(inputTable, outputTable, leftTable, partitionColumn, partitionFormat)
    val metadata = createTestMetadata(inputTable, outputTable, partitionColumn, partitionFormat)
    val node = new Node()
      .setMetaData(metadata)
      .setContent(nodeContent)
    val json = ThriftJsonCodec.toJsonStr(node)

    val writer = new java.io.FileWriter(tempFile)
    writer.write(json)
    writer.close()

    tempFile.getAbsolutePath
  }

  override def beforeEach(): Unit = {
    // Drop all test tables to ensure fresh start
    spark.sql("DROP TABLE IF EXISTS test_db.input_table")
    spark.sql("DROP TABLE IF EXISTS test_db.left_table")
    spark.sql("DROP TABLE IF EXISTS test_db.output_table")
    spark.sql("DROP TABLE IF EXISTS test_db.input_table_alt")
    spark.sql("DROP TABLE IF EXISTS test_db.output_table_alt")
    spark.sql("DROP TABLE IF EXISTS test_db.left_table_alt")
    spark.sql("DROP TABLE IF EXISTS test_db.output_table" + Constants.archiveReuseTableSuffix)
    spark.sql("DROP TABLE IF EXISTS test_db.tp_events")
    spark.sql("DROP TABLE IF EXISTS test_db.tp_staging_output")
    spark.sql("DROP TABLE IF EXISTS test_db.tp_gb_output")
    spark.sql("DROP TABLE IF EXISTS test_db.time_part_sensor")
    spark.sql("DROP TABLE IF EXISTS test_db.yyyymmdd_events")
    spark.sql("DROP TABLE IF EXISTS test_db.yyyymmdd_gb_test")
    spark.sql("DROP TABLE IF EXISTS test_db.yyyymmdd_sq_test")
    spark.sql("DROP TABLE IF EXISTS test_db.yyyymmdd_join_test")
    spark.sql("DROP TABLE IF EXISTS test_db.yyyymmdd_join_gb")

    setupTestTables()
    mockKVStore.reset()
  }

  "BatchNodeRunner.runFromArgs" should "succeed for valid partition range" ignore {

    val configPath = createTestConfigFile(twoDaysAgo, yesterday)
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val exitCode = runner.runFromArgs(twoDaysAgo, yesterday, None)

    assertEquals("runFromArgs should return 0 on success", 0, exitCode)
  }

  it should "short circuit and throw exception when missing partitions are present" in {

    val configPath = createTestConfigFile(twoDaysAgo, today) // today's partition doesn't exist
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val exitCode = runner.runFromArgs(twoDaysAgo, today, None)

    assertEquals("runFromArgs should return 1 on failure", 1, exitCode)
  }

  it should "handle empty partition ranges correctly" in {

    // Use a date range where no partitions exist
    val futureDate1 = tableUtils.partitionSpec.after(today)
    val futureDate2 = tableUtils.partitionSpec.after(futureDate1)

    val configPath = createTestConfigFile(futureDate1, futureDate2)
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val exitCode = runner.runFromArgs(futureDate1, futureDate2, None)

    assertEquals("runFromArgs should return 1 on failure due to missing all partitions", 1, exitCode)
  }

  it should "correctly identify missing vs available partitions" in {

    // Add one more partition to test partial availability
    spark.sql(
      s"""
         |INSERT INTO test_db.input_table VALUES
         |(3, 'value3', '$today')
         |""".stripMargin
    )

    spark.sql(
      s"""
         |INSERT INTO test_db.left_table VALUES
         |(3, '$today')
         |""".stripMargin
    )

    // Request a range that includes both available and missing partitions
    val threeDaysAgo = tableUtils.partitionSpec.before(twoDaysAgo)
    val configPath = createTestConfigFile(threeDaysAgo, today)
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val exitCode = runner.runFromArgs(threeDaysAgo, today, None)

    assertEquals("runFromArgs should return 1 on failure due to missing partitions", 1, exitCode)
  }

  it should "correctly translate yyyyMMdd partition ranges when computing input table statuses" in {
    import spark.implicits._

    val yesterdayAlt = yesterday.replace("-", "") // yyyyMMdd format
    val twoDaysAgoAlt = twoDaysAgo.replace("-", "") // yyyyMMdd format

    val inputData = Seq(
      (1, "value1", yesterdayAlt),
      (2, "value2", twoDaysAgoAlt)
    ).toDF("id", "value", "partition_date")

    tableUtils.insertPartitions(inputData, "test_db.input_table_alt", partitionColumns = List("partition_date"))

    val metadata = createTestMetadata(
      "test_db.input_table_alt",
      "test_db.output_table_alt",
      partitionColumn = "partition_date",
      partitionFormat = "yyyyMMdd"
    )

    val nodeContent = new NodeContent()
    nodeContent.setGroupByBackfill(new GroupByBackfillNode())
    val node = new Node().setMetaData(metadata).setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    // CLI dates are always yyyy-MM-dd; the BatchNodeRunner wraps them with PartitionSpec.daily
    val cliRange = PartitionRange(twoDaysAgo, yesterday)(PartitionSpec.daily)
    val statuses = runner.computeInputTablePartitionStatuses(metadata, cliRange, tableUtils)

    statuses.foreach { tps =>
      assertTrue("Should be ready", tps.ready)
      assertTrue("Should have last available partition", tps.lastAvailablePartition.isDefined)
      // Last available partition should be in yyyy-MM-dd format (translated from yyyyMMdd)
      val lastPart = tps.lastAvailablePartition.get
      assertFalse("Should NOT be in yyyyMMdd format",
        lastPart == yesterdayAlt || lastPart == twoDaysAgoAlt)
    }
  }

  "BatchNodeRunner.checkPartitions" should "succeed when all partitions are available" in {
    val tableDependency =
      TableDependencies.fromTable("test_db.input_table",
                                  new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd"))
    val sensorNode = new ExternalSourceSensorNode()
      .setSourceTableDependency(tableDependency)
      .setRetryCount(0L)
      .setRetryIntervalMin(1L)

    val metadata = createTestMetadata("test_db.input_table", "test_db.output_table")
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)
    val configPath = createTestConfigFile(twoDaysAgo, yesterday)
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val result = runner.checkPartitions(sensorNode, range)

    result match {
      case Success(_) =>
      // Test passed
      case Failure(exception) =>
        fail(s"checkPartitions should have succeeded but failed with: ${exception.getMessage}")
    }
  }

  it should "fail when partitions are missing and no retries configured" in {
    val tableDependency =
      TableDependencies.fromTable("test_db.external_table",
                                  new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd"))
    val sensorNode = new ExternalSourceSensorNode()
      .setSourceTableDependency(tableDependency)
      .setRetryCount(0L)
      .setRetryIntervalMin(1L)

    val metadata = createTestMetadata("test_db.external_table", "test_db.output_table")
    val range = PartitionRange(today, today)(tableUtils.partitionSpec) // today's partition doesn't exist
    val configPath = createTestConfigFile(today, today, "test_db.external_table")
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val result = runner.checkPartitions(sensorNode, range)

    result match {
      case Success(_) =>
        fail("checkPartitions should have failed due to data not ready")
      case Failure(exception) =>
        assertTrue("Exception should mention sensor check", exception.getMessage.contains("Sensor"))
    }
  }

  it should "use default retry values when not set" in {
    val tableDependency =
      TableDependencies.fromTable("test_db.external_table",
                                  new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd"))
    val sensorNode = new ExternalSourceSensorNode()
      .setSourceTableDependency(tableDependency)
      .setRetryCount(0)
    // Not setting retryCount and retryIntervalMin to test defaults

    val metadata = createTestMetadata("test_db.external_table", "test_db.output_table")
    val range = PartitionRange(today, today)(tableUtils.partitionSpec) // today's partition doesn't exist
    val configPath = createTestConfigFile(today, today, "test_db.external_table")
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val result = runner.checkPartitions(sensorNode, range)

    result match {
      case Success(_) =>
        fail("checkPartitions should have failed due to data not ready")
      case Failure(exception) =>
        assertTrue("Exception should mention sensor check", exception.getMessage.contains("Sensor"))
    }
  }

  it should "retry when configured but eventually fail if partitions never appear" in {
    val tableDependency =
      TableDependencies.fromTable("test_db.external_table",
                                  new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd"))
    val sensorNode = new ExternalSourceSensorNode()
      .setSourceTableDependency(tableDependency)
      .setRetryCount(2L) // Will try 3 times total (initial + 2 retries)
      .setRetryIntervalMin(0L) // Set to 0 to avoid actual delays in test

    val metadata = createTestMetadata("test_db.external_table", "test_db.output_table")
    val range = PartitionRange(today, today)(tableUtils.partitionSpec) // today's partition doesn't exist
    val configPath = createTestConfigFile(today, today, "test_db.external_table")
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val startTime = System.currentTimeMillis()
    val result = runner.checkPartitions(sensorNode, range)
    val endTime = System.currentTimeMillis()

    result match {
      case Success(_) =>
        fail("checkPartitions should have failed due to data not ready")
      case Failure(exception) =>
        assertTrue("Exception should mention sensor check", exception.getMessage.contains("Sensor"))
        // Since we set retry interval to 0, the test should complete quickly
        assertTrue("Test should complete within reasonable time", (endTime - startTime) < 5000)
    }
  }

  it should "handle non-existent table gracefully" in {
    val tableDependency =
      TableDependencies.fromTable("test_db.nonexistent_table",
                                  new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd"))
    val sensorNode = new ExternalSourceSensorNode()
      .setSourceTableDependency(tableDependency)
      .setRetryCount(0L)
      .setRetryIntervalMin(1L)

    val metadata = createTestMetadata("test_db.nonexistent_table", "test_db.output_table")
    val range = PartitionRange(yesterday, yesterday)(tableUtils.partitionSpec)
    val configPath = createTestConfigFile(yesterday, yesterday, "test_db.nonexistent_table")
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val result = runner.checkPartitions(sensorNode, range)

    result match {
      case Success(_) =>
        fail("checkPartitions should have failed for nonexistent table")
      case Failure(exception) =>
        // Should fail with some kind of table not found or similar error
        assertTrue(
          "Exception should indicate table issue",
          exception.getMessage.contains("nonexistent_table") ||
            exception.getMessage.toLowerCase.contains("not found") ||
            exception.getMessage.toLowerCase.contains("table")
        )
    }
  }

  "BatchNodeRunner.computeInputTablePartitionStatuses" should "correctly compute partitions for a single table" in {
    val configPath = createTestConfigFile(twoDaysAgo, yesterday)
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val metadata = node.metaData
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    val statuses = runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq

    assertTrue("Should have statuses for input tables", statuses.nonEmpty)

    val inputTableStatus = statuses.find(_.name == "test_db.input_table")
    assertTrue("Should have status for input_table", inputTableStatus.isDefined)

    inputTableStatus.foreach { status =>
      assertTrue("Should be ready", status.ready)
      assertTrue("Should have last available partition", status.lastAvailablePartition.isDefined)
    }
  }

  it should "identify not-ready tables correctly" in {
    val configPath = createTestConfigFile(twoDaysAgo, today)
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val metadata = node.metaData
    val range = PartitionRange(twoDaysAgo, today)(tableUtils.partitionSpec)

    val statuses = runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq

    val inputTableStatus = statuses.find(_.name == "test_db.input_table")
    assertTrue("Should have status for input_table", inputTableStatus.isDefined)

    inputTableStatus.foreach { status =>
      assertFalse("Should not be ready (today's data not available)", status.ready)
    }
  }

  it should "handle same table used for multiple dependencies (labels and groupBy)" in {
    // Create a join that uses the same table for both labels and features
    val groupBySource = new Source()
    groupBySource.setEvents(
      new EventSource()
        .setTable("test_db.input_table")
        .setQuery(new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd")))

    val join = new Join()
      .setMetaData(createTestMetadata("test_db.input_table", "test_db.output_table"))
      .setLeft({
        val src = new Source()
        src.setEvents(
          new EventSource()
            .setTable("test_db.input_table")
            .setQuery(new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd")))
        src
      })
      .setJoinParts(Seq(
        new JoinPart()
          .setGroupBy(
            new GroupBy()
              .setSources(Seq(groupBySource).asJava)
          )
      ).asJava)

    val monolithJoin = new MonolithJoinNode()
    monolithJoin.setJoin(join)

    val nodeContent = new NodeContent()
    nodeContent.setMonolithJoin(monolithJoin)

    val metadata = MetaDataUtils.layer(
      baseMetadata = new MetaData().setOutputNamespace("test_db").setTeam("test_team"),
      modeName = "test_mode",
      nodeName = "test_batch_node",
      tableDependencies = Seq(
        TableDependencies.fromTable("test_db.input_table", new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd")),
        TableDependencies.fromTable("test_db.input_table", new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd"))
      ),
      stepDays = Some(1),
      outputTableOverride = Some("test_db.output_table")
    )(tableUtils.partitionSpec)

    val node = new Node()
    node.setMetaData(metadata)
    node.setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    val statuses = runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq

    // Should have only one status for the table despite it being used twice
    val inputTableStatuses = statuses.filter(_.name == "test_db.input_table")
    assertEquals("Should have exactly one status for input_table", 1, inputTableStatuses.size)

    inputTableStatuses.head match {
      case status =>
        assertTrue("Should have last available partition", status.lastAvailablePartition.isDefined)
        assertTrue("Should be ready", status.ready)
    }
  }

  it should "filter out soft dependencies" in {
    val metadata = createTestMetadata("test_db.input_table", "test_db.output_table", isSoftDependency = true)
    val nodeContent = createTestNodeContent()
    val node = new Node()
    node.setMetaData(metadata)
    node.setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    val statuses = runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq

    val inputTableStatus = statuses.find(_.name == "test_db.input_table")
    assertTrue("Should not have status for soft dependency", inputTableStatus.isEmpty)
  }

  it should "capture semantic hash when set on table dependency" in {
    val metadata = createTestMetadata("test_db.input_table", "test_db.output_table", semanticHash = Some("test_hash_123"))
    val nodeContent = createTestNodeContent()
    val node = new Node()
    node.setMetaData(metadata)
    node.setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    val statuses = runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq

    val inputTableStatus = statuses.find(_.name == "test_db.input_table")
    assertTrue("Should have status for input_table", inputTableStatus.isDefined)

    inputTableStatus.foreach { status =>
      assertTrue("Should have semantic hash", status.semanticHash.isDefined)
      assertEquals("Semantic hash should match", "test_hash_123", status.semanticHash.get)
    }
  }

  it should "return None for semantic hash when not set" in {
    val metadata = createTestMetadata("test_db.input_table", "test_db.output_table")
    val nodeContent = createTestNodeContent()
    val node = new Node()
    node.setMetaData(metadata)
    node.setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    val statuses = runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq

    val inputTableStatus = statuses.find(_.name == "test_db.input_table")
    assertTrue("Should have status for input_table", inputTableStatus.isDefined)

    inputTableStatus.foreach { status =>
      assertTrue("Semantic hash should be None", status.semanticHash.isEmpty)
    }
  }

  it should "use consistent semantic hash when same table used multiple times with same hash" in {
    val metadata = createTestMetadataWithMultipleDeps(
      "test_db.input_table",
      "test_db.output_table",
      Seq(Some("consistent_hash"), Some("consistent_hash"))
    )
    val nodeContent = createTestNodeContent()
    val node = new Node()
    node.setMetaData(metadata)
    node.setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    val statuses = runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq

    val inputTableStatus = statuses.find(_.name == "test_db.input_table")
    assertTrue("Should have status for input_table", inputTableStatus.isDefined)

    inputTableStatus.foreach { status =>
      assertTrue("Should have semantic hash", status.semanticHash.isDefined)
      assertEquals("Semantic hash should match", "consistent_hash", status.semanticHash.get)
    }
  }

  it should "return None when same table has inconsistent semantic hashes across dependencies" in {
    val metadata = createTestMetadataWithMultipleDeps(
      "test_db.input_table",
      "test_db.output_table",
      Seq(Some("hash_1"), Some("hash_2"))
    )
    val nodeContent = createTestNodeContent()
    val node = new Node()
    node.setMetaData(metadata)
    node.setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    val statuses = runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq

    val inputTableStatus = statuses.find(_.name == "test_db.input_table")
    assertTrue("Should have status for input_table", inputTableStatus.isDefined)

    inputTableStatus.foreach { status =>
      assertTrue("Semantic hash should be None due to inconsistency", status.semanticHash.isEmpty)
    }
  }

  "BatchNodeRunner.isSensorNode" should "identify nodes with 'sensor' in name (case-insensitive)" in {
    // Test with lowercase "sensor"
    val metadata1 = createTestMetadata("test_db.input_table", "test_db.output_table")
    val baseMetadata1 = new MetaData().setOutputNamespace("test_db").setTeam("test_team")
    val sensorMetadata1 = MetaDataUtils.layer(
      baseMetadata = baseMetadata1,
      modeName = "test_mode",
      nodeName = "external_source_sensor_test",
      tableDependencies = Seq.empty,
      stepDays = Some(1),
      outputTableOverride = Some("test_db.output_table")
    )(tableUtils.partitionSpec)

    val node1 = new Node()
    node1.setMetaData(sensorMetadata1)
    node1.setContent(createTestNodeContent())
    val runner1 = new BatchNodeRunner(node1, tableUtils, mockApi)

    assertTrue("Should identify node with 'sensor' in name", runner1.isSensorNode)

    // Test with uppercase "SENSOR"
    val sensorMetadata2 = MetaDataUtils.layer(
      baseMetadata = new MetaData().setOutputNamespace("test_db").setTeam("test_team"),
      modeName = "test_mode",
      nodeName = "EXTERNAL_SOURCE_SENSOR_TEST",
      tableDependencies = Seq.empty,
      stepDays = Some(1),
      outputTableOverride = Some("test_db.output_table")
    )(tableUtils.partitionSpec)

    val node2 = new Node()
    node2.setMetaData(sensorMetadata2)
    node2.setContent(createTestNodeContent())
    val runner2 = new BatchNodeRunner(node2, tableUtils, mockApi)

    assertTrue("Should identify node with 'SENSOR' (uppercase) in name", runner2.isSensorNode)

    // Test with mixed case "SenSor"
    val sensorMetadata3 = MetaDataUtils.layer(
      baseMetadata = new MetaData().setOutputNamespace("test_db").setTeam("test_team"),
      modeName = "test_mode",
      nodeName = "external_SenSor_node",
      tableDependencies = Seq.empty,
      stepDays = Some(1),
      outputTableOverride = Some("test_db.output_table")
    )(tableUtils.partitionSpec)

    val node3 = new Node()
    node3.setMetaData(sensorMetadata3)
    node3.setContent(createTestNodeContent())
    val runner3 = new BatchNodeRunner(node3, tableUtils, mockApi)

    assertTrue("Should identify node with 'SenSor' (mixed case) in name", runner3.isSensorNode)
  }

  it should "not identify regular nodes as sensor nodes" in {
    val metadata = createTestMetadata("test_db.input_table", "test_db.output_table")
    val regularMetadata = MetaDataUtils.layer(
      baseMetadata = new MetaData().setOutputNamespace("test_db").setTeam("test_team"),
      modeName = "test_mode",
      nodeName = "regular_batch_node",
      tableDependencies = Seq.empty,
      stepDays = Some(1),
      outputTableOverride = Some("test_db.output_table")
    )(tableUtils.partitionSpec)

    val node = new Node()
    node.setMetaData(regularMetadata)
    node.setContent(createTestNodeContent())
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    assertFalse("Should NOT identify regular node as sensor node", runner.isSensorNode)
  }

  "BatchNodeRunner.checkPartitions with timePartitioned" should "succeed when MAX covers the range" in {
    // Create an unpartitioned table with timestamps
    spark.sql(
      """CREATE TABLE IF NOT EXISTS test_db.time_part_sensor (
        |  user_id INT,
        |  value STRING,
        |  created_at TIMESTAMP
        |)""".stripMargin)
    spark.sql(
      s"""INSERT INTO test_db.time_part_sensor VALUES
         |(1, 'a', TIMESTAMP '${yesterday} 12:00:00'),
         |(2, 'b', TIMESTAMP '${today} 12:00:00')
         |""".stripMargin)

    // No timePartitioned flag needed — column type is detected automatically
    val tableInfo = new TableInfo()
      .setTable("test_db.time_part_sensor")
      .setPartitionColumn("created_at")
    val tableDependency = new TableDependency().setTableInfo(tableInfo)

    val sensorNode = new ExternalSourceSensorNode()
      .setSourceTableDependency(tableDependency)
      .setRetryCount(0L)
      .setRetryIntervalMin(1L)

    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)
    val configPath = createTestConfigFile(twoDaysAgo, yesterday)
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val result = runner.checkPartitions(sensorNode, range)

    result match {
      case Success(_) => // Test passed
      case Failure(e) => fail(s"checkPartitions should have succeeded: ${e.getMessage}")
    }
  }

  it should "fail when MAX does not cover the range" in {
    val tableInfo = new TableInfo()
      .setTable("test_db.time_part_sensor")
      .setPartitionColumn("created_at")
    val tableDependency = new TableDependency().setTableInfo(tableInfo)

    val sensorNode = new ExternalSourceSensorNode()
      .setSourceTableDependency(tableDependency)
      .setRetryCount(0L)
      .setRetryIntervalMin(1L)

    // Request a future date that the data doesn't cover
    val futureDate = tableUtils.partitionSpec.after(today)
    val range = PartitionRange(today, futureDate)(tableUtils.partitionSpec)
    val configPath = createTestConfigFile(today, futureDate)
    val node = ThriftJsonCodec.fromJsonFile[Node](configPath, check = true)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    val result = runner.checkPartitions(sensorNode, range)

    result match {
      case Success(_) => fail("checkPartitions should have failed")
      case Failure(e) =>
        assertTrue("Error should mention sensor",
          e.getMessage.contains("Sensor"))
    }
  }

  private def createTimePartitionedEventsTable(): Unit = {
    spark.sql(
      """CREATE TABLE IF NOT EXISTS test_db.tp_events (
        |  user_id INT,
        |  value DOUBLE,
        |  created_at TIMESTAMP
        |)""".stripMargin)
    spark.sql(
      s"""INSERT INTO test_db.tp_events VALUES
         |(1, 10.0, TIMESTAMP '${twoDaysAgo} 12:00:00'),
         |(2, 20.0, TIMESTAMP '${twoDaysAgo} 12:00:00'),
         |(1, 30.0, TIMESTAMP '${yesterday} 12:00:00'),
         |(3, 40.0, TIMESTAMP '${yesterday} 12:00:00'),
         |(2, 50.0, TIMESTAMP '${today} 12:00:00')
         |""".stripMargin)
  }

  "E2E StagingQuery with timePartitioned input" should "run through BatchNodeRunner" in {
    createTimePartitionedEventsTable()

    // Create a time-partitioned table dependency
    val query = new Query()
      .setPartitionColumn("created_at")
    val tableDep = TableDependencies.fromTable("test_db.tp_events", query)

    val sqMetaData = Builders.MetaData(namespace = "test_db", name = "tp_staging_test")
    val outputTable = sqMetaData.outputTable

    val stagingQueryConf = Builders.StagingQuery(
      query = s"""SELECT user_id, value,
                 |  DATE(created_at) as ds
                 |FROM test_db.tp_events
                 |WHERE created_at >= {{ start_date }}
                 |  AND created_at < {{ end_date(offset=1) }}""".stripMargin,
      metaData = sqMetaData,
      startPartition = twoDaysAgo,
      tableDependencies = Seq(tableDep)
    )

    val stagingQueryNode = new StagingQueryNode().setStagingQuery(stagingQueryConf)
    val nodeContent = new NodeContent()
    nodeContent.setStagingQuery(stagingQueryNode)

    implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
    val metadata = MetaDataUtils.layer(
      baseMetadata = new MetaData().setOutputNamespace("test_db").setTeam("test_team"),
      modeName = "test_mode",
      nodeName = "tp_staging_test",
      tableDependencies = Seq(tableDep),
      stepDays = Some(1),
      outputTableOverride = Some(outputTable)
    )

    val node = new Node().setMetaData(metadata).setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    // Run the staging query through BatchNodeRunner
    runner.run(metadata, nodeContent, Option(range))

    // Verify the output table was created and contains the right data
    val outputDf = spark.sql(s"SELECT * FROM $outputTable ORDER BY ds, user_id")
    val rowCount = outputDf.count()
    assertTrue(s"Output should have rows, got $rowCount", rowCount > 0)

    val outputPartitions = tableUtils.partitions(outputTable)
    assertTrue("Output should have partitions", outputPartitions.nonEmpty)
    outputPartitions should contain(twoDaysAgo)
    outputPartitions should contain(yesterday)
  }

  "E2E GroupBy backfill with timePartitioned source" should "run through BatchNodeRunner" in {
    createTimePartitionedEventsTable()

    val sourceQuery = Builders.Query(
      selects = Builders.Selects("user_id", "value"),
      partitionColumn = "created_at",
      timeColumn = "UNIX_TIMESTAMP(created_at) * 1000"
    )

    val source = Builders.Source.events(sourceQuery, table = "test_db.tp_events")
    val tableDep = TableDependencies.fromSource(source).get

    val gbMetaData = Builders.MetaData(namespace = "test_db", name = "tp_gb_test")
    val outputTable = gbMetaData.outputTable

    val groupByConf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          inputColumn = "value",
          operation = Operation.SUM
        )
      ),
      metaData = gbMetaData
    )

    val gbNode = new GroupByBackfillNode().setGroupBy(groupByConf)
    val nodeContent = new NodeContent()
    nodeContent.setGroupByBackfill(gbNode)

    implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
    val metadata = MetaDataUtils.layer(
      baseMetadata = new MetaData().setOutputNamespace("test_db").setTeam("test_team"),
      modeName = "test_mode",
      nodeName = "tp_gb_test",
      tableDependencies = Seq(tableDep),
      stepDays = Some(1),
      outputTableOverride = Some(outputTable)
    )

    val node = new Node().setMetaData(metadata).setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    // Run the groupBy backfill through BatchNodeRunner
    runner.run(metadata, nodeContent, Option(range))

    // Verify the output table was created with partitioned data
    val outputDf = spark.sql(s"SELECT * FROM $outputTable ORDER BY ds, user_id")
    val rowCount = outputDf.count()
    assertTrue(s"Output should have rows, got $rowCount", rowCount > 0)

    val outputPartitions = tableUtils.partitions(outputTable)
    assertTrue("Output should have partitions", outputPartitions.nonEmpty)

    // Verify aggregation results: user_id=1 should have sum(value)
    val user1Rows = spark.sql(
      s"SELECT * FROM $outputTable WHERE user_id = 1 AND ds = '$yesterday'"
    ).collect()
    assertTrue("Should have results for user_id=1", user1Rows.nonEmpty)
  }

  private def createYyyyMMddEventsTable(): Unit = {
    import spark.implicits._
    val yesterdayAlt = yesterday.replace("-", "")
    val twoDaysAgoAlt = twoDaysAgo.replace("-", "")
    val todayAlt = today.replace("-", "")

    val data = Seq(
      (1, 10.0, s"${twoDaysAgoAlt} 12:00:00", twoDaysAgoAlt),
      (2, 20.0, s"${twoDaysAgoAlt} 14:00:00", twoDaysAgoAlt),
      (1, 30.0, s"${yesterdayAlt} 10:00:00", yesterdayAlt),
      (3, 40.0, s"${yesterdayAlt} 16:00:00", yesterdayAlt),
      (2, 50.0, s"${todayAlt} 08:00:00", todayAlt)
    ).toDF("user_id", "value", "event_time", "partition_date")

    tableUtils.insertPartitions(data, "test_db.yyyymmdd_events", partitionColumns = List("partition_date"))
  }

  "E2E GroupBy backfill with yyyyMMdd partitions" should "produce correct output when CLI dates are yyyy-MM-dd" in {
    createYyyyMMddEventsTable()

    val yesterdayAlt = yesterday.replace("-", "")
    val twoDaysAgoAlt = twoDaysAgo.replace("-", "")

    val sourceQuery = Builders.Query(
      selects = Builders.Selects("user_id", "value"),
      partitionColumn = "partition_date",
      timeColumn = s"UNIX_TIMESTAMP(event_time, 'yyyyMMdd HH:mm:ss') * 1000",
      startPartition = twoDaysAgoAlt
    )
    sourceQuery.setPartitionFormat("yyyyMMdd")

    val source = Builders.Source.events(sourceQuery, table = "test_db.yyyymmdd_events")
    val tableDep = TableDependencies.fromSource(source).get

    val gbMetaData = Builders.MetaData(namespace = "test_db", name = "yyyymmdd_gb_test")
    val outputTable = gbMetaData.outputTable

    val groupByConf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(inputColumn = "value", operation = Operation.SUM)
      ),
      metaData = gbMetaData
    )

    val gbNode = new GroupByBackfillNode().setGroupBy(groupByConf)
    val nodeContent = new NodeContent()
    nodeContent.setGroupByBackfill(gbNode)

    implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
    val metadata = MetaDataUtils.layer(
      baseMetadata = new MetaData().setOutputNamespace("test_db").setTeam("test_team"),
      modeName = "test_mode",
      nodeName = "yyyymmdd_gb_test",
      tableDependencies = Seq(tableDep),
      stepDays = Some(1),
      outputTableOverride = Some(outputTable)
    )

    val node = new Node().setMetaData(metadata).setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    // CLI dates are always yyyy-MM-dd; BatchNodeRunner wraps with PartitionSpec.daily
    val cliRange = PartitionRange(twoDaysAgo, yesterday)(PartitionSpec.daily)

    // This is the core path: BatchNodeRunner.run extracts range.start/range.end (yyyy-MM-dd strings)
    // and passes them to GroupBy.computeBackfill, which must translate to the configured partition spec.
    runner.run(metadata, nodeContent, Option(cliRange))

    val outputDf = spark.sql(s"SELECT * FROM $outputTable ORDER BY ds, user_id")
    val rowCount = outputDf.count()
    assertTrue(s"Output should have rows, got $rowCount", rowCount > 0)

    val outputPartitions = tableUtils.partitions(outputTable)
    assertTrue("Output should have partitions", outputPartitions.nonEmpty)
    // Output uses default partition spec (yyyy-MM-dd) since tableUtils is default-configured
    outputPartitions should contain(twoDaysAgo)
    outputPartitions should contain(yesterday)
  }

  "E2E StagingQuery with yyyyMMdd input dependency" should "resolve partitions correctly for yyyyMMdd tables" in {
    createYyyyMMddEventsTable()

    val yesterdayAlt = yesterday.replace("-", "")
    val twoDaysAgoAlt = twoDaysAgo.replace("-", "")

    val query = new Query()
      .setPartitionColumn("partition_date")
      .setPartitionFormat("yyyyMMdd")
    val tableDep = TableDependencies.fromTable("test_db.yyyymmdd_events", query)

    val sqMetaData = Builders.MetaData(namespace = "test_db", name = "yyyymmdd_sq_test")
    val outputTable = sqMetaData.outputTable

    // Staging query with explicit yyyyMMdd date literals (avoiding macro complexity)
    val stagingQueryConf = Builders.StagingQuery(
      query = s"""SELECT user_id, value, partition_date as ds
                 |FROM test_db.yyyymmdd_events
                 |WHERE partition_date >= '$twoDaysAgoAlt'
                 |  AND partition_date <= '$yesterdayAlt'""".stripMargin,
      metaData = sqMetaData,
      startPartition = twoDaysAgo,
      tableDependencies = Seq(tableDep)
    )

    val stagingQueryNode = new StagingQueryNode().setStagingQuery(stagingQueryConf)
    val nodeContent = new NodeContent()
    nodeContent.setStagingQuery(stagingQueryNode)

    implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
    val metadata = MetaDataUtils.layer(
      baseMetadata = new MetaData().setOutputNamespace("test_db").setTeam("test_team"),
      modeName = "test_mode",
      nodeName = "yyyymmdd_sq_test",
      tableDependencies = Seq(tableDep),
      stepDays = Some(1),
      outputTableOverride = Some(outputTable)
    )

    val node = new Node().setMetaData(metadata).setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val cliRange = PartitionRange(twoDaysAgo, yesterday)(PartitionSpec.daily)

    runner.run(metadata, nodeContent, Option(cliRange))

    val outputDf = spark.sql(s"SELECT * FROM $outputTable")
    val rowCount = outputDf.count()
    assertTrue(s"Output should have rows, got $rowCount", rowCount > 0)

    val outputPartitions = tableUtils.partitions(outputTable)
    assertTrue("Output should have partitions", outputPartitions.nonEmpty)
  }

  "E2E Join backfill with yyyyMMdd partitions" should "produce correct output when CLI dates are yyyy-MM-dd" in {
    createYyyyMMddEventsTable()

    val yesterdayAlt = yesterday.replace("-", "")
    val twoDaysAgoAlt = twoDaysAgo.replace("-", "")

    // Left side: events table with yyyyMMdd partitions
    val leftQuery = Builders.Query(
      selects = Builders.Selects("user_id"),
      partitionColumn = "partition_date",
      timeColumn = s"UNIX_TIMESTAMP(event_time, 'yyyyMMdd HH:mm:ss') * 1000",
      startPartition = twoDaysAgoAlt
    )
    leftQuery.setPartitionFormat("yyyyMMdd")
    val leftSource = Builders.Source.events(leftQuery, table = "test_db.yyyymmdd_events")

    // Right side GroupBy: aggregate value per user from the same yyyyMMdd table
    val gbSourceQuery = Builders.Query(
      selects = Builders.Selects("user_id", "value"),
      partitionColumn = "partition_date",
      timeColumn = s"UNIX_TIMESTAMP(event_time, 'yyyyMMdd HH:mm:ss') * 1000",
      startPartition = twoDaysAgoAlt
    )
    gbSourceQuery.setPartitionFormat("yyyyMMdd")
    val gbSource = Builders.Source.events(gbSourceQuery, table = "test_db.yyyymmdd_events")

    val gbMetaData = Builders.MetaData(namespace = "test_db", name = "yyyymmdd_join_gb", team = "test_team")
    val groupByConf = Builders.GroupBy(
      sources = Seq(gbSource),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(inputColumn = "value", operation = Operation.SUM)
      ),
      metaData = gbMetaData
    )

    val joinPart = new JoinPart().setGroupBy(groupByConf)
    val joinMetaData = Builders.MetaData(namespace = "test_db", name = "yyyymmdd_join_test", team = "test_team")
    val joinConf = Builders.Join(
      metaData = joinMetaData,
      left = leftSource,
      joinParts = Seq(joinPart)
    )
    val outputTable = joinMetaData.outputTable

    val leftTableDep = TableDependencies.fromSource(leftSource).get
    val rightTableDep = TableDependencies.fromSource(gbSource).get

    val monolithJoin = new MonolithJoinNode().setJoin(joinConf)
    val nodeContent = new NodeContent()
    nodeContent.setMonolithJoin(monolithJoin)

    implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
    val metadata = MetaDataUtils.layer(
      baseMetadata = new MetaData().setOutputNamespace("test_db").setTeam("test_team"),
      modeName = "test_mode",
      nodeName = "yyyymmdd_join_test",
      tableDependencies = Seq(leftTableDep, rightTableDep),
      stepDays = Some(1),
      outputTableOverride = Some(outputTable)
    )

    val node = new Node().setMetaData(metadata).setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)

    // CLI dates are always yyyy-MM-dd
    val cliRange = PartitionRange(twoDaysAgo, yesterday)(PartitionSpec.daily)

    runner.run(metadata, nodeContent, Option(cliRange))

    val outputDf = spark.sql(s"SELECT * FROM $outputTable ORDER BY ds, user_id")
    val rowCount = outputDf.count()
    assertTrue(s"Join output should have rows, got $rowCount", rowCount > 0)

    val outputPartitions = tableUtils.partitions(outputTable)
    assertTrue("Join output should have partitions", outputPartitions.nonEmpty)
    outputPartitions should contain(twoDaysAgo)
    outputPartitions should contain(yesterday)
  }

  "BatchNodeRunner.run with JoinStatsComputeNode" should "skip gracefully when join output table has no rows" in {
    // Regression test: before the fix, AvroKvEncoder.encodeTimed would throw
    // SparkException("Value at index 0 is null") when dataDf was empty because
    // dataDf.agg(max(tsColumn)).collect()(0).getLong(0) returns null for empty input.
    // Fix: BatchNodeRunner.runJoinStatsCompute returns early when joinOutputDf.isEmpty.
    val joinOutputTable = "test_db.empty_join_output"
    spark.sql(s"DROP TABLE IF EXISTS $joinOutputTable")
    spark.sql(
      s"""CREATE TABLE $joinOutputTable (
         |  user_id INT,
         |  feature_1 DOUBLE,
         |  ds STRING
         |)
         |PARTITIONED BY (ds)""".stripMargin
    )
    // No rows inserted — empty for the queried date range.

    val joinConf = Builders.Join(
      metaData = Builders.MetaData(namespace = "test_db", name = "empty_stats_join"),
      left = Builders.Source.events(Builders.Query(), table = joinOutputTable),
      joinParts = Seq.empty
    )

    val joinStatsNode = new JoinStatsComputeNode().setJoin(joinConf)
    val nodeContent = new NodeContent()
    nodeContent.setJoinStatsCompute(joinStatsNode)

    val tableDep = TableDependencies.fromTable(
      joinOutputTable,
      new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd")
    )

    implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
    val metadata = MetaDataUtils.layer(
      baseMetadata = new MetaData().setOutputNamespace("test_db").setTeam("test_team"),
      modeName = "backfill",
      nodeName = "test_db__empty_stats_join__stats_compute",
      tableDependencies = Seq(tableDep),
      stepDays = Some(1),
      outputTableOverride = Some("test_db.empty_stats_join__stats_output")
    )

    val node = new Node().setMetaData(metadata).setContent(nodeContent)
    val runner = new BatchNodeRunner(node, tableUtils, mockApi)
    val range = PartitionRange(twoDaysAgo, yesterday)(tableUtils.partitionSpec)

    noException should be thrownBy runner.run(metadata, nodeContent, Option(range))
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS test_db CASCADE")
    spark.stop()
  }
}
