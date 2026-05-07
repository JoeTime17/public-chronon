package ai.chronon.spark.batch

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.api.planner.{MetaDataUtils, TableDependencies}
import ai.chronon.planner.{MonolithJoinNode, Node, NodeContent}
import ai.chronon.spark.utils.{MockApi, SparkTestBase}
import ai.chronon.spark.catalog.{CreationUtils, TableUtils}
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.utils.SemanticUtils
import org.junit.Assert._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._

class BatchNodeRunnerArchivalRollbackTest extends AnyFlatSpec with BeforeAndAfterEach {

  // Build session without Iceberg HadoopCatalog so that table renames work
  lazy val spark = SparkSessionBuilder.build("archival_rollback_test", local = true)
  implicit lazy val tableUtils: TableUtils = TableUtils(spark)

  private val namespace = "archival_rollback_test"
  private val mockKVStore = new MockKVStoreWithTracking()
  private val mockApi = new MockApi(() => mockKVStore, namespace)
  private lazy val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private lazy val yesterday = tableUtils.partitionSpec.before(today)
  private lazy val twoDaysAgo = tableUtils.partitionSpec.before(yesterday)

  override def beforeEach(): Unit = {
    SparkTestBase.createDatabase(spark, namespace)
    spark.sql(s"DROP TABLE IF EXISTS $namespace.input_table")
    spark.sql(s"DROP TABLE IF EXISTS $namespace.output_table")
    spark.sql(s"DROP TABLE IF EXISTS $namespace.output_table${Constants.archiveReuseTableSuffix}")

    // Create input table with partitions needed for runFromArgs
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $namespace.input_table (
         |  id INT, value STRING, ds STRING
         |) PARTITIONED BY (ds)""".stripMargin)
    spark.sql(s"INSERT INTO $namespace.input_table VALUES (1, 'v1', '$yesterday'), (2, 'v2', '$twoDaysAgo')")

    super.beforeEach()
  }

  override def afterEach(): Unit = {
    try {
      spark.sql(s"DROP DATABASE IF EXISTS $namespace CASCADE")
    } finally {
      super.afterEach()
    }
  }

  private def createOutputTableWithHash(tableName: String, hash: String): Unit = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $tableName (
         |  id INT, value STRING, ds STRING
         |) PARTITIONED BY (ds)""".stripMargin)
    spark.sql(s"INSERT INTO $tableName VALUES (1, 'existing', '$yesterday')")
    val props = tableUtils.getTableProperties(tableName).getOrElse(Map.empty)
    tableUtils.sql(CreationUtils.alterTablePropertiesSql(tableName, props + (Constants.SemanticHashKey -> hash)))
  }

  private def makeNode(outputTable: String, semanticHash: String): Node = {
    implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
    val inputTable = s"$namespace.input_table"
    val query = new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd")
    val tableDependency = TableDependencies.fromTable(inputTable, query)

    val metadata = MetaDataUtils.layer(
      baseMetadata = new MetaData().setOutputNamespace(namespace).setTeam("test_team"),
      modeName = "test_mode",
      nodeName = "test_batch_node",
      tableDependencies = Seq(tableDependency),
      stepDays = Some(1),
      outputTableOverride = Some(outputTable)
    )

    val join = new Join()
      .setMetaData(metadata)
      .setLeft({
        val src = new Source()
        src.setEvents(
          new EventSource().setTable(inputTable).setQuery(query))
        src
      })
      .setJoinParts(Seq.empty.asJava)
    val monolithJoin = new MonolithJoinNode()
    monolithJoin.setJoin(join)
    val nodeContent = new NodeContent()
    nodeContent.setMonolithJoin(monolithJoin)

    val node = new Node()
    node.setMetaData(metadata)
    node.setContent(nodeContent)
    node.setSemanticHash(semanticHash)
    node
  }

  "BatchNodeRunner.runFromArgs" should "rollback archived table when run() fails" in {
    val outputTable = s"$namespace.output_table"
    val archiveTable = outputTable + Constants.archiveReuseTableSuffix

    createOutputTableWithHash(outputTable, "old_hash")
    assertTrue("Output table should exist before test", tableUtils.tableReachable(outputTable))

    val node = makeNode(outputTable, "new_hash")

    val failingRunner = new BatchNodeRunner(node, tableUtils, mockApi) {
      override def run(metadata: MetaData, conf: NodeContent, maybeRange: Option[PartitionRange]): Unit = {
        throw new RuntimeException("Simulated job failure")
      }
    }

    val exitCode = failingRunner.runFromArgs(twoDaysAgo, yesterday, None)

    assertEquals("runFromArgs should return 1 on failure", 1, exitCode)
    assertTrue(s"Output table should be restored after failure", tableUtils.tableReachable(outputTable))
    assertFalse(s"Archive table should not exist after rollback", tableUtils.tableReachable(archiveTable))
  }

  it should "skip rollback when another writer already produced the table with the new hash" in {
    val outputTable = s"$namespace.output_table"
    val archiveTable = outputTable + Constants.archiveReuseTableSuffix
    val newHash = "new_hash"

    createOutputTableWithHash(outputTable, "old_hash")

    val node = makeNode(outputTable, newHash)

    val failingRunner = new BatchNodeRunner(node, tableUtils, mockApi) {
      override def run(metadata: MetaData, conf: NodeContent, maybeRange: Option[PartitionRange]): Unit = {
        // Simulate another writer creating the output table with the new hash
        spark.sql(
          s"""CREATE TABLE $outputTable (
             |  id INT, value STRING, ds STRING
             |) PARTITIONED BY (ds)""".stripMargin)
        spark.sql(s"INSERT INTO $outputTable VALUES (99, 'from_other_writer', '$yesterday')")
        val props = tableUtils.getTableProperties(outputTable).getOrElse(Map.empty)
        tableUtils.sql(CreationUtils.alterTablePropertiesSql(outputTable, props + (Constants.SemanticHashKey -> newHash)))

        throw new RuntimeException("Simulated job failure")
      }
    }

    val exitCode = failingRunner.runFromArgs(twoDaysAgo, yesterday, None)

    assertEquals("runFromArgs should return 1 on failure", 1, exitCode)
    assertTrue(s"Output table should still exist", tableUtils.tableReachable(outputTable))
    val currentHash = tableUtils.getTableProperties(outputTable).flatMap(_.get(Constants.SemanticHashKey))
    assertEquals("Output table should retain the new semantic hash", Some(newHash), currentHash)
    assertTrue(s"Archive table should still exist since rollback was skipped", tableUtils.tableReachable(archiveTable))
  }
}
