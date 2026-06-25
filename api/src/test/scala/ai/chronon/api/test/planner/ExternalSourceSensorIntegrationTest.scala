package ai.chronon.api.test.planner

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.api.planner.{ExternalSourceSensorUtil, GroupByPlanner, MonolithJoinPlanner, StagingQueryPlanner}
import ai.chronon.api.test.planner.GroupByPlannerTest.buildGroupBy
import ai.chronon.planner.Mode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class ExternalSourceSensorIntegrationTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  private def createTableDependency(tableName: String): TableDependency = {
    val tableInfo = new TableInfo()
      .setTable(tableName)
      .setPartitionColumn("ds")
      .setPartitionFormat("yyyy-MM-dd")
    new TableDependency().setTableInfo(tableInfo)
  }

  private def createMetaDataWithDependencies(name: String, tableDeps: Seq[TableDependency]): MetaData = {
    val executionInfo = new ExecutionInfo()
      .setTableDependencies(tableDeps.asJava)
      .setOfflineSchedule("@daily")

    new MetaData()
      .setName(name)
      .setTeam("test_team")
      .setVersion("1")
      .setOutputNamespace("test_namespace")
      .setExecutionInfo(executionInfo)
  }

  "GroupByPlanner" should "create external sensor nodes for table dependencies" in {
    // Create GroupBy sources that reference the tables directly
    val sources = Seq(
      Builders.Source.events(Builders.Query(), table = "data.purchases"),
      Builders.Source.events(Builders.Query(), table = "data.user_events")
    )

    val groupBy = Builders.GroupBy(
      sources = sources,
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "charges", Seq(WindowUtils.Unbounded))),
      metaData = Builders.MetaData(namespace = "test_namespace", name = "test_groupby"),
      accuracy = Accuracy.TEMPORAL
    )

    val planner = GroupByPlanner(groupBy)
    val plan = planner.buildPlan

    // Find sensor nodes in the plan
    val sensorNodes = plan.nodes.asScala.filter(_.content.isSetExternalSourceSensor)

    val purchasesSensor =
      sensorNodes.find(_.content.getExternalSourceSensor.sourceTableDependency.tableInfo.table == "data.purchases")
    val eventsSensor =
      sensorNodes.find(_.content.getExternalSourceSensor.sourceTableDependency.tableInfo.table == "data.user_events")

    purchasesSensor should be(defined)
    eventsSensor should be(defined)

    sensorNodes.foreach { node =>
      val sensor = node.content.getExternalSourceSensor

      // Verify sensor structure
      sensor should not be null
      sensor.sourceTableDependency.tableInfo.table should not be empty
      sensor.metaData should not be null

      // Verify sensor metadata follows expected naming pattern
      sensor.metaData.name should include("sensor")

      // Verify sensor has no table dependencies (sensors are leaf nodes)
      sensor.metaData.executionInfo.tableDependencies should be(empty)

      // Verify sensor output table matches source name
      sensor.metaData.executionInfo.outputTableInfo.table should equal(sensor.sourceTableDependency.tableInfo.table)
    }
  }

  it should "use the resolved groupBy output spec for time-partitioned source sensors" in {
    val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)
    val query = new Query()
      .setPartitionColumn("event_ts")
      .setPartitionFormat(threeHourSpec.format)
      .setTimePartitioned(true)

    val outputInfo = new TableInfo()
      .setTable("test_namespace.subdaily_groupby")
      .withSpec(threeHourSpec)

    val groupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query, table = "data.time_partitioned_events")),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "events", Seq(WindowUtils.Hour))),
      metaData = Builders.MetaData(
        namespace = "test_namespace",
        name = "subdaily_groupby",
        executionInfo = new ExecutionInfo().setOutputTableInfo(outputInfo)
      ),
      accuracy = Accuracy.TEMPORAL
    )

    val sensor = GroupByPlanner(groupBy).buildPlan.nodes.asScala
      .find(_.content.isSetExternalSourceSensor)
      .get
      .content
      .getExternalSourceSensor

    sensor.sourceTableDependency.tableInfo.isTimePartitioned should be(true)
    sensor.metaData.executionInfo.outputTableInfo.partitionColumn should equal("event_ts")
    sensor.metaData.executionInfo.outputTableInfo.partitionFormat should equal(threeHourSpec.format)
    sensor.metaData.executionInfo.outputTableInfo.partitionInterval should equal(WindowUtils.fromMillis(threeHourSpec.spanMillis))
  }

  "StagingQueryPlanner" should "create external sensor nodes for table dependencies" in {
    val tableDeps = Seq(
      createTableDependency("data.raw_events"),
      createTableDependency("data.enrichment_data")
    )

    val metaData = createMetaDataWithDependencies("test_staging_query", tableDeps)

    val stagingQuery = new StagingQuery()
      .setMetaData(metaData)
      .setQuery("SELECT * FROM data.raw_events")

    val planner = StagingQueryPlanner(stagingQuery)
    val plan = planner.buildPlan

    // Find sensor nodes in the plan
    val sensorNodes = plan.nodes.asScala.filter(_.content.isSetExternalSourceSensor)

    sensorNodes.foreach { node =>
      val sensor = node.content.getExternalSourceSensor

      // Verify sensor structure
      sensor should not be null
      sensor.sourceTableDependency.tableInfo.table should not be empty
      sensor.metaData should not be null

      // Verify sensor metadata follows expected naming pattern
      sensor.metaData.name should include("__sensor")
      sensor.metaData.name should include(stagingQuery.metaData.name)

      // Verify sensor has no table dependencies
      sensor.metaData.executionInfo.tableDependencies should be(empty)

      // Verify sensor output table matches source name
      sensor.metaData.executionInfo.outputTableInfo.table should equal(sensor.sourceTableDependency.tableInfo.table)
    }
  }

  "MonolithJoinPlanner" should "create external sensor nodes for table dependencies" in {
    // Create metadata without explicit table dependencies
    val metaData = new MetaData()
      .setName("test_join")
      .setTeam("test_team")
      .setVersion("1")
      .setOutputNamespace("test_namespace")
      .setExecutionInfo(new ExecutionInfo().setOfflineSchedule("@daily"))

    // Create left hand side source pointing to fact table
    val leftSource = new Source()
    leftSource.setEvents(
      new EventSource()
        .setTable("data.fact_table")
        .setQuery(new Query().setPartitionColumn("ds").setPartitionFormat("yyyy-MM-dd"))
    )

    // Create a groupBy that uses dimension table as source
    val dimensionGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = "data.dimension_table")),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "events", Seq(WindowUtils.Unbounded))),
      metaData = Builders.MetaData(namespace = "test_namespace", name = "dimension_features"),
      accuracy = Accuracy.TEMPORAL
    )

    // Create join with left source and joinParts containing groupBy
    val join = new Join()
      .setMetaData(metaData)
      .setRowIds(List("user_id").asJava)
      .setLeft(leftSource)
      .setJoinParts(Seq(Builders.JoinPart(groupBy = dimensionGroupBy)).asJava)

    val planner = MonolithJoinPlanner(join)
    val plan = planner.buildPlan

    // Find sensor nodes in the plan
    val sensorNodes = plan.nodes.asScala.filter(_.content.isSetExternalSourceSensor)

    sensorNodes should have size 2

    sensorNodes.foreach { node =>
      val sensor = node.content.getExternalSourceSensor

      // Verify sensor structure
      sensor should not be null
      sensor.sourceTableDependency.tableInfo.table should not be empty
      sensor.metaData should not be null

      // Verify sensor metadata follows expected naming pattern
      sensor.metaData.name should include("sensor")

      // Verify sensor has no table dependencies
      sensor.metaData.executionInfo.tableDependencies should be(empty)

      // Verify sensor output table matches source name
      sensor.metaData.executionInfo.outputTableInfo.table should equal(sensor.sourceTableDependency.tableInfo.table)
    }
  }

  "All planners" should "create sensors with consistent metadata structure" in {
    val tableDeps = Seq(
      createTableDependency("data.shared_table_1"),
      createTableDependency("data.shared_table_2")
    )

    // Test GroupBy planner
    val groupBy = buildGroupBy()
    val gbMetaData = createMetaDataWithDependencies("test_groupby", tableDeps)
    groupBy.setMetaData(gbMetaData)
    val gbPlan = GroupByPlanner(groupBy).buildPlan

    // Test StagingQuery planner
    val stagingQuery = new StagingQuery()
      .setMetaData(createMetaDataWithDependencies("test_staging_query", tableDeps))
      .setQuery("SELECT * FROM data.shared_table_1")
    val sqPlan = StagingQueryPlanner(stagingQuery).buildPlan

    // Test Join planner
    val join = new Join()
      .setMetaData(createMetaDataWithDependencies("test_join", tableDeps))
      .setRowIds(List("user_id").asJava)
    val joinPlan = MonolithJoinPlanner(join).buildPlan

    val allPlans = Seq(
      ("GroupBy", gbPlan),
      ("StagingQuery", sqPlan),
      ("Join", joinPlan)
    )

    allPlans.foreach { case (plannerType, plan) =>
      val sensorNodes = plan.nodes.asScala.filter(_.content.isSetExternalSourceSensor)

      sensorNodes.foreach { node =>
        val sensor = node.content.getExternalSourceSensor

        withClue(s"$plannerType planner sensor validation: ") {
          // Verify sensor follows consistent structure across all planners
          sensor.metaData.name should fullyMatch regex ".*__sensor"
          sensor.metaData.executionInfo should not be null
          sensor.metaData.executionInfo.outputTableInfo should not be null
          sensor.metaData.executionInfo.outputTableInfo.table should not be empty

          // Verify sensor output table matches source name
          sensor.metaData.executionInfo.outputTableInfo.table should equal(sensor.sourceTableDependency.tableInfo.table)

          // Verify sensor has empty table dependencies
          Option(sensor.metaData.executionInfo.tableDependencies).foreach { deps =>
            deps should be(empty)
          }

          // Verify partition spec is correctly applied
          sensor.metaData.executionInfo.outputTableInfo.partitionColumn should equal(testPartitionSpec.column)
          sensor.metaData.executionInfo.outputTableInfo.partitionFormat should equal(testPartitionSpec.format)
        }
      }
    }
  }

  "ExternalSourceSensorUtil" should "override resource configs but preserve non-resource configs from downstream" in {
    val tableDeps = Seq(createTableDependency("data.source_table"))

    val conf = new ConfigProperties()
    conf.setCommon(Map(
      "spark.driver.memory" -> "8g",
      "spark.driver.cores" -> "8",
      "spark.executor.memory" -> "16g",
      "spark.executor.cores" -> "8",
      "spark.executor.instances" -> "100",
      "spark.default.parallelism" -> "5000",
      "spark.sql.shuffle.partitions" -> "10000",
      "spark.chronon.partition.format" -> "yyyy-MM-dd",
      "spark.chronon.table_write.format" -> "iceberg",
      "spark.sql.catalog.spark_catalog.warehouse" -> "s3://my-bucket/warehouse/"
    ).asJava)

    val executionInfo = new ExecutionInfo()
      .setTableDependencies(tableDeps.asJava)
      .setOfflineSchedule("@daily")
      .setConf(conf)

    val metaData = new MetaData()
      .setName("heavy_job")
      .setTeam("test_team")
      .setVersion("1")
      .setOutputNamespace("test_namespace")
      .setExecutionInfo(executionInfo)

    val sensorNodes = ExternalSourceSensorUtil.sensorNodes(metaData)
    sensorNodes should have size 1

    val sensorConf = sensorNodes.head.metaData.executionInfo.conf.common.asScala

    // Resource configs should be overridden to lightweight defaults
    sensorConf("spark.driver.memory") should equal("1g")
    sensorConf("spark.driver.cores") should equal("4")
    sensorConf("spark.executor.memory") should equal("1g")
    sensorConf("spark.executor.cores") should equal("4")
    sensorConf("spark.executor.instances") should equal("1")
    sensorConf("spark.default.parallelism") should equal("4")
    sensorConf("spark.sql.shuffle.partitions") should equal("4")

    // Non-resource configs should be preserved from downstream
    sensorConf("spark.chronon.partition.format") should equal("yyyy-MM-dd")
    sensorConf("spark.chronon.table_write.format") should equal("iceberg")
    sensorConf("spark.sql.catalog.spark_catalog.warehouse") should equal("s3://my-bucket/warehouse/")
  }

  it should "handle root-level table dependencies correctly" in {
    // Test with deeply nested table names and complex scenarios
    val complexTableDeps = Seq(
      createTableDependency("production.events.user_actions"),
      createTableDependency("staging.analytics.feature_store"),
      createTableDependency("warehouse.raw.transaction_logs")
    )

    val metaData = createMetaDataWithDependencies("complex_entity.v2_production", complexTableDeps)

    val sensorNodes = ExternalSourceSensorUtil.sensorNodes(metaData)

    sensorNodes should have size 3

    // Verify each sensor targets the correct root-level table
    val expectedTables = Set(
      "production.events.user_actions",
      "staging.analytics.feature_store",
      "warehouse.raw.transaction_logs"
    )

    val actualTables = sensorNodes.map(_.sourceTableDependency.tableInfo.table).toSet
    actualTables should equal(expectedTables)

    // Verify each sensor has unique and correctly formatted metadata
    sensorNodes.foreach { sensor =>
      sensor.metaData.name should endWith("__sensor")
      sensor.metaData.executionInfo.outputTableInfo.table should equal(sensor.sourceTableDependency.tableInfo.table)
      sensor.metaData.executionInfo.tableDependencies should be(empty)
    }

    // Verify all sensor names are unique
    val sensorNames = sensorNodes.map(_.metaData.name).toSet
    sensorNames should have size 3
  }

  "Sensor nodes" should "be correctly integrated into planner execution graphs" in {
    val tableDeps = Seq(createTableDependency("data.source_table"))
    val groupBy = buildGroupBy()
    val metaData = createMetaDataWithDependencies("integration_test", tableDeps)
    groupBy.setMetaData(metaData)

    val planner = GroupByPlanner(groupBy)
    val plan = planner.buildPlan

    // Verify sensors are included in the overall plan
    val allNodes = plan.nodes.asScala
    val sensorNodes = allNodes.filter(_.content.isSetExternalSourceSensor)
    val backfillNodes = allNodes.filter(_.content.isSetGroupByBackfill)
    val deployNodes = allNodes.filter(node => node.content.isSetGroupByUploadToKV || node.content.isSetGroupByStreaming)

    sensorNodes should not be empty
    backfillNodes should not be empty
    deployNodes should not be empty

    // Verify sensors are part of the plan's execution graph
    allNodes should contain allElementsOf sensorNodes

    // Verify terminal nodes include the expected modes
    plan.terminalNodeNames should not be null
    plan.terminalNodeNames.containsKey(Mode.BACKFILL) should be(true)
    plan.terminalNodeNames.containsKey(Mode.DEPLOY) should be(true)
  }
}
