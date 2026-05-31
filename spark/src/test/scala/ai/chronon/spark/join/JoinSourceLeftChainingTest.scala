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

package ai.chronon.spark.join

import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.{Accuracy, Builders, ConfigProperties, ExecutionInfo, LongType, Operation, PartitionRange, StringType, StructField, StructType, TsUtils}
import ai.chronon.spark._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.utils.TestUtils
import org.apache.spark.sql.Row
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}

import scala.collection.JavaConverters._

class JoinSourceLeftChainingTest extends BaseJoinTest {

  private case class EntityClaimsChain(contextWithRole: api.Join,
                                       enrichedClaims: api.Join,
                                       finalFeatures: api.Join,
                                       flattenedFinalFeatures: api.Join)

  private def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)

  private def modularExecutionInfo: ExecutionInfo =
    new ExecutionInfo().setConf(
      new ConfigProperties().setCommon(Map("modular_execution" -> "true").asJava)
    )

  private def buildJoinChain(suffix: String): (api.Join, api.Join, api.Join) = {
    val chainingNamespace = s"${namespace}_${suffix}"
    createDatabase(chainingNamespace)

    val requestsTable = s"$chainingNamespace.requests"
    val userListingsTable = s"$chainingNamespace.user_listings"
    val listingFeaturesTable = s"$chainingNamespace.listing_features"

    val requestsSchema = StructType(
      "requests",
      Array(
        StructField("user_id", LongType),
        StructField("ts", LongType),
        StructField("ds", StringType)
      )
    )
    val requestsRows = List(
      Row(1L, toTs("2023-06-01 10:00:00"), "2023-06-01"),
      Row(2L, toTs("2023-06-01 12:00:00"), "2023-06-01"),
      Row(1L, toTs("2023-06-02 09:00:00"), "2023-06-02")
    )
    TestUtils.makeDf(spark, requestsSchema, requestsRows).save(requestsTable)

    val userListingsSchema = StructType(
      "user_listings",
      Array(
        StructField("user_id", LongType),
        StructField("listing_id", LongType),
        StructField("ts", LongType),
        StructField("ds", StringType)
      )
    )
    val userListingsRows = List(
      Row(1L, 101L, toTs("2023-06-01 08:00:00"), "2023-06-01"),
      Row(2L, 202L, toTs("2023-06-01 11:00:00"), "2023-06-01"),
      Row(1L, 303L, toTs("2023-06-02 08:30:00"), "2023-06-02")
    )
    TestUtils.makeDf(spark, userListingsSchema, userListingsRows).save(userListingsTable)

    val listingFeaturesSchema = StructType(
      "listing_features",
      Array(
        StructField("listing_id", LongType),
        StructField("feature_value", LongType),
        StructField("ts", LongType),
        StructField("ds", StringType)
      )
    )
    val listingFeaturesRows = List(
      Row(101L, 11L, toTs("2023-06-01 07:00:00"), "2023-06-01"),
      Row(202L, 22L, toTs("2023-06-01 10:00:00"), "2023-06-01"),
      Row(303L, 33L, toTs("2023-06-02 08:00:00"), "2023-06-02")
    )
    TestUtils.makeDf(spark, listingFeaturesSchema, listingFeaturesRows).save(listingFeaturesTable)

    val userListingGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(
        query = Builders.Query(startPartition = "2023-06-01", partitionColumn = "ds"),
        table = userListingsTable
      )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "listing_id")),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = s"user_listing_lookup_$suffix", namespace = chainingNamespace)
    )

    val upstreamJoin = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("user_id", "ts"),
          startPartition = "2023-06-01",
          partitionColumn = "ds"
        ),
        table = requestsTable
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = userListingGroupBy)),
      metaData = Builders.MetaData(name = s"upstream_join_$suffix", namespace = chainingNamespace, team = "chronon")
    )

    val listingIdColumn = upstreamJoin.joinPartColumns.values.flatten.head
    val chainedLeftQuery = Builders.Query(
      selects = Builders.Selects.exprs(
        "user_id" -> "user_id",
        "listing_id" -> listingIdColumn,
        "ts" -> "ts"
      ),
      startPartition = "2023-06-01",
      partitionColumn = "ds"
    )

    val listingFeaturesGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(
        query = Builders.Query(startPartition = "2023-06-01", partitionColumn = "ds"),
        table = listingFeaturesTable
      )),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "feature_value")),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = s"listing_features_lookup_$suffix", namespace = chainingNamespace)
    )

    val chainedJoin = Builders.Join(
      left = Builders.Source.joinSource(upstreamJoin, chainedLeftQuery),
      joinParts = Seq(Builders.JoinPart(groupBy = listingFeaturesGroupBy)),
      metaData = Builders.MetaData(name = s"downstream_join_nested_$suffix", namespace = chainingNamespace, team = "chronon")
    )

    val flattenedJoin = Builders.Join(
      left = Builders.Source.events(chainedLeftQuery, upstreamJoin.metaData.outputTable),
      joinParts = Seq(Builders.JoinPart(groupBy = listingFeaturesGroupBy)),
      metaData = Builders.MetaData(name = s"downstream_join_flat_$suffix", namespace = chainingNamespace, team = "chronon")
    )

    (upstreamJoin, chainedJoin, flattenedJoin)
  }

  private def dropTable(table: String): Unit =
    spark.sql(s"DROP TABLE IF EXISTS $table")

  private def dropJoinOutput(joinConf: api.Join): Unit = {
    dropTable(joinConf.metaData.outputTable)
    if (joinConf.finalOutputTable != joinConf.metaData.outputTable) {
      dropTable(joinConf.finalOutputTable)
    }
    dropTable(joinConf.metaData.bootstrapTable)
  }

  private def computeJoinOutput(joinConf: api.Join, endPartition: String): Unit = {
    val startPartition = Option(joinConf.left)
      .flatMap(left => Option(left.rootQuery))
      .flatMap(query => Option(query.startPartition))
      .getOrElse(endPartition)

    if (joinConf.hasSeparateDerivedOutput) {
      ai.chronon.spark.batch.ModularMonolith.run(
        joinConf,
        new api.DateRange().setStartDate(startPartition).setEndDate(endPartition)
      )(tableUtils)
    } else {
      new ai.chronon.spark.Join(joinConf, endPartition, tableUtils).computeJoin()
    }
    ()
  }

  private def buildModularDerivedJoinSource(suffix: String,
                                           forceSeparateDerivedOutput: Boolean = false): (api.Join, api.Source) = {
    val chainingNamespace = s"${namespace}_${suffix}"
    createDatabase(chainingNamespace)

    val requestsTable = s"$chainingNamespace.requests"
    val userListingsTable = s"$chainingNamespace.user_listings"

    val requestsSchema = StructType(
      "requests",
      Array(
        StructField("user_id", LongType),
        StructField("ts", LongType),
        StructField("ds", StringType)
      )
    )
    val requestsRows = List(
      Row(1L, toTs("2023-06-01 10:00:00"), "2023-06-01"),
      Row(2L, toTs("2023-06-01 12:00:00"), "2023-06-01"),
      Row(1L, toTs("2023-06-02 09:00:00"), "2023-06-02")
    )
    TestUtils.makeDf(spark, requestsSchema, requestsRows).save(requestsTable)

    val userListingsSchema = StructType(
      "user_listings",
      Array(
        StructField("user_id", LongType),
        StructField("listing_id", LongType),
        StructField("ts", LongType),
        StructField("ds", StringType)
      )
    )
    val userListingsRows = List(
      Row(1L, 101L, toTs("2023-06-01 08:00:00"), "2023-06-01"),
      Row(2L, 202L, toTs("2023-06-01 11:00:00"), "2023-06-01"),
      Row(1L, 303L, toTs("2023-06-02 08:30:00"), "2023-06-02")
    )
    TestUtils.makeDf(spark, userListingsSchema, userListingsRows).save(userListingsTable)

    val userListingGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(
        query = Builders.Query(startPartition = "2023-06-01", partitionColumn = "ds"),
        table = userListingsTable
      )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "listing_id")),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = s"user_listing_lookup_$suffix", namespace = chainingNamespace)
    )

    val userListingTsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(
        query = Builders.Query(startPartition = "2023-06-01", partitionColumn = "ds"),
        table = userListingsTable
      )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "ts")),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = s"user_listing_ts_lookup_$suffix", namespace = chainingNamespace)
    )

    val joinParts =
      if (forceSeparateDerivedOutput) {
        Seq(
          Builders.JoinPart(groupBy = userListingGroupBy),
          Builders.JoinPart(groupBy = userListingTsGroupBy)
        )
      } else {
        Seq(Builders.JoinPart(groupBy = userListingGroupBy))
      }

    val upstreamJoin = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("user_id", "ts"),
          startPartition = "2023-06-01",
          partitionColumn = "ds"
        ),
        table = requestsTable
      ),
      joinParts = joinParts,
      metaData = Builders.MetaData(
        name = s"upstream_join_modular_$suffix",
        namespace = chainingNamespace,
        team = "chronon",
        executionInfo = modularExecutionInfo
      )
    )

    val listingIdColumn = upstreamJoin.joinPartColumns.values.flatten.head
    upstreamJoin.setDerivations(Seq(Builders.Derivation("derived_listing_id", listingIdColumn)).asJava)

    val chainedLeftQuery = Builders.Query(
      selects = Builders.Selects.exprs(
        "user_id" -> "user_id",
        "listing_id" -> "derived_listing_id",
        "ts" -> "ts"
      ),
      startPartition = "2023-06-01",
      partitionColumn = "ds"
    )

    (upstreamJoin, Builders.Source.joinSource(upstreamJoin, chainedLeftQuery))
  }

  private def buildModularDerivedJoinChain(suffix: String,
                                           forceSeparateDerivedOutput: Boolean = false): (api.Join, api.Join, api.Join) = {
    val chainingNamespace = s"${namespace}_${suffix}"
    createDatabase(chainingNamespace)

    val requestsTable = s"$chainingNamespace.requests"
    val userListingsTable = s"$chainingNamespace.user_listings"
    val listingFeaturesTable = s"$chainingNamespace.listing_features"

    val requestsSchema = StructType(
      "requests",
      Array(
        StructField("user_id", LongType),
        StructField("ts", LongType),
        StructField("ds", StringType)
      )
    )
    val requestsRows = List(
      Row(1L, toTs("2023-06-01 10:00:00"), "2023-06-01"),
      Row(2L, toTs("2023-06-01 12:00:00"), "2023-06-01"),
      Row(1L, toTs("2023-06-02 09:00:00"), "2023-06-02")
    )
    TestUtils.makeDf(spark, requestsSchema, requestsRows).save(requestsTable)

    val userListingsSchema = StructType(
      "user_listings",
      Array(
        StructField("user_id", LongType),
        StructField("listing_id", LongType),
        StructField("ts", LongType),
        StructField("ds", StringType)
      )
    )
    val userListingsRows = List(
      Row(1L, 101L, toTs("2023-06-01 08:00:00"), "2023-06-01"),
      Row(2L, 202L, toTs("2023-06-01 11:00:00"), "2023-06-01"),
      Row(1L, 303L, toTs("2023-06-02 08:30:00"), "2023-06-02")
    )
    TestUtils.makeDf(spark, userListingsSchema, userListingsRows).save(userListingsTable)

    val listingFeaturesSchema = StructType(
      "listing_features",
      Array(
        StructField("listing_id", LongType),
        StructField("feature_value", LongType),
        StructField("ts", LongType),
        StructField("ds", StringType)
      )
    )
    val listingFeaturesRows = List(
      Row(101L, 11L, toTs("2023-06-01 07:00:00"), "2023-06-01"),
      Row(202L, 22L, toTs("2023-06-01 10:00:00"), "2023-06-01"),
      Row(303L, 33L, toTs("2023-06-02 08:00:00"), "2023-06-02")
    )
    TestUtils.makeDf(spark, listingFeaturesSchema, listingFeaturesRows).save(listingFeaturesTable)

    val userListingGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(
        query = Builders.Query(startPartition = "2023-06-01", partitionColumn = "ds"),
        table = userListingsTable
      )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "listing_id")),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = s"user_listing_lookup_$suffix", namespace = chainingNamespace)
    )

    val userListingTsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(
        query = Builders.Query(startPartition = "2023-06-01", partitionColumn = "ds"),
        table = userListingsTable
      )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "ts")),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = s"user_listing_ts_lookup_$suffix", namespace = chainingNamespace)
    )

    val joinParts =
      if (forceSeparateDerivedOutput) {
        Seq(
          Builders.JoinPart(groupBy = userListingGroupBy),
          Builders.JoinPart(groupBy = userListingTsGroupBy)
        )
      } else {
        Seq(Builders.JoinPart(groupBy = userListingGroupBy))
      }

    val upstreamJoin = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("user_id", "ts"),
          startPartition = "2023-06-01",
          partitionColumn = "ds"
        ),
        table = requestsTable
      ),
      joinParts = joinParts,
      metaData = Builders.MetaData(
        name = s"upstream_join_modular_$suffix",
        namespace = chainingNamespace,
        team = "chronon",
        executionInfo = modularExecutionInfo
      )
    )

    val listingIdColumn = upstreamJoin.joinPartColumns.values.flatten.head
    upstreamJoin.setDerivations(Seq(Builders.Derivation("derived_listing_id", listingIdColumn)).asJava)

    val chainedLeftQuery = Builders.Query(
      selects = Builders.Selects.exprs(
        "user_id" -> "user_id",
        "listing_id" -> "derived_listing_id",
        "ts" -> "ts"
      ),
      startPartition = "2023-06-01",
      partitionColumn = "ds"
    )

    val listingFeaturesGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(
        query = Builders.Query(startPartition = "2023-06-01", partitionColumn = "ds"),
        table = listingFeaturesTable
      )),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "feature_value")),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = s"listing_features_lookup_$suffix", namespace = chainingNamespace)
    )

    val chainedJoin = Builders.Join(
      left = Builders.Source.joinSource(upstreamJoin, chainedLeftQuery),
      joinParts = Seq(Builders.JoinPart(groupBy = listingFeaturesGroupBy)),
      metaData = Builders.MetaData(name = s"downstream_join_modular_nested_$suffix",
                                   namespace = chainingNamespace,
                                   team = "chronon")
    )

    val flattenedJoin = Builders.Join(
      left = Builders.Source.events(chainedLeftQuery, upstreamJoin.finalOutputTable),
      joinParts = Seq(Builders.JoinPart(groupBy = listingFeaturesGroupBy)),
      metaData = Builders.MetaData(name = s"downstream_join_modular_flat_$suffix",
                                   namespace = chainingNamespace,
                                   team = "chronon")
    )

    (upstreamJoin, chainedJoin, flattenedJoin)
  }

  private def buildModularDerivedGroupByChain(suffix: String,
                                              forceSeparateDerivedOutput: Boolean = false): (api.Join, api.GroupBy, api.GroupBy) = {
    val (upstreamJoin, chainedSource) = buildModularDerivedJoinSource(s"${suffix}_source", forceSeparateDerivedOutput)
    val chainingNamespace = upstreamJoin.metaData.outputNamespace
    val groupByName = s"downstream_group_by_$suffix"

    val chainedGroupBy = Builders.GroupBy(
      sources = Seq(chainedSource),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "listing_id")),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = s"${groupByName}_nested", namespace = chainingNamespace)
    )

    val flattenedGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(chainedSource.query, upstreamJoin.finalOutputTable)),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "listing_id")),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = s"${groupByName}_flat", namespace = chainingNamespace)
    )

    (upstreamJoin, chainedGroupBy, flattenedGroupBy)
  }

  private def buildEntityClaimsChain(suffix: String): EntityClaimsChain = {
    val chainingNamespace = s"${namespace}_${suffix}"
    createDatabase(chainingNamespace)

    val contextTable = s"$chainingNamespace.context"
    val roleTable = s"$chainingNamespace.claim_contact_role"
    val incidentTable = s"$chainingNamespace.incident"
    val bodypartTable = s"$chainingNamespace.bodypart"
    val startPartition = "2023-06-01"

    val contextSchema = StructType(
      "context",
      Array(
        StructField("cc_id", LongType),
        StructField("claim_id", LongType),
        StructField("exposure_id", LongType),
        StructField("incident_id", LongType),
        StructField("ds", StringType)
      )
    )
    val contextRows = List(
      Row(1L, 100L, 500L, 9001L, "2023-06-01"),
      Row(2L, 100L, 500L, 9002L, "2023-06-01"),
      Row(3L, 101L, 501L, 9003L, "2023-06-01"),
      Row(4L, 102L, 502L, 9004L, "2023-06-01")
    )
    TestUtils.makeDf(spark, contextSchema, contextRows).save(contextTable)

    val roleSchema = StructType(
      "claim_contact_role",
      Array(
        StructField("cc_id", LongType),
        StructField("role_code", LongType),
        StructField("ts", LongType),
        StructField("ds", StringType)
      )
    )
    val roleRows = List(
      Row(1L, 11011L, toTs("2023-06-01 08:00:00"), "2023-06-01"),
      Row(2L, 11011L, toTs("2023-06-01 08:05:00"), "2023-06-01"),
      Row(3L, 99999L, toTs("2023-06-01 08:10:00"), "2023-06-01"),
      Row(4L, 11011L, toTs("2023-06-01 08:15:00"), "2023-06-01")
    )
    TestUtils.makeDf(spark, roleSchema, roleRows).save(roleTable)

    val incidentSchema = StructType(
      "incident",
      Array(
        StructField("incident_id", LongType),
        StructField("hospital_flag", LongType),
        StructField("fatality_flag", LongType),
        StructField("treatment_flag", LongType),
        StructField("ts", LongType),
        StructField("ds", StringType)
      )
    )
    val incidentRows = List(
      Row(9001L, 1L, 0L, 1L, toTs("2023-06-01 09:00:00"), "2023-06-01"),
      Row(9002L, 0L, 1L, 1L, toTs("2023-06-01 09:05:00"), "2023-06-01"),
      Row(9003L, 1L, 1L, 0L, toTs("2023-06-01 09:10:00"), "2023-06-01"),
      Row(9004L, 0L, 0L, 1L, toTs("2023-06-01 09:15:00"), "2023-06-01")
    )
    TestUtils.makeDf(spark, incidentSchema, incidentRows).save(incidentTable)

    val bodypartSchema = StructType(
      "bodypart",
      Array(
        StructField("incident_id", LongType),
        StructField("medical_condition_flag", LongType),
        StructField("injury_count", LongType),
        StructField("ds", StringType)
      )
    )
    val bodypartRows = List(
      Row(9001L, 1L, 2L, "2023-06-01"),
      Row(9001L, 0L, 1L, "2023-06-01"),
      Row(9002L, 1L, 1L, "2023-06-01"),
      Row(9003L, 1L, 3L, "2023-06-01"),
      Row(9004L, 1L, 4L, "2023-06-01")
    )
    TestUtils.makeDf(spark, bodypartSchema, bodypartRows).save(bodypartTable)

    val contextSource = Builders.Source.entities(
      query = Builders.Query(startPartition = startPartition),
      snapshotTable = contextTable
    )

    val roleLookupName = s"role_lookup_$suffix"
    val roleLookupGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.entities(
        query = Builders.Query(
          selects = Builders.Selects("cc_id", "role_code", "ts"),
          startPartition = startPartition
        ),
        snapshotTable = roleTable
      )),
      keyColumns = Seq("cc_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "role_code")),
      metaData = Builders.MetaData(name = roleLookupName, namespace = chainingNamespace, team = "chronon")
    )

    val contextWithRole = Builders.Join(
      left = contextSource.deepCopy(),
      joinParts = Seq(Builders.JoinPart(groupBy = roleLookupGroupBy)),
      metaData = Builders.MetaData(name = s"context_with_role_$suffix", namespace = chainingNamespace, team = "chronon")
    )

    val roleColumn = contextWithRole.joinPartColumns(roleLookupName).head
    val enrichedLeftQuery = Builders.Query(
      selects = Builders.Selects.exprs(
        "cc_id" -> "cc_id",
        "claim_id" -> "claim_id",
        "exposure_id" -> "exposure_id",
        "incident_id" -> "incident_id",
        "role_code" -> roleColumn
      ),
      wheres = Seq(s"$roleColumn = 11011"),
      startPartition = startPartition
    )

    val incidentFeaturesName = s"incident_features_$suffix"
    val incidentFeaturesGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.entities(
        query = Builders.Query(
          selects = Builders.Selects("incident_id", "hospital_flag", "fatality_flag", "treatment_flag", "ts"),
          startPartition = startPartition
        ),
        snapshotTable = incidentTable
      )),
      keyColumns = Seq("incident_id"),
      aggregations = Seq(
        Builders.Aggregation(Operation.LAST, "hospital_flag"),
        Builders.Aggregation(Operation.LAST, "fatality_flag"),
        Builders.Aggregation(Operation.LAST, "treatment_flag")
      ),
      metaData = Builders.MetaData(name = incidentFeaturesName, namespace = chainingNamespace, team = "chronon")
    )

    val bodypartFeaturesName = s"bodypart_features_$suffix"
    val bodypartFeaturesGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.entities(
        query = Builders.Query(startPartition = startPartition),
        snapshotTable = bodypartTable
      )),
      keyColumns = Seq("incident_id"),
      aggregations = Seq(
        Builders.Aggregation(Operation.SUM, "medical_condition_flag"),
        Builders.Aggregation(Operation.SUM, "injury_count")
      ),
      metaData = Builders.MetaData(name = bodypartFeaturesName, namespace = chainingNamespace, team = "chronon")
    )

    val enrichedClaims = Builders.Join(
      left = Builders.Source.joinSource(contextWithRole, enrichedLeftQuery),
      joinParts = Seq(
        Builders.JoinPart(groupBy = incidentFeaturesGroupBy),
        Builders.JoinPart(groupBy = bodypartFeaturesGroupBy)
      ),
      metaData = Builders.MetaData(name = s"enriched_claims_$suffix", namespace = chainingNamespace, team = "chronon")
    )

    def columnBySuffix(columns: Array[String], suffixValue: String): String =
      columns.find(_.endsWith(suffixValue)).get

    val incidentColumns = enrichedClaims.joinPartColumns(incidentFeaturesName)
    val bodypartColumns = enrichedClaims.joinPartColumns(bodypartFeaturesName)
    val hospitalColumn = columnBySuffix(incidentColumns, "hospital_flag_last")
    val fatalityColumn = columnBySuffix(incidentColumns, "fatality_flag_last")
    val treatmentColumn = columnBySuffix(incidentColumns, "treatment_flag_last")
    val medicalConditionColumn = columnBySuffix(bodypartColumns, "medical_condition_flag_sum")
    val injuryCountColumn = columnBySuffix(bodypartColumns, "injury_count_sum")

    def allKeysQuery: api.Query =
      Builders.Query(
        selects = Builders.Selects.exprs(
          "cc_id" -> "cc_id",
          "claim_id" -> "claim_id",
          "exposure_id" -> "exposure_id",
          "hospital_flag" -> hospitalColumn,
          "fatality_flag" -> fatalityColumn,
          "treatment_flag" -> treatmentColumn,
          "medical_condition_flag" -> medicalConditionColumn
        ),
        startPartition = startPartition
      )

    def injuryByCcQuery: api.Query =
      Builders.Query(
        selects = Builders.Selects.exprs(
          "cc_id" -> "cc_id",
          "injury_count" -> injuryCountColumn
        ),
        startPartition = startPartition
      )

    def injuryByExposureQuery: api.Query =
      Builders.Query(
        selects = Builders.Selects.exprs(
          "claim_id" -> "claim_id",
          "exposure_id" -> "exposure_id",
          "injury_count" -> injuryCountColumn
        ),
        startPartition = startPartition
      )

    def rolesByExposureQuery: api.Query =
      Builders.Query(
        selects = Builders.Selects.exprs(
          "exposure_id" -> "exposure_id",
          "role_present" -> "1"
        ),
        startPartition = startPartition
      )

    def buildFinalJoinParts(sourceBuilder: api.Query => api.Source): Seq[api.JoinPart] = Seq(
      Builders.JoinPart(
        groupBy = Builders.GroupBy(
          sources = Seq(sourceBuilder(allKeysQuery)),
          keyColumns = Seq("cc_id", "claim_id", "exposure_id"),
          aggregations = Seq(
            Builders.Aggregation(Operation.SUM, "hospital_flag"),
            Builders.Aggregation(Operation.SUM, "fatality_flag"),
            Builders.Aggregation(Operation.SUM, "treatment_flag"),
            Builders.Aggregation(Operation.SUM, "medical_condition_flag")
          ),
          metaData = Builders.MetaData(name = s"all_keys_features_$suffix", namespace = chainingNamespace, team = "chronon")
        )
      ),
      Builders.JoinPart(
        groupBy = Builders.GroupBy(
          sources = Seq(sourceBuilder(injuryByCcQuery)),
          keyColumns = Seq("cc_id"),
          aggregations = Seq(Builders.Aggregation(Operation.SUM, "injury_count")),
          metaData = Builders.MetaData(name = s"injury_by_cc_$suffix", namespace = chainingNamespace, team = "chronon")
        )
      ),
      Builders.JoinPart(
        groupBy = Builders.GroupBy(
          sources = Seq(sourceBuilder(injuryByExposureQuery)),
          keyColumns = Seq("claim_id", "exposure_id"),
          aggregations = Seq(Builders.Aggregation(Operation.SUM, "injury_count")),
          metaData =
            Builders.MetaData(name = s"injury_by_exposure_$suffix", namespace = chainingNamespace, team = "chronon")
        )
      ),
      Builders.JoinPart(
        groupBy = Builders.GroupBy(
          sources = Seq(sourceBuilder(rolesByExposureQuery)),
          keyColumns = Seq("exposure_id"),
          aggregations = Seq(Builders.Aggregation(Operation.SUM, "role_present")),
          metaData =
            Builders.MetaData(name = s"roles_by_exposure_$suffix", namespace = chainingNamespace, team = "chronon")
        )
      )
    )

    val finalFeatures = Builders.Join(
      left = contextSource.deepCopy(),
      joinParts = buildFinalJoinParts(query => Builders.Source.joinSource(enrichedClaims, query)),
      metaData = Builders.MetaData(name = s"final_features_$suffix", namespace = chainingNamespace, team = "chronon")
    )

    val flattenedFinalFeatures = Builders.Join(
      left = contextSource.deepCopy(),
      joinParts = buildFinalJoinParts(
        query => Builders.Source.entities(query = query, snapshotTable = enrichedClaims.metaData.outputTable)),
      metaData =
        Builders.MetaData(name = s"final_features_flat_$suffix", namespace = chainingNamespace, team = "chronon")
    )

    EntityClaimsChain(contextWithRole, enrichedClaims, finalFeatures, flattenedFinalFeatures)
  }

  it should "read from an existing upstream join when the left source is a join source" in {
    val (upstreamJoin, chainedJoin, flattenedJoin) = buildJoinChain("left_join_source")

    dropJoinOutput(upstreamJoin)
    dropJoinOutput(chainedJoin)
    dropJoinOutput(flattenedJoin)

    assertFalse(spark.catalog.tableExists(upstreamJoin.metaData.outputTable))

    computeJoinOutput(upstreamJoin, "2023-06-02")
    val actual = new ai.chronon.spark.Join(chainedJoin, "2023-06-02", tableUtils).computeJoin()

    assertTrue(spark.catalog.tableExists(upstreamJoin.metaData.outputTable))

    val expected = new ai.chronon.spark.Join(flattenedJoin, "2023-06-02", tableUtils).computeJoin()

    val diff = Comparison.sideBySide(actual, expected, List("user_id", "ts", "ds"))
    if (diff.count() > 0) {
      diff.show(false)
    }

    assertEquals(0, diff.count())
  }

  it should "support multi hop entity chaining across joins and join source groupbys" in {
    val chain = buildEntityClaimsChain("entity_claims")

    Seq(
      chain.contextWithRole,
      chain.enrichedClaims,
      chain.finalFeatures,
      chain.flattenedFinalFeatures
    ).foreach(dropJoinOutput)

    assertFalse(spark.catalog.tableExists(chain.contextWithRole.metaData.outputTable))
    assertFalse(spark.catalog.tableExists(chain.enrichedClaims.metaData.outputTable))

    computeJoinOutput(chain.contextWithRole, "2023-06-01")
    computeJoinOutput(chain.enrichedClaims, "2023-06-01")
    val actual = new ai.chronon.spark.Join(chain.finalFeatures, "2023-06-01", tableUtils).computeJoin()

    assertTrue(spark.catalog.tableExists(chain.contextWithRole.metaData.outputTable))
    assertTrue(spark.catalog.tableExists(chain.enrichedClaims.metaData.outputTable))

    val expected = new ai.chronon.spark.Join(chain.flattenedFinalFeatures, "2023-06-01", tableUtils).computeJoin()

    val diff = Comparison.sideBySide(actual, expected, List("cc_id", "claim_id", "exposure_id", "incident_id", "ds"))
    if (diff.count() > 0) {
      diff.show(false)
    }

    assertEquals(0, diff.count())
  }

  it should "materialize modular upstream joins from the base output table when derivations are present but union join optimization applies" in {
    val (upstreamJoin, joinSource) = buildModularDerivedJoinSource("modular_derived_union")

    dropJoinOutput(upstreamJoin)

    assertFalse(spark.catalog.tableExists(upstreamJoin.metaData.outputTable))
    assertFalse(spark.catalog.tableExists(upstreamJoin.derivedOutputTable))

    // Simulate the upstream join already being materialized through the standalone backfill path.
    new ai.chronon.spark.Join(upstreamJoin, "2023-06-02", tableUtils).computeJoin()

    assertTrue(spark.catalog.tableExists(upstreamJoin.metaData.outputTable))
    assertFalse(spark.catalog.tableExists(upstreamJoin.derivedOutputTable))

    val materialized = JoinUtils.materializeJoinSource(joinSource, "2023-06-02", tableUtils)

    assertTrue(spark.catalog.tableExists(upstreamJoin.metaData.outputTable))
    assertFalse(spark.catalog.tableExists(upstreamJoin.derivedOutputTable))
    assertEquals(upstreamJoin.metaData.outputTable, materialized.table)

    val projected = tableUtils.scanDf(materialized.query, materialized.table, range = None)
    assertTrue(projected.columns.contains("listing_id"))
  }

  it should "materialize modular upstream joins from the derived output table when derivations are planned as a separate node" in {
    val (upstreamJoin, joinSource) = buildModularDerivedJoinSource("modular_derived_separate", forceSeparateDerivedOutput = true)

    dropJoinOutput(upstreamJoin)

    assertFalse(spark.catalog.tableExists(upstreamJoin.metaData.outputTable))
    assertFalse(spark.catalog.tableExists(upstreamJoin.derivedOutputTable))

    val materialized = JoinUtils.materializeJoinSource(joinSource, "2023-06-02", tableUtils)

    assertTrue(spark.catalog.tableExists(upstreamJoin.metaData.outputTable))
    assertTrue(spark.catalog.tableExists(upstreamJoin.derivedOutputTable))
    assertEquals(upstreamJoin.finalOutputTable, materialized.table)

    val projected = tableUtils.scanDf(materialized.query, materialized.table, range = None)
    assertTrue(projected.columns.contains("listing_id"))
  }

  it should "support multi hop chaining through modular joins with derivations when union join optimization applies" in {
    val (upstreamJoin, chainedJoin, flattenedJoin) = buildModularDerivedJoinChain("modular_derived_union_chain")

    Seq(upstreamJoin, chainedJoin, flattenedJoin).foreach(dropJoinOutput)

    assertFalse(spark.catalog.tableExists(upstreamJoin.metaData.outputTable))
    assertFalse(spark.catalog.tableExists(upstreamJoin.derivedOutputTable))

    computeJoinOutput(upstreamJoin, "2023-06-02")
    val actual = new ai.chronon.spark.Join(chainedJoin, "2023-06-02", tableUtils).computeJoin()

    assertTrue(spark.catalog.tableExists(upstreamJoin.metaData.outputTable))
    assertFalse(spark.catalog.tableExists(upstreamJoin.derivedOutputTable))

    val expected = new ai.chronon.spark.Join(flattenedJoin, "2023-06-02", tableUtils).computeJoin()

    val diff = Comparison.sideBySide(actual, expected, List("user_id", "ts", "ds"))
    if (diff.count() > 0) {
      diff.show(false)
    }

    assertEquals(0, diff.count())
  }

  it should "support multi day chaining through modular joins with derivations when derivations are planned as a separate node" in {
    val (upstreamJoin, chainedJoin, flattenedJoin) =
      buildModularDerivedJoinChain("modular_derived_separate_chain", forceSeparateDerivedOutput = true)

    Seq(upstreamJoin, chainedJoin, flattenedJoin).foreach(dropJoinOutput)

    assertFalse(spark.catalog.tableExists(upstreamJoin.metaData.outputTable))
    assertFalse(spark.catalog.tableExists(upstreamJoin.derivedOutputTable))

    computeJoinOutput(upstreamJoin, "2023-06-02")
    val actual = new ai.chronon.spark.Join(chainedJoin, "2023-06-02", tableUtils).computeJoin()

    assertTrue(spark.catalog.tableExists(upstreamJoin.metaData.outputTable))
    assertTrue(spark.catalog.tableExists(upstreamJoin.derivedOutputTable))

    val expected = new ai.chronon.spark.Join(flattenedJoin, "2023-06-02", tableUtils).computeJoin()

    val diff = Comparison.sideBySide(actual, expected, List("user_id", "ts", "ds"))
    if (diff.count() > 0) {
      diff.show(false)
    }

    assertEquals(0, diff.count())
    assertEquals(Set("2023-06-01", "2023-06-02"), tableUtils.partitions(chainedJoin.metaData.outputTable).toSet)
  }

  it should "support multi day group by chaining through modular joins with derivations when derivations are planned as a separate node" in {
    val (upstreamJoin, chainedGroupBy, flattenedGroupBy) =
      buildModularDerivedGroupByChain("modular_derived_separate_group_by", forceSeparateDerivedOutput = true)

    dropJoinOutput(upstreamJoin)

    assertFalse(spark.catalog.tableExists(upstreamJoin.metaData.outputTable))
    assertFalse(spark.catalog.tableExists(upstreamJoin.derivedOutputTable))

    val queryRange = PartitionRange("2023-06-01", "2023-06-02")(tableUtils.partitionSpec)
    val actual = GroupBy.from(chainedGroupBy, queryRange, tableUtils, computeDependency = true)

    assertTrue(spark.catalog.tableExists(upstreamJoin.metaData.outputTable))
    assertTrue(spark.catalog.tableExists(upstreamJoin.derivedOutputTable))

    val expected = GroupBy.from(flattenedGroupBy, queryRange, tableUtils, computeDependency = true)

    val diff = Comparison.sideBySide(actual.inputDf, expected.inputDf, List("user_id", "ts", "ds"))
    if (diff.count() > 0) {
      diff.show(false)
    }

    assertEquals(0, diff.count())
    assertEquals(Set("2023-06-01", "2023-06-02"), tableUtils.partitions(upstreamJoin.metaData.outputTable).toSet)
    assertEquals(Set("2023-06-01", "2023-06-02"), tableUtils.partitions(upstreamJoin.derivedOutputTable).toSet)
  }
}
