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

package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api._
import ai.chronon.api.DataModel.ENTITIES
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.online.serde.SparkConversions
import ai.chronon.planner.{JoinBootstrapNode, JoinPartNode}
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils._
import ai.chronon.spark.batch._
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/*
 * hashes: a list containing bootstrap hashes that represent the list of bootstrap parts that a record has matched
 *         during the bootstrap join
 * rowCount: number of records with this particular combination of hashes. for logging purpose only
 * isCovering: whether this combination of hashes fully covers the required fields of a join_part. the join_part
 *             reference itself is omitted here. but essentially each CoveringSet is pertinent to a specific join_part
 */
case class CoveringSet(hashes: Seq[String], rowCount: Long, isCovering: Boolean)

object CoveringSet {
  def toFilterExpression(coveringSets: Seq[CoveringSet]): String = {
    val coveringSetHashExpression = "(" +
      coveringSets
        .map { coveringSet =>
          val hashes = coveringSet.hashes.map("'" + _.trim + "'").mkString(", ")
          s"array($hashes)"
        }
        .mkString(", ") +
      ")"

    s"( ${Constants.MatchedHashes} IS NULL ) OR ( ${Constants.MatchedHashes} NOT IN $coveringSetHashExpression )"
  }
}

class Join(joinConf: api.Join,
           endPartition: String,
           tableUtils: TableUtils,
           skipFirstHole: Boolean = true,
           showDf: Boolean = false,
           selectedJoinParts: Option[List[String]] = None)
// we copy the joinConfCloned to prevent modification of shared joinConf's in unit tests
    extends JoinBase(joinConf.deepCopy(), endPartition, tableUtils, skipFirstHole, showDf, selectedJoinParts) {

  private implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
  private def padFields(df: DataFrame, structType: sql.types.StructType): DataFrame = {
    structType.foldLeft(df) { case (df, field) =>
      if (df.columns.contains(field.name)) {
        df
      } else {
        df.withColumn(field.name, lit(null).cast(field.dataType))
      }
    }
  }

  private def toSparkSchema(fields: Seq[StructField]): sql.types.StructType =
    SparkConversions.fromChrononSchema(StructType("", fields.toArray))

  /*
   * For all external fields that are not already populated during the bootstrap step, fill in NULL.
   * This is so that if any derivations depend on the these external fields, they will still pass and not complain
   * about missing columns. This is necessary when we directly bootstrap a derived column and skip the base columns.
   */
  private def padExternalFields(bootstrapDf: DataFrame, bootstrapInfo: BootstrapInfo): DataFrame = {

    val nonContextualFields = toSparkSchema(
      bootstrapInfo.externalParts
        .filter(!_.externalPart.isContextual)
        .flatMap(part => part.keySchema ++ part.valueSchema))
    val contextualFields = toSparkSchema(
      bootstrapInfo.externalParts.filter(_.externalPart.isContextual).flatMap(_.keySchema))

    def withNonContextualFields(df: DataFrame): DataFrame = padFields(df, nonContextualFields)

    // Ensure keys and values for contextual fields are consistent even if only one of them is explicitly bootstrapped
    def withContextualFields(df: DataFrame): DataFrame =
      contextualFields.foldLeft(df) { case (df, field) =>
        var newDf = df
        if (!newDf.columns.contains(field.name)) {
          newDf = newDf.withColumn(field.name, lit(null).cast(field.dataType))
        }
        val prefixedName = s"${Constants.ContextualPrefix}_${field.name}"
        if (!newDf.columns.contains(prefixedName)) {
          newDf = newDf.withColumn(prefixedName, lit(null).cast(field.dataType))
        }
        newDf
          .withColumn(field.name, coalesce(col(field.name), col(prefixedName)))
          .withColumn(prefixedName, coalesce(col(field.name), col(prefixedName)))
      }

    withContextualFields(withNonContextualFields(bootstrapDf))
  }

  /*
   * For all external fields that are not already populated during the group by backfill step, fill in NULL.
   * This is so that if any derivations depend on the these group by fields, they will still pass and not complain
   * about missing columns. This is necessary when we directly bootstrap a derived column and skip the base columns.
   */
  private def padGroupByFields(baseJoinDf: DataFrame, bootstrapInfo: BootstrapInfo): DataFrame = {
    val groupByFields = toSparkSchema(bootstrapInfo.joinParts.flatMap(_.valueSchema))
    padFields(baseJoinDf, groupByFields)
  }

  private def findBootstrapSetCoverings(bootstrapDf: DataFrame,
                                        bootstrapInfo: BootstrapInfo,
                                        leftRange: PartitionRange): Seq[(JoinPartMetadata, Seq[CoveringSet])] = {

    val distinctBootstrapSets: Seq[(Seq[String], Long)] =
      if (!bootstrapDf.columns.contains(Constants.MatchedHashes)) {
        Seq()
      } else {
        val collected = bootstrapDf
          .groupBy(Constants.MatchedHashes)
          .agg(count(lit(1)).as("row_count"))
          .collect()

        collected.map { row =>
          val hashes = if (row.isNullAt(0)) {
            Seq()
          } else {
            row.getAs[mutable.WrappedArray[String]](0)
          }
          (hashes.toSeq, row.getAs[Long](1))
        }
      }

    val partsToCompute: Seq[JoinPartMetadata] = {
      if (selectedJoinParts.isEmpty) {
        bootstrapInfo.joinParts
      } else {
        bootstrapInfo.joinParts.filter(part => selectedJoinParts.get.contains(part.joinPart.fullPrefix))
      }
    }

    if (selectedJoinParts.isDefined && partsToCompute.isEmpty) {
      throw new IllegalArgumentException(
        s"Selected join parts are not found. Available ones are: ${bootstrapInfo.joinParts.map(_.joinPart.fullPrefix).prettyInline}")
    }

    val coveringSetsPerJoinPart: Seq[(JoinPartMetadata, Seq[CoveringSet])] = bootstrapInfo.joinParts
      .filter(part => selectedJoinParts.isEmpty || partsToCompute.contains(part))
      .map { joinPartMetadata =>
        val coveringSets = distinctBootstrapSets.map { case (hashes, rowCount) =>
          val schema = hashes.toSet.flatMap(bootstrapInfo.hashToSchema.apply)
          val isCovering = joinPartMetadata.derivationDependencies
            .map { case (derivedField, baseFields) =>
              schema.contains(derivedField) || baseFields.forall(schema.contains)
            }
            .forall(identity)

          CoveringSet(hashes, rowCount, isCovering)
        }
        (joinPartMetadata, coveringSets)
      }

    logger.info(
      s"\n======= CoveringSet for Join ${joinMetaData.name} for PartitionRange(${leftRange.start}, ${leftRange.end}) =======\n")
    coveringSetsPerJoinPart.foreach { case (joinPartMetadata, coveringSets) =>
      logger.info(s"Bootstrap sets for join part ${joinPartMetadata.joinPart.groupBy.metaData.name}")
      coveringSets.foreach { coveringSet =>
        logger.info(
          s"CoveringSet(hash=${coveringSet.hashes.prettyInline}, rowCount=${coveringSet.rowCount}, isCovering=${coveringSet.isCovering})")
      }
    }

    coveringSetsPerJoinPart
  }

  private def getRightPartsData(leftRange: PartitionRange): Seq[(JoinPart, DataFrame)] = {
    joinConfCloned.joinParts.asScala.map { joinPart =>
      val partTable = joinConfCloned.partOutputTable(joinPart)
      val effectiveRange =
        if (joinConfCloned.left.dataModel != ENTITIES && joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT) {
          val partSpec = JoinUtils.partSnapshotSpec(joinPart)
          if (partSpec.hasSameGrid(joinOutputSpec)) {
            // Same-grid snapshot parts keep the historical behavior: read one partition back.
            JoinUtils.snapshotLookbackRange(leftRange, partSpec)
          } else {
            // Cross-grid snapshot part tables are partitioned exactly like the join output -
            // one ds per join partition, each row's snapshot time in `ts` - so read the
            // requested range as-is.
            leftRange
          }
        } else {
          leftRange
        }
      val wheres = effectiveRange.whereClauses
      val sql = QueryUtils.build(null, partTable, wheres)
      logger.info(s"Pulling data from joinPart table with: $sql")
      (joinPart, tableUtils.scanDfBase(null, partTable, List.empty, wheres, None))
    }.toSeq
  }

  override def computeFinalJoin(leftDf: DataFrame, leftRange: PartitionRange, bootstrapInfo: BootstrapInfo): Unit = {
    val bootstrapDf =
      tableUtils.scanDf(query = null, table = bootstrapTable, range = Some(leftRange)).addTimebasedColIfExists()
    val rightPartsData = getRightPartsData(leftRange)
    val joinedDfTry =
      try {
        Success(
          rightPartsData
            .foldLeft(bootstrapDf) { case (partialDf, (rightPart, rightDf)) =>
              joinWithLeft(partialDf, rightDf, rightPart)
            }
            // drop all processing metadata columns
            .drop(Constants.MatchedHashes, Constants.TimePartitionColumn))
      } catch {
        case e: Exception =>
          e.printStackTrace()
          Failure(e)
      }
    val df = processJoinedDf(joinedDfTry, leftDf, bootstrapInfo, bootstrapDf)
    JoinUtils.withFinalJoinWriteOptimizations(tableUtils) {
      df.save(outputTable, tableProps, autoExpand = true)
    }
  }

  override def computeRange(leftDf: DataFrame,
                            leftRange: PartitionRange,
                            bootstrapInfo: BootstrapInfo,
                            runSmallMode: Boolean = false,
                            usingBootstrappedLeft: Boolean = false): Option[DataFrame] = {

    val leftTaggedDf = leftDf.addTimebasedColIfExists()

    // compute bootstrap table - a left outer join between left source and various bootstrap source table
    // this becomes the "new" left for the following GB backfills
    val bootstrapDf = if (usingBootstrappedLeft) {
      leftTaggedDf
    } else {
      val bootstrapJobRange = new DateRange()
        .setStartDate(leftRange.start)
        .setEndDate(leftRange.end)

      val bootstrapMetadata = joinConfCloned.metaData.deepCopy()
      bootstrapMetadata.setName(bootstrapTable)

      val bootstrapNode = new JoinBootstrapNode()
        .setJoin(joinConfCloned)

      val bootstrapJob = new JoinBootstrapJob(bootstrapNode, bootstrapMetadata, bootstrapJobRange)
      bootstrapJob.computeBootstrapTable(leftTaggedDf, bootstrapInfo, tableProps = tableProps)

    }

    val bootStrapWithStats = DfWithStats(bootstrapDf)(leftRange.partitionSpec)

    // for each join part, find the bootstrap sets that can fully "cover" the required fields. Later we will use this
    // info to filter records that need backfills vs can be waived from backfills
    val bootstrapCoveringSets = findBootstrapSetCoverings(bootstrapDf, bootstrapInfo, leftRange)

    // compute a single bloom filter at join level if there is no bootstrap operation
    lazy val joinLevelBloomMapOpt = if (bootstrapDf.columns.contains(Constants.MatchedHashes)) {
      // do not compute if any bootstrap is involved
      None
    } else {
      val leftRowCount = bootStrapWithStats.count
      val skipBloomFilter = runSmallMode || leftRowCount > tableUtils.bloomFilterThreshold
      if (skipBloomFilter) {
        None
      } else {
        val leftBlooms = joinConfCloned.leftKeyCols.iterator
          .map { key =>
            key -> bootstrapDf.generateBloomFilter(key, leftRowCount, joinConfCloned.left.table, leftRange)
          }
          .toMap
          .asJava
        Some(leftBlooms)
      }
    }

    val joinedDfTry = Try {

      // compute join parts (GB) backfills
      // for each GB, we first find out the unfilled subset of bootstrap table which still requires the backfill.
      // we do this by utilizing the per-record metadata computed during the bootstrap process.
      // then for each GB, we compute a join_part table that contains aggregated feature values for the required key space
      // the required key space is a slight superset of key space of the left, due to the nature of using bloom-filter.
      try {
        val rightResults = bootstrapCoveringSets.flatMap { case (partMetadata, coveringSets) =>
          val joinPart = partMetadata.joinPart
          val unfilledLeftDf = findUnfilledRecords(bootStrapWithStats, coveringSets.filter(_.isCovering))

          // if the join part contains ChrononRunDs macro, then we need to make sure the join is for a single day
          val selects = Option(joinPart.groupBy.sources.toScala.map(_.query.selects).map(_.toScala))
          if (
            selects.isDefined && selects.get.nonEmpty && selects.get.exists(selectsMap =>
              Option(selectsMap).isDefined && selectsMap.values.exists(_.contains(Constants.ChrononRunDs)))
          ) {
            assert(
              leftRange.isSinglePartition,
              s"Macro ${Constants.ChrononRunDs} is only supported for single day join, current range is $leftRange")
          }

          // Small mode changes the JoinPart definition, which creates a different part table hash suffix
          // We want to make sure output table is consistent based on original semantics, not small mode behavior
          // So partTable needs to be defined BEFORE the runSmallMode logic below
          val partTable = planner.RelevantLeftForJoinPart.partTableName(joinConfCloned, joinPart)

          val bloomFilterOpt = if (runSmallMode) {
            // If left DF is small, hardcode the key filter into the joinPart's GroupBy's where clause.
            injectKeyFilter(leftDf, joinPart)
            None
          } else {
            joinLevelBloomMapOpt
          }

          val runContext =
            JoinPartJobContext(unfilledLeftDf, bloomFilterOpt, tableProps, runSmallMode)

          val skewKeys: Option[Map[String, Seq[String]]] = Option(joinConfCloned.skewKeys).map { jmap =>
            val scalaMap = jmap.toScala
            scalaMap.map { case (key, list) =>
              key -> list.asScala.toSeq
            }
          }

          val leftTable = if (usingBootstrappedLeft) {
            joinConfCloned.metaData.bootstrapTable
          } else {
            JoinUtils.computeFullLeftSourceTableName(joinConfCloned)
          }

          val joinPartJobRange = new DateRange()
            .setStartDate(leftRange.start)
            .setEndDate(leftRange.end)

          val skewKeysAsJava = skewKeys.map { keyMap =>
            keyMap.map { case (key, value) =>
              (key, value.asJava)
            }.asJava
          }.orNull

          val joinPartNodeMetadata = joinConfCloned.metaData.deepCopy()
          joinPartNodeMetadata.setName(partTable)

          val joinPartNode = new JoinPartNode()
            .setLeftDataModel(joinConfCloned.getLeft.dataModel)
            .setJoinPart(joinPart)
            .setSkewKeys(skewKeysAsJava)

          val joinPartJob = new JoinPartJob(joinPartNode, joinPartNodeMetadata, joinPartJobRange)
          val df = joinPartJob.run(Some(runContext)).map(df => joinPart -> df)

          df
        }

        // early exit if selectedJoinParts is defined. Otherwise, we combine all join parts
        if (selectedJoinParts.isDefined) return None

        // combine bootstrap table and join part tables
        // sequentially join bootstrap table and each join part table. some column may exist both on left and right because
        // a bootstrap source can cover a partial date range. we combine the columns using coalesce-rule
        Success(
          rightResults
            .foldLeft(bootstrapDf.addTimebasedColIfExists()) { case (partialDf, (rightPart, rightDf)) =>
              joinWithLeft(partialDf, rightDf, rightPart)
            }
            // drop all processing metadata columns
            .drop(Constants.MatchedHashes, Constants.TimePartitionColumn))
      } catch {
        case e: Exception =>
          e.printStackTrace()
          Failure(e)
      }
    }.get

    Some(processJoinedDf(joinedDfTry, leftTaggedDf, bootstrapInfo, bootstrapDf))
  }

  private def processJoinedDf(joinedDfTry: Try[DataFrame],
                              leftDf: DataFrame,
                              bootstrapInfo: BootstrapInfo,
                              bootstrapDf: DataFrame): DataFrame = {
    if (joinedDfTry.isFailure) throw joinedDfTry.failed.get
    val joinedDf = joinedDfTry.get
    val outputColumns = joinedDf.columns.filter(bootstrapInfo.fieldNames ++ bootstrapDf.columns)
    val finalBaseDf = padGroupByFields(joinedDf.selectExpr(outputColumns.map(c => s"`$c`"): _*), bootstrapInfo)
    val finalDf = cleanUpContextualFields(applyDerivation(finalBaseDf, bootstrapInfo, leftDf.columns),
                                          bootstrapInfo,
                                          leftDf.columns)
    JoinUtils.explainFinalDfIfEnabled(finalDf)(tableUtils)
    finalDf
  }

  private def applyDerivation(baseDf: DataFrame, bootstrapInfo: BootstrapInfo, leftColumns: Seq[String]): DataFrame = {
    if (!joinConfCloned.isSetDerivations || joinConfCloned.derivations.isEmpty) {
      return baseDf
    }

    val projections = joinConfCloned.derivations.toScala.derivationProjection(bootstrapInfo.baseValueNames)
    val projectionsMap = projections.toMap
    val baseOutputColumns = baseDf.columns.toSet

    val finalOutputColumns =
      /*
       * Loop through all columns in the base join output:
       * 1. If it is one of the value columns, then skip it here, and it will be handled later as we loop through
       *    derived columns again - derivation is a projection from all value columns to desired derived columns
       * 2.  (see case 2 below) If it is matching one of the projected output columns, then there are 2 subcases
       *     a. matching with a left column, then we handle the "coalesce" here to make sure left columns show on top
       *     b. a bootstrapped derivation case, the skip it here, and it will be handled later as
       *        loop through derivations to perform coalescing
       * 3. Else, we keep it in the final output - cases falling here are either (1) key columns, or (2)
       *    arbitrary columns selected from left.
       */
      baseDf.columns.flatMap { c =>
        if (bootstrapInfo.baseValueNames.contains(c)) {
          None
        } else if (projectionsMap.contains(c)) {
          if (leftColumns.contains(c)) {
            Some(coalesce(col(c), expr(projectionsMap(c))).as(c))
          } else {
            None
          }
        } else {
          Some(col(c))
        }
      } ++
        /*
         * Loop through all clauses in derivation projections:
         * 1. (see case 2 above) If it is matching one of the projected output columns, then there are 2 sub-cases
         *     a. matching with a left column, then we skip since it is handled above
         *     b. a bootstrapped derivation case (see case 2 below), then we do the coalescing to achieve the bootstrap
         *        behavior.
         * 2. Else, we do the standard projection.
         */
        projections
          .flatMap { case (name, expression) =>
            if (baseOutputColumns.contains(name)) {
              if (leftColumns.contains(name)) {
                None
              } else {
                Some(coalesce(col(name), expr(expression)).as(name))
              }
            } else {
              Some(expr(expression).as(name))
            }
          }

    val result = baseDf.select(finalOutputColumns: _*)
    if (showDf) {
      logger.info(s"printing results for join: ${joinConfCloned.metaData.name}")
      result.prettyPrint()
    }
    result
  }

  /*
   * Remove extra contextual keys unless it is a result of derivations or it is a column from left
   */
  private def cleanUpContextualFields(finalDf: DataFrame,
                                      bootstrapInfo: BootstrapInfo,
                                      leftColumns: Seq[String]): DataFrame = {

    val contextualNames =
      bootstrapInfo.externalParts.filter(_.externalPart.isContextual).flatMap(_.keySchema).map(_.name)
    val projections = if (joinConfCloned.isSetDerivations) {
      joinConfCloned.derivations.toScala.derivationProjection(bootstrapInfo.baseValueNames).map(_._1)
    } else {
      Seq()
    }
    contextualNames.foldLeft(finalDf) { case (df, name) =>
      if (leftColumns.contains(name) || projections.contains(name)) {
        df
      } else {
        df.drop(name)
      }
    }
  }

  /*
   * We leverage metadata information created from the bootstrap step to tell which record was already joined to a
   * bootstrap source, and therefore had certain columns pre-populated. for these records and these columns, we do not
   * need to run backfill again. this is possible because the hashes in the metadata columns can be mapped back to
   * full schema information.
   */
  private def findUnfilledRecords(bootstrapDfWithStats: DfWithStats,
                                  coveringSets: Seq[CoveringSet]): Option[DfWithStats] = {
    val bootstrapDf = bootstrapDfWithStats.df
    if (coveringSets.isEmpty || !bootstrapDf.columns.contains(Constants.MatchedHashes)) {
      // this happens whether bootstrapParts is NULL for the JOIN and thus no metadata columns were created
      return Some(bootstrapDfWithStats)
    }
    val filterExpr = CoveringSet.toFilterExpression(coveringSets)
    logger.info(s"Using covering set filter: $filterExpr")
    val filteredDf = bootstrapDf.where(filterExpr)
    val filteredCount = filteredDf.count()
    if (bootstrapDfWithStats.count == filteredCount) { // counting is faster than computing stats
      Some(bootstrapDfWithStats)
    } else if (filteredCount == 0) {
      None
    } else {
      Some(DfWithStats(filteredDf)(bootstrapDfWithStats.partitionSpec))
    }
  }
}
