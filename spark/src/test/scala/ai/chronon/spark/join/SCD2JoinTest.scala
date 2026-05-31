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
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps}
import ai.chronon.api.planner.RelevantLeftForJoinPart
import ai.chronon.api.{ThriftJsonCodec, TimeUnit, Window}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.Join

case class SCD2ContractRow(user_id: String,
                           contract_id: String,
                           valid_from_ts: Long,
                           valid_to_ts: java.lang.Long,
                           amount: Double,
                           status: String,
                           birth_ts: Long,
                           ds: String)

case class SCD2QueryRow(user_id: String, ts: Long, ds: String)

class SCD2JoinTest extends BaseJoinTest {

  it should "join SCD2 helper compiled output with as-of semantics" in {
    import spark.implicits._

    val confPath = this.getClass.getClassLoader.getResource("scd2/compiled_join").getPath
    val joinConf = ThriftJsonCodec.fromJsonFile[api.Join](confPath, check = false)

    val contractsTable = s"$namespace.scd2_contracts"
    val queriesTable = s"$namespace.scd2_queries"
    val joinPart = joinConf.joinParts.get(0)
    assert(joinPart.groupBy.getAccuracy == api.Accuracy.TEMPORAL)
    assert(joinPart.groupBy.inferredAccuracy == api.Accuracy.TEMPORAL)
    val partTable = s"$namespace.${RelevantLeftForJoinPart.partTableName(joinConf, joinPart)}"

    Seq(contractsTable, queriesTable, joinConf.metaData.outputTable, joinConf.metaData.bootstrapTable, partTable)
      .foreach(table => spark.sql(s"DROP TABLE IF EXISTS $table"))

    val day1 = tableUtils.partitionSpec.minus(today, new Window(2, TimeUnit.DAYS))
    val day2 = tableUtils.partitionSpec.plus(day1, new Window(1, TimeUnit.DAYS))
    val day3 = today
    val day1Ts = tableUtils.partitionSpec.epochMillis(day1)
    val day2Ts = tableUtils.partitionSpec.epochMillis(day2)
    val day3Ts = tableUtils.partitionSpec.epochMillis(day3)
    val hour = 60L * 60L * 1000L

    Seq(
      SCD2ContractRow("u1", "c1", day1Ts, day2Ts, 10.0, "basic", day1Ts, day1),
      SCD2ContractRow("u1", "c1", day2Ts, day3Ts, 20.0, "pro", day1Ts, day2),
      SCD2ContractRow("u1", "c2", day2Ts, null, 5.0, "addon", day2Ts, day2),
      SCD2ContractRow("u2", "c3", day1Ts, null, 7.0, "basic", day1Ts, day1)
    ).toDF().save(contractsTable)

    Seq(
      SCD2QueryRow("u1", day1Ts + hour, day1),
      SCD2QueryRow("u1", day2Ts + hour, day2),
      SCD2QueryRow("u1", day3Ts + hour, day3),
      SCD2QueryRow("u2", day2Ts + hour, day2),
      SCD2QueryRow("u3", day2Ts + hour, day2)
    ).toDF().save(queriesTable)

    val result = new Join(joinConf, endPartition = day3, tableUtils = tableUtils)
      .computeJoin(overrideStartPartition = Some(day1))
      .select(
        "user_id",
        "ts",
        "contract_user_id_amount_sum",
        "contract_user_id_amount_avg",
        "contract_user_id_count",
        "contract_user_id_exists",
        "contract_user_id_count_by_birth_ts_1d",
        "contract_user_id_status_count_distinct"
      )
      .collect()
      .map { row =>
        def doubleOpt(column: String): Option[Double] = {
          val index = row.fieldIndex(column)
          if (row.isNullAt(index)) None else Some(row.getDouble(index))
        }
        row.getAs[String]("user_id") -> row.getAs[Long]("ts") -> (
          doubleOpt("contract_user_id_amount_sum"),
          doubleOpt("contract_user_id_amount_avg"),
          row.getAs[Long]("contract_user_id_count"),
          row.getAs[Boolean]("contract_user_id_exists"),
          row.getAs[Long]("contract_user_id_count_by_birth_ts_1d"),
          row.getAs[Long]("contract_user_id_status_count_distinct")
        )
      }
      .toMap

    assert(result(("u1", day1Ts + hour)) == (Some(10.0), Some(10.0), 1L, true, 1L, 1L))
    assert(result(("u1", day2Ts + hour)) == (Some(25.0), Some(12.5), 2L, true, 1L, 2L))
    assert(result(("u1", day3Ts + hour)) == (Some(5.0), Some(5.0), 1L, true, 0L, 1L))
    assert(result(("u2", day2Ts + hour)) == (Some(7.0), Some(7.0), 1L, true, 0L, 1L))
    assert(result(("u3", day2Ts + hour)) == (None, None, 0L, false, 0L, 0L))
  }
}
