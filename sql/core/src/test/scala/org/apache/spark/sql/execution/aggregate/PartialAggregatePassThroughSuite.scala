/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class PartialAggregatePassThroughSuite
  extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {

  private val minRows = "spark.sql.aggregate.partialPassThrough.minRows"
  private val minRatio = "spark.sql.aggregate.partialPassThrough.minRatio"

  /** Output rows of the partial hash aggregates of the executed query. */
  private def partialOutputRows(df: DataFrame): Long = {
    df.collect()
    collect(df.queryExecution.executedPlan) {
      case a: HashAggregateExec if a.aggregateExpressions.exists(_.mode == Partial) =>
        a.metrics("numOutputRows").value
    }.sum
  }

  /**
   * Runs the query with pass-through off and then on (after 64 rows) for each map setup, checks
   * that the results are the same and returns the partial aggregates' output rows (off, on).
   */
  private def checkPassThrough(query: => DataFrame): (Long, Long) = {
    var expected: Seq[Row] = Nil
    var rowsOff = 0L
    withSQLConf(minRows -> "0") {
      expected = query.collect().toSeq
      rowsOff = partialOutputRows(query)
    }
    var rowsOn = -1L
    for (fastMap <- Seq("true", "false"); batch <- Seq("16", "0")) {
      withSQLConf(minRows -> "64", minRatio -> "0.5",
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> fastMap,
          "spark.sql.codegen.aggregate.prefetchBatch" -> batch,
          "spark.sql.codegen.aggregate.prefetchMinKeys" -> "16") {
        checkAnswer(query, expected)
        val rows = partialOutputRows(query)
        assert(rowsOn < 0 || rows == rowsOn, s"fastMap=$fastMap batch=$batch")
        rowsOn = rows
      }
    }
    (rowsOff, rowsOn)
  }

  private def keyed(n: Int): DataFrame = spark.range(0, n, 1, 4).selectExpr(
    "IF(id % 97 = 0, NULL, id % 9001) AS k",
    "CAST(id % 9001 AS STRING) AS s",
    "id AS v",
    "IF(id % 5 = 0, NULL, CAST(id AS DECIMAL(12, 2))) AS d",
    "CAST(id % 13 AS DOUBLE) AS x")

  test("high-cardinality keys pass through and give the same result") {
    val (off, on) = checkPassThrough {
      keyed(40000).groupBy("k").agg(
        "v" -> "sum", "v" -> "count", "d" -> "avg", "d" -> "sum", "x" -> "min", "x" -> "max")
    }
    assert(on > off)
  }

  test("string keys, filters and several functions") {
    val (off, on) = checkPassThrough {
      keyed(40000).createOrReplaceTempView("t")
      spark.sql(
        """SELECT s, count(*), count(d) FILTER (WHERE x > 3), sum(v) FILTER (WHERE k IS NULL),
          |  avg(x), max(d)
          |FROM t GROUP BY s""".stripMargin)
    }
    assert(on > off)
  }

  test("keys that repeat do not pass through") {
    val (off, on) = checkPassThrough {
      spark.range(0, 40000, 1, 4).selectExpr("id % 10 AS k", "id AS v").groupBy("k").sum("v")
    }
    assert(on == off)
  }

  test("children that produce several rows per input row") {
    // Broadcast join with duplicate build keys: up to three output rows per streamed row.
    checkPassThrough {
      val build = spark.range(0, 3000).selectExpr("id % 1000 AS k", "id AS w")
      keyed(40000).join(build.hint("broadcast"), "k").groupBy("k", "w").agg("v" -> "sum")
    }
    // Expand (rollup): several rows per input row.
    checkPassThrough {
      keyed(40000).rollup("k", "s").agg("v" -> "sum", "d" -> "avg")
    }
  }
}
