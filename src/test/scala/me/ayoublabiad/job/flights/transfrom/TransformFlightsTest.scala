package me.ayoublabiad.job.flights.transfrom

import dijon.{arr, obj}
import me.ayoublabiad.common.BaseTest
import me.ayoublabiad.job.flights.transfrom.TransformFlights.{getDestinationsWithTotalCount, getParamFromTable}
import org.apache.spark.sql.DataFrame

class TransformFlightsTest extends BaseTest {
  import spark.implicits._
  "getDestinationsWithTotalCount" should "return a dataframe with total count of aggregated flights by destination" in {

    val input: DataFrame = createDataFrame(
      obj(
        "destination" -> "morocco",
        "origin" -> "spain",
        "count" -> 3
      ),
      obj(
        "destination" -> "morocco",
        "origin" -> "egypt",
        "count" -> 5
      ),
      obj(
        "destination" -> "france",
        "origin" -> "germany",
        "count" -> 10
      )
    )

    val actual: DataFrame = getDestinationsWithTotalCount(input)
    val expected: DataFrame = createDataFrame(
      arr(
        obj(
          "destination" -> "france",
          "total_count" -> 10
        ),
        obj(
          "destination" -> "morocco",
          "total_count" -> 8
        )
      )
    )

    assertSmallDataFrameEquality(actual, expected, ignoreNullable = true, orderedComparison = false)
  }

  "getParamFromTable" should "turn the threshold if it exists" in {
    val input: DataFrame = createDataFrame(
      obj(
        "minFlightsThreshold" -> 4
      )
    )

    val threshold: Long = getParamFromTable(spark, input, "minFlightsThreshold")

    assert(threshold == 4)
  }

  "getParamFromTable" should "throw an Exception if the threshold doesn't exist" in {
    val input: DataFrame = Seq.empty[Long].toDF("minFlightsThreshold")

    val caught =
      intercept[Exception] {
        getParamFromTable(spark, input, "minFlightsThreshold")
      }

    assert(caught.getMessage == "Threshold not found.")
  }

}
