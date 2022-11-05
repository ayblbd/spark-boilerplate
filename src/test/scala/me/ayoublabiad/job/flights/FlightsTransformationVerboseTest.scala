package me.ayoublabiad.job.flights

import dijon.{arr, obj}
import me.ayoublabiad.common.BaseTest
import me.ayoublabiad.job.flights.FlightsTransformation.getDestinationsWithTotalCount
import org.apache.spark.sql.DataFrame

class FlightsTransformationVerboseTest extends BaseTest {
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

    assertDataFrameEquality(actual, expected)
  }

}
