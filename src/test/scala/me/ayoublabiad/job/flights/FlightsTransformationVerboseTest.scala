package me.ayoublabiad.job.flights

import me.ayoublabiad.common.BaseTest
import me.ayoublabiad.job.flights.FlightsTransformation.getDestinationsWithTotalCount
import org.apache.spark.sql.DataFrame

class FlightsTransformationVerboseTest extends BaseTest {
  "getDestinationsWithTotalCount" should "return a dataframe with total count of aggregated flights by destination" in {

    val input: DataFrame = createDataFrame(
      Map(
        "destination" -> "morocco",
        "origin" -> "spain",
        "count" -> 3
      ),
      Map(
        "destination" -> "morocco",
        "origin" -> "egypt",
        "count" -> 5
      ),
      Map(
        "destination" -> "france",
        "origin" -> "germany",
        "count" -> 10
      )
    )

    val actual: DataFrame = getDestinationsWithTotalCount(input)
    val expected: DataFrame = createDataFrame(
      Map(
        "destination" -> "france",
        "total_count" -> 10
      ),
      Map(
        "destination" -> "morocco",
        "total_count" -> 8
      )
    )

    assertDataFrameEquality(actual, expected)
  }

}
