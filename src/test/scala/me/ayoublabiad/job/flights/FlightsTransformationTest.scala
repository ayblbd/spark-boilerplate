package me.ayoublabiad.job.flights

import me.ayoublabiad.common.BaseTest
import me.ayoublabiad.job.flights.FlightsTransformation.getDestinationsWithTotalCount
import org.apache.spark.sql.DataFrame

class FlightsTransformationTest extends BaseTest {
  import spark.implicits._

  it should "return a dataframe with total count of aggregated flights by destination" in {

    val input: DataFrame = Seq(
      ("morocco", "spain", 3),
      ("morocco", "egypt", 5),
      ("france", "germany", 10)
    ).toDF("destination", "origin", "count")

    val actual: DataFrame = getDestinationsWithTotalCount(input)

    val expected: DataFrame = Seq(
      ("france", 10L),
      ("morocco", 8L)
    ).toDF("destination", "total_count")

    assertSmallDataFrameEquality(actual, expected, ignoreNullable = true, orderedComparison = false)
  }
}
