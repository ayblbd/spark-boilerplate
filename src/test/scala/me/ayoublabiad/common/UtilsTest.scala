package me.ayoublabiad.common

import me.ayoublabiad.common.Utils.getParamFromTable
import org.apache.spark.sql.DataFrame

class UtilsTest extends BaseTest {
  import spark.implicits._
  "getParamFromTable" should "turn the threshold if it exists" in {
    val input: DataFrame = createDataFrame(
      Map(
        "minFlightsThreshold" -> 4
      )
    )

    val threshold: Long = getParamFromTable(input, "minFlightsThreshold")

    assert(threshold == 4)
  }

  "getParamFromTable" should "throw an Exception if the threshold doesn't exist" in {
    val input: DataFrame = Seq.empty[Long].toDF("minFlightsThreshold")

    val caught =
      intercept[Exception] {
        getParamFromTable(input, "minFlightsThreshold")
      }

    assert(caught.getMessage == "Threshold not found.")
  }
}
