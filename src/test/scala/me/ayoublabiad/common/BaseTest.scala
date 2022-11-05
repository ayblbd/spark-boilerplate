package me.ayoublabiad.common

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.scalatest.flatspec.AnyFlatSpec

trait BaseTest extends AnyFlatSpec with SparkSessionTestWrapper with DataFrameComparer {

  import spark.implicits._
  lazy val emptyDataFrame: DataFrame = spark.emptyDataFrame

  def createEmptyDataFrame(columnNames: String*): DataFrame =
    createDataFrame(
      columnNames
        .map(col => Map(col -> null)): _*)

  def createDataFrame(data: Map[String, Any]*): DataFrame =
    spark.read.json(Seq(Json(DefaultFormats).write(data)).toDS)

  def assertDataFrameEquality(
    actual: DataFrame,
    expected: DataFrame,
    ignoreNullable: Boolean = true,
    debug: Boolean = false
  ): Any = {
    def sortColumns(df: DataFrame): DataFrame =
      df.select(df.columns.sorted.map(col): _*)
    val ActualWithSortedColumns = sortColumns(actual)
    val expectedWithSortedColumns = sortColumns(expected)
    if (debug) {
      println("Actual: ")
      ActualWithSortedColumns.show(false)
      println("Expected: ")
      expectedWithSortedColumns.show(false)
    }
    assertSmallDataFrameEquality(
      ActualWithSortedColumns,
      expectedWithSortedColumns,
      ignoreNullable,
      orderedComparison = false
    )
  }

}
