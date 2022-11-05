package me.ayoublabiad.common

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import dijon.{arr, obj, SomeJson}
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

trait BaseTest extends AnyFlatSpec with SparkSessionTestWrapper with DataFrameComparer {

  import spark.implicits._

  def createDataFrame(data: SomeJson): DataFrame = {
    spark.read.json(Seq(data.toString()).toDS)
  }

  def createEmptyDataFrame(columnNames: String*): DataFrame = {
    val columns: Seq[SomeJson] = columnNames
      .map(col => obj(col -> None))
    createDataFrame(columns: _*)
  }

  def createDataFrame(data: SomeJson*): DataFrame = {
    spark.read.json(Seq(arr(data: _*).toString()).toDS)
  }

  def assertDataFrameEquality(actual: DataFrame, expected: DataFrame): Unit =
    assertSmallDataFrameEquality(actual, expected, ignoreNullable = true, orderedComparison = false)
}
