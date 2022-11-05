package me.ayoublabiad.job.flights

import me.ayoublabiad.common.SparkSessionWrapper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, sum}

object FlightsTransformation extends SparkSessionWrapper {
  def getDestinationsWithTotalCount(flights: DataFrame): DataFrame =
    flights
      .groupBy("destination")
      .agg(sum("count").alias("total_count"))

  def addTotalCountColumn(flights: DataFrame): DataFrame = {
    val windowExpression = Window.partitionBy("destination")

    flights
      .withColumn("total_count", count("count").over(windowExpression))
  }
}
