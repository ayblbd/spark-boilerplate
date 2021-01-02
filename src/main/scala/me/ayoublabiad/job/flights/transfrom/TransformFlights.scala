package me.ayoublabiad.job.flights.transfrom

import me.ayoublabiad.common.SparkSessionWrapper
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}

object TransformFlights extends SparkSessionWrapper {
  def getDestinationsWithTotalCount(flights: DataFrame): DataFrame =
    flights
      .groupBy("destination")
      .agg(sum("count").alias("total_count"))

  def getParamFromTable(spark: SparkSession, dataFrame: DataFrame, paramColumnName: String): Long = {
    import spark.implicits._
    dataFrame
      .select(paramColumnName)
      .as[Long]
      .collect match {
      case array if array.isEmpty => throw new Exception("Threshold not found.")
      case array => array(0)
    }
  }
}
