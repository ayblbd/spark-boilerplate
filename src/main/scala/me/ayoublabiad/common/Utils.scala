package me.ayoublabiad.common

import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {
  def getParamFromTable(spark: SparkSession, dataFrame: DataFrame, paramColumnName: String): Long = {
    import spark.implicits._
    dataFrame
      .select(paramColumnName)
      .as[Long]
      .collect()
      .headOption
      .getOrElse(throw new Exception("Threshold not found."))
  }
}
