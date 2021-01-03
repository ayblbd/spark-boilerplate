package me.ayoublabiad.common

import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {
  def getParamFromTable(spark: SparkSession, dataFrame: DataFrame, paramColumnName: String): Long = {
    import spark.implicits._
    val params: Array[Long] = dataFrame.select(paramColumnName).as[Long].collect
    if (params.isEmpty) {
      throw new Exception("Threshold not found.")
    } else {
      params(0)
    }
  }
}
