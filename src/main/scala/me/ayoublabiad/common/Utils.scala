package me.ayoublabiad.common

import org.apache.spark.sql.DataFrame

object Utils {
  def getParamFromTable(dataFrame: DataFrame, paramColumnName: String): Long =
    dataFrame
      .select(paramColumnName)
      .collect
      .headOption
      .map(_.getLong(0))
      .getOrElse(throw new Exception("Threshold not found."))
}
