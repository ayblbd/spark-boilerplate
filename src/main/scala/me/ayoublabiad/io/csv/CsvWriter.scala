package me.ayoublabiad.io.csv

import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvWriter {
  def writeToCsv(dataFrame: DataFrame, location: String): Unit =
    dataFrame.write
      .option("header", "true")
      .csv(location)

  def writeTohive(spark: SparkSession, dataFrame: DataFrame, database: String, tableName: String): Unit = {
    dataFrame.createGlobalTempView(s"${tableName}_view")
    spark.sql(s"DROP TABLE IF EXISTS $database.$tableName")
    spark.sql(s"CREATE TABLE $database.$tableName AS SELECT * FROM ${tableName}_view")
  }
}
