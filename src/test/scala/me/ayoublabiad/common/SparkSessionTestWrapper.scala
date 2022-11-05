package me.ayoublabiad.common

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkTestingSession")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
}
