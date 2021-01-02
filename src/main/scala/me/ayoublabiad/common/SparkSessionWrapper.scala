package me.ayoublabiad.common

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  lazy val spark = SparkSession
    .builder()
    .appName("FlightsJob")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
}
