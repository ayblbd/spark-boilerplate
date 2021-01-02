package me.ayoublabiad.job

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

trait Job {
  def process(spark: SparkSession, config: Config): Unit
}
