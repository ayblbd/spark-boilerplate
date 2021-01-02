package me.ayoublabiad

import com.typesafe.config.ConfigFactory
import me.ayoublabiad.common.SparkSessionWrapper
import me.ayoublabiad.job.flights.FlightsJob

object Main extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()

    FlightsJob.process(spark, config)

    spark.close()
  }
}
