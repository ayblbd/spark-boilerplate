package me.ayoublabiad.job.flights

import com.typesafe.config.Config
import me.ayoublabiad.io.Read.readFromCsvFileWithSchema
import me.ayoublabiad.job.Job
import org.apache.spark.sql.{DataFrame, SparkSession}

object FlightsJob extends Job {
  override def process(spark: SparkSession, config: Config): Unit = {
    val schema = "destination STRING, origin STRING, count INT"
    val flights: DataFrame = readFromCsvFileWithSchema("/2015-summary.csv", schema)

    val destinationsWithTotalCount = FlightsTransformation.getDestinationsWithTotalCount(flights)

    destinationsWithTotalCount.show(10, false)
  }
}
