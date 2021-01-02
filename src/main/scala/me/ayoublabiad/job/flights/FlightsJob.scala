package me.ayoublabiad.job.flights

import com.typesafe.config.Config
import me.ayoublabiad.io.Read.readTableFromHive
import me.ayoublabiad.io.Write
import me.ayoublabiad.job.Job
import me.ayoublabiad.job.flights.transfrom.TransformFlights
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object FlightsJob extends Job {
  override def process(spark: SparkSession, config: Config): Unit = {
    val db: String = config.getString("settings.database")
    val flights: DataFrame = readTableFromHive(db, "flights")
    val params: DataFrame = readTableFromHive(db, "params")

    val minFlightsThreshold: Long = TransformFlights.getParamFromTable(spark, params, "minFlightsThreshold")

    val destinationsWithTotalCount = TransformFlights.getDestinationsWithTotalCount(flights)

    val filteredDestinations = destinationsWithTotalCount
      .filter(col("total_count") >= lit(minFlightsThreshold))

    Write.writeTohive(spark, filteredDestinations, db, "filtered_flights")
  }
}
