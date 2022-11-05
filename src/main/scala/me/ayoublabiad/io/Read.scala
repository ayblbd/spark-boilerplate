package me.ayoublabiad.io

import me.ayoublabiad.Main.spark
import org.apache.spark.sql.DataFrame

object Read {

  def readFromCsvFileWithSchema(location: String, schema: String): DataFrame =
    spark.read
      .option("header", "true")
      .schema(schema)
      .csv(getClass.getResource(location).getPath)

  def readFromCsvFile(location: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(location)

  def readTableFromHive(database: String, tableName: String): DataFrame =
    spark.table(s"$database.$tableName")
}
