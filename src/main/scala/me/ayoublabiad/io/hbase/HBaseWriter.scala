package me.ayoublabiad.io.hbase

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

object HBaseWriter {
  implicit class HBaseWriterOps(df: DataFrame) {
    def buildKey(columns: String*): DataFrame =
      filterKeyEmptyNullValues(df, columns).withColumn(
        "key",
        concat_ws(
          "-",
          stringToColumn(df.columns, columns): _*
        )
      )

    def toHbase(cf: String = "d", options: Map[String, String]): Unit =
      df.write
        .format("org.apache.hadoop.hbase.spark")
        .option("hbase.spark.use.hbasecontext", value = false)
        .option(
          "hbase.columns.mapping",
          f"key STRING :key, ${hbaseColumnsMapping(df, cf)}"
        )
        .options(options)
        .save()
  }

  def stringToColumn(
    dfColumns: Seq[String],
    columns: Seq[String]
  ): Seq[Column] =
    columns.map(c => if (dfColumns.contains(c)) col(c) else lit(c))

  def filterKeyEmptyNullValues(df: DataFrame, columns: Seq[String]): DataFrame =
    columns
      .filter(c => df.columns.contains(c))
      .foldLeft(df)((df, c) => df.filter(col(c).isNotNull && trim(col(c)) =!= lit("")))

  def hbaseColumnsMapping(df: DataFrame, cf: String): String =
    df.schema.fields
      .filter(_.name != "key")
      .map {
        case StructField(c: String, _: DoubleType, _, _) =>
          f"""$c DOUBLE $cf:$c"""
        case StructField(c: String, _: IntegerType, _, _) =>
          f"""$c INTEGER $cf:$c"""
        case StructField(c: String, _: LongType, _, _) =>
          f"""$c LONG $cf:$c"""
        case StructField(c: String, _: BooleanType, _, _) =>
          f"""$c BOOLEAN $cf:$c"""
        case sf @ (StructField(_: String, _: ArrayType, _, _) | StructField(_: String, _: ByteType, _, _)) =>
          f"""${sf.name} BINARY $cf:${sf.name}"""
        case StructField(c: String, _: StringType, _, _) =>
          f"""$c STRING $cf:$c"""
        case StructField(c: String, _: FloatType, _, _) =>
          f"""$c FLOAT $cf:$c"""
        case StructField(c: String, _: ShortType, _, _) =>
          f"""$c SHORT $cf:$c"""
        case StructField(c: String, _: TimestampType, _, _) =>
          f"""$c TIMESTAMP $cf:$c"""
        case StructField(c: String, _: DateType, _, _) =>
          f"""$c DATE $cf:$c"""
        case StructField(c: String, _: DecimalType, _, _) =>
          f"""$c STRING $cf:$c"""
      }
      .mkString(",")
}
