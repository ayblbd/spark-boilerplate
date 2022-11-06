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

    def toHbase(cf: String = "d", options: Map[String, String]): Unit = {
      val changedTypes = changeTypes(df)

      changedTypes.write
        .format("org.apache.hadoop.hbase.spark")
        .option("hbase.spark.use.hbasecontext", value = false)
        .option(
          "hbase.columns.mapping",
          f"key STRING :key, ${hbaseColumnsMapping(changedTypes, cf)}"
        )
        .options(options)
        .save()
    }
  }

  def stringToColumn(
    dfColumns: Seq[String],
    columns: Seq[String]
  ): Seq[Column] =
    columns.map(c => if (dfColumns.contains(c)) col(c) else lit(c))

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
        case StructField(c: String, _: DecimalType, _, _) =>
          f"""$c DOUBLE $cf:$c"""
      }
      .mkString(",")

  def changeTypes(df: DataFrame): DataFrame = {
    val schema = df.schema.fields.map {
      case sf @ (StructField(_: String, _: DateType, _, _) | StructField(_: String, _: TimestampType, _, _)) =>
        col(sf.name).cast(StringType).as(sf.name)
      case StructField(name: String, _: DecimalType, _, _) =>
        col(name).cast(DoubleType)
      case StructField(name: String, _: StringType, _, _) =>
        when(trim(col(name)) =!= lit(""), col(name)).as(name)
      case f => col(f.name)
    }

    df.select(schema: _*)
  }

  def filterKeyEmptyNullValues(df: DataFrame, columns: Seq[String]): DataFrame =
    columns
      .filter(c => df.columns.contains(c))
      .foldLeft(df)((df, c) => df.filter(col(c).isNotNull && trim(col(c)) =!= lit("")))
}
