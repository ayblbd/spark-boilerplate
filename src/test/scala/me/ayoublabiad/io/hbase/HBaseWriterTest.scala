package me.ayoublabiad.io.hbase

import me.ayoublabiad.common.BaseTest
import me.ayoublabiad.io.hbase.HBaseWriter.{filterKeyEmptyNullValues, hbaseColumnsMapping}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
class HBaseWriterTest extends BaseTest {
  "hbaseColumnsMapping" should "return the right mapping" in {
    val df = createEmptyDataFrame(
      "integer",
      "long",
      "boolean",
      "double",
      "float",
      "string",
      "decimal",
      "array"
    ).withColumn("integer", col("integer").cast(IntegerType))
      .withColumn("long", col("long").cast(LongType))
      .withColumn("boolean", col("boolean").cast(BooleanType))
      .withColumn("double", col("double").cast(DoubleType))
      .withColumn("float", col("float").cast(FloatType))
      .withColumn("decimal", col("decimal").cast("decimal(12,7)"))
      .withColumn("array", array("array").cast("array<string>"))
    val actual = hbaseColumnsMapping(df, "data")
    val expected =
      """array BINARY data:array,boolean BOOLEAN data:boolean,decimal STRING data:decimal,double DOUBLE data:double,float FLOAT data:float,integer INTEGER data:integer,long LONG data:long,string STRING data:string""".stripMargin
    assert(actual == expected)
  }

  "filterEmptyNullValues" should "filter empty and null values" in {
    val df = createDataFrame(
      Map(
        "column" -> "value"
      ),
      Map(
        "column" -> ""
      ),
      Map(
        "column" -> null
      ),
      Map(
        "column" -> " "
      )
    )
    val actual = filterKeyEmptyNullValues(df, Seq("column", "column1"))
    val expected = createDataFrame(
      Map(
        "column" -> "value"
      )
    )
    assertDataFrameEquality(actual, expected)
  }

  "stringToColumn" should "return the right columns" in {
    val df = createEmptyDataFrame("column1", "column2")
    val actual = HBaseWriter.stringToColumn(df.columns, Seq("column1", "column2", "column3"))
    val expected = Seq(col("column1"), col("column2"), lit("column3"))
    assert(actual == expected)
  }

  "buildKey" should "generate the right key" in {
    import me.ayoublabiad.io.hbase.HBaseWriter._
    val df = createDataFrame(
      Map(
        "numero_compte" -> "00000000222222222"
      ),
      Map(
        "numero_compte" -> null
      ),
      Map(
        "numero_compte" -> " "
      )
    )
    val actual = df.buildKey(
      "numero_compte",
      "COMPTE",
      "CUMUL",
      "ANNUEL"
    )
    val expected = createDataFrame(
      Map(
        "key" -> "00000000222222222-COMPTE-CUMUL-ANNUEL",
        "numero_compte" -> "00000000222222222"
      )
    )
    assertDataFrameEquality(actual, expected)
  }

  "dataframe" should "be written to hbase" in {
    import me.ayoublabiad.io.hbase.HBaseWriter._
    val df = createDataFrame(
      Map(
        "key" -> "KEY-1",
        "integer" -> 1,
        "long" -> 2L,
        "boolean" -> true,
        "double" -> 5555.5555d,
        "float" -> 6666.7777f,
        "string" -> "string",
        "date" -> "2012-12-12",
        "timestamp" -> "2012-12-13 00:10:00",
        "decimal" -> "88888888888888888888888.88888888888"
      )
    ).withColumn("integer", col("integer").cast(IntegerType))
      .withColumn("date", col("date").cast(DateType))
      .withColumn("timestamp", col("timestamp").cast(TimestampType))
      .withColumn("decimal", col("decimal").cast(DecimalType(38, 18)))

    val options = Map(
      ("hbase.table", "test")
    )

    df.buildKey("KEY", "integer").toHbase("cf", options)

    val actual = spark.read
      .format("org.apache.hadoop.hbase.spark")
      .option(
        "hbase.columns.mapping",
        s"key STRING :key, ${hbaseColumnsMapping(df, "cf")}"
      )
      .options(options)
      .load()
      .withColumn("decimal", col("decimal").cast(DecimalType(38, 18)))

    assertDataFrameEquality(actual, df)

  }
}
