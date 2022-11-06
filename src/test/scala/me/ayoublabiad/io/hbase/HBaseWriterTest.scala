package me.ayoublabiad.io.hbase

import me.ayoublabiad.common.BaseTest
import me.ayoublabiad.io.hbase.HBaseWriter.{changeTypes, filterKeyEmptyNullValues, hbaseColumnsMapping}
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
      """array BINARY data:array,boolean BOOLEAN data:boolean,decimal DOUBLE data:decimal,double DOUBLE data:double,float FLOAT data:float,integer INTEGER data:integer,long LONG data:long,string STRING data:string""".stripMargin
    assert(actual == expected)
  }

  "changeTypes" should "generate the right schema" in {
    val df = createEmptyDataFrame(
      "integer",
      "long",
      "boolean",
      "double",
      "float",
      "string",
      "date",
      "timestamp",
      "decimal",
      "array",
      "byte"
    ).withColumn("integer", col("integer").cast(IntegerType))
      .withColumn("long", col("long").cast(LongType))
      .withColumn("boolean", col("boolean").cast(BooleanType))
      .withColumn("double", col("double").cast(DoubleType))
      .withColumn("float", col("float").cast(FloatType))
      .withColumn("date", col("date").cast(DateType))
      .withColumn("timestamp", col("timestamp").cast(TimestampType))
      .withColumn("decimal", col("decimal").cast("decimal(12,7)"))
      .withColumn("array", array("array").cast("array<string>"))
      .withColumn("byte", col("byte").cast(ByteType))

    val actual = changeTypes(df).schema.fields.map(_.dataType).toSet
    val expected =
      Set(
        StringType,
        LongType,
        IntegerType,
        DoubleType,
        ArrayType(StringType, containsNull = true),
        BooleanType,
        FloatType,
        ByteType
      )

    assert(actual == expected)
  }

  "changeTypes" should "generate the right dataframe" in {
    val df = createDataFrame(
      Map(
        "integer" -> 1,
        "long" -> 2,
        "boolean" -> true,
        "double" -> 5555.5555,
        "float" -> 6666.7777,
        "string" -> "string",
        "date" -> "2012-12-12",
        "timestamp" -> "2012-12-13 00:10:00",
        "decimal" -> "88888888888888888888.888888888888888888",
        "array" -> Array("name"),
        "byte" -> 1
      )
    ).withColumn("integer", col("integer").cast(IntegerType))
      .withColumn("long", col("long").cast(LongType))
      .withColumn("boolean", col("boolean").cast(BooleanType))
      .withColumn("double", col("double").cast(DoubleType))
      .withColumn("date", col("date").cast(DateType))
      .withColumn("timestamp", col("timestamp").cast(TimestampType))
      .withColumn("decimal", col("decimal").cast(DecimalType(38, 18)))
      .withColumn("array", col("array").cast("array<string>"))
      .withColumn("byte", col("byte").cast(ByteType))

    val actual = changeTypes(df)

    val expected = df
      .withColumn("decimal", lit(88888888888888888888.888888888888888888d))
      .withColumn("date", lit("2012-12-12"))
      .withColumn("timestamp", lit("2012-12-13 00:10:00"))

    assertDataFrameEquality(actual, expected)
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
      "MARCHE",
      "COMPTE",
      "CUMUL",
      "ANNUEL"
    )
    val expected = createDataFrame(
      Map(
        "key" -> "00000000222222222-MARCHE-COMPTE-CUMUL-ANNUEL",
        "numero_compte" -> "00000000222222222"
      )
    )
    assertDataFrameEquality(actual, expected)
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

  "toHbase" should "write to hbase" in {
    val df = createDataFrame(
      Map(
        "key" -> "1-KEY",
        "integer" -> 1,
        "long" -> 2,
        "boolean" -> true,
        "double" -> 5555.5555,
        "float" -> 6666.7777,
        "string" -> "string",
        "date" -> "2012-12-12",
        "timestamp" -> "2012-12-13 00:10:00",
        "decimal" -> "88888888888888888888.888888888888888888"
      )
    ).withColumn("integer", col("integer").cast(IntegerType))
      .withColumn("long", col("long").cast(LongType))
      .withColumn("boolean", col("boolean").cast(BooleanType))
      .withColumn("double", col("double").cast(DoubleType))
      .withColumn("date", col("date").cast(DateType))
      .withColumn("timestamp", col("timestamp").cast(TimestampType))
      .withColumn("decimal", col("decimal").cast(DecimalType(38, 18)))

    import me.ayoublabiad.io.hbase.HBaseWriter._

    df.toHbase(
      "data",
      Map(
        ("hbase.zookeeper.quorum", "localhost"),
        ("hbase.zookeeper.property.clientPort", "2181"),
        ("hbase.table", "labi")
      ))

    val actual = spark.read
      .format("org.apache.hadoop.hbase.spark")
      .option("hbase.columns.mapping", f"key STRING :key, ${hbaseColumnsMapping(changeTypes(df), "data")}")
      .option("hbase.table", "labi")
      .load()

    val expected = createDataFrame(
      Map(
        "key" -> "1-KEY",
        "integer" -> 1,
        "long" -> 2L,
        "boolean" -> true,
        "double" -> 5555.5555d,
        "float" -> 6666.7777,
        "string" -> "string",
        "date" -> "2012-12-12",
        "timestamp" -> "2012-12-13 00:10:00",
        "decimal" -> 8.888888888888889e19d
      )
    ).withColumn("integer", col("integer").cast(IntegerType))

    assertDataFrameEquality(actual, expected)

  }
}
