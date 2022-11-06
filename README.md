# Unit Testing Spark Job

A boilerplate for my attempt at creating a testable Spark job. You can find the details [here](https://ayoublabiad.me/posts/unit-testing-spark-jobs/).

The data is from [Spark: The Definitive Guide’s Code Repository](https://github.com/databricks/Spark-The-Definitive-Guide/tree/master/data/flight-data/csv).

## A test example

A test should look something like this:

```scala

    val input: DataFrame = createDataFrame(
      obj(
        "destination" -> "morocco",
        "origin" -> "spain",
        "count" -> 3
      ),
      obj(
        "destination" -> "morocco",
        "origin" -> "egypt",
        "count" -> 5
      ),
      obj(
        "destination" -> "france",
        "origin" -> "germany",
        "count" -> 10
      )
    )

    val actual: DataFrame = getDestinationsWithTotalCount(input)
    val expected: DataFrame = createDataFrame(
        obj(
          "destination" -> "france",
          "total_count" -> 10
        ),
        obj(
          "destination" -> "morocco",
          "total_count" -> 8
        )
    )

    assertSmallDataFrameEquality(actual, expected, ignoreNullable = true, orderedComparison = false)
```

Check the code for the full implementation.

## Project structure
<pre>
scala
+-- me.ayoublabiad
|   +-- common
|   |   +-- SparkSessionWrapper.scala
|   +-- io
|   |   +-- Read.scala
|   |   +-- Write.scala
|   +-- job
|   |   +-- flights
|   |   |   +-- FlightsTransformation.scala
|   |   |   +-- FlightsJob.scala
|   |   +-- Job.scala
|   +-- Main.scala
</pre>
- **common**: Package for common classes between jobs. A `Utils` class with frequently used functions could be added here.
- **io**: Package for input and output functions.
- **job**: Is where the magic happens. This package contains domain specific subpackages. Inside each subpackage, we find the transformations and the job of each domain specific use case. This is a personal preference. But I found that grouping transformations in one package and the jobs in another package is burdensome when dealing the code. Especially when the number of use cases grows.
    - The transformation functions should have one of the following signatures:
```scala
import org.apache.spark.sql.{Column, DataFrame}

def function1(dataFrame: DataFrame): DataFrame
// or 
def function2(column: Column): Column
// or even
def function3(columnName: String): Column
```
They could be used:

```scala
val transformedDf: DataFrame = function1(rawDataFrame)

// or 

rawDataFrame
  .withColumn("newColumnName", function2(col("oldColumnName")))

// or even 

rawDataFrame
  .withColumn("newColumnName", function3("oldColumnName"))
```
- The job is where we groupe the three phases: The read, transform, and write. In its simplest form, it should look something like this:

```scala
import com.typesafe.config.Config
import me.ayoublabiad.io.csv.CsvReader.readTableFromHive
import me.ayoublabiad.io.csv.CsvWriter
import me.ayoublabiad.job.Job
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object FlightsJob extends Job {
  override def process(spark: SparkSession, config: Config): Unit = {
    val db: String = config.getString("settings.database")

    val flights: DataFrame = readTableFromHive(db, "flights")

    val destinationsWithTotalCount = FlightsTransformation.getDestinationsWithTotalCount(flights)

    CsvWriter.writeTohive(spark, filteredDestinations, db, "flights_total_count")
  }
}

```

- **Job.scala**: In a trait to bind the jobs implementations.
- **Main.scala**: The entry point of your job.

