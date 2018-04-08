package examples

import edb.server.DBServer
import examples.utils.RDDUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * This illustrates updates using the simplest update-capable data source example,
  * the ParallelRowReadWriteDataSource.
  *
  * First a dataframe is created that is used to populate a table for the first time. At that
  * point the newly created table's database schema is calculated from the dataframe schema.
  * Notice that even though we create a dataframe with 6 partitions, later when we read
  * from the table we always obtain dataframes with 4 partitions. This is because all tables
  * in ExampleDB advertise 4 partitions by default, and we would have to override that default
  * when reading to obtain different partitioning. However, the partitioning of the dataframe
  * DOES impact update parallelism -- notice from the log output that six tasks write to six temporary tables --
  * and these would have run in parallel had we not specified only 4 executors as we do in all these examples.
  *
  * We then put all four settings of SaveMode through their paces and see their impact.
  */
object SReadWriteParallel {
  def main(args: Array[String]) {

    val serverHost: String = "localhost"
    val serverPort: Int = 50199
    val server: DBServer = new DBServer(serverPort)
    server.start()
    System.out.println("*** Example database server started")

    val spark =
      SparkSession.builder()
        .appName("SReadWriteParallel")
        .master("local[4]")
        .getOrCreate()

    //
    // Set up the data source
    //
    val source = "datasources.ParallelRowReadWriteDataSource"

    val tableName = "myTable"

    val schema = StructType(
      Seq(
        StructField("id", LongType, true),
        StructField("count", LongType, true),
        StructField("group", StringType, true)
      )
    )

    //
    // insert some initial contents
    //

    val initialRowsToWrite = Seq(
      Row(100l, 500l, "A"),
      Row(200l, 50l, "B"),
      Row(300l, 160l, "A"),
      Row(400l, 100l, "B")
    )
    val initialRowsToWriteRDD = spark.sparkContext.parallelize(initialRowsToWrite, 6)
    val initialDfToWrite = spark.createDataFrame(initialRowsToWriteRDD, schema)

    initialDfToWrite.write
      .format(source)
      .option("host", serverHost)
      .option("port", serverPort)
      .option("table", tableName)
      .mode(SaveMode.Append)
      .save()

    println("*** Initial contents have been written to data source")

    val df = spark.read
      .format(source)
      .option("host", serverHost)
      .option("port", serverPort)
      .option("table", tableName)
      .load()

    println("*** Initial contents of data source")
    df.printSchema()
    df.show()
    RDDUtils.analyze(df)

    //
    // Set up another data frame to write to the above data source in
    // various values of SaveMode
    //
    val rowsToWrite = Seq(
      Row(1000l, 500l, "A"),
      Row(2000l, 150l, "C"),
      Row(3000l, 160l, "A"),
      Row(4000l, 1000l, "A")
    )
    val rowsToWriteRDD = spark.sparkContext.parallelize(rowsToWrite, 4)
    val dfToWrite = spark.createDataFrame(rowsToWriteRDD, df.schema)

    //
    // SaveMode.ErrorIfExists
    //
    try {
      dfToWrite.write
        .format(source)
        .option("host", serverHost)
        .option("port", serverPort)
        .option("table", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()
      println("*** Write should have failed, but didn't!")
    } catch  {
      case re: RuntimeException => {
        println(s"*** Threw RuntimeException as expected: ${re.getMessage}")
      }
      case e: Exception => {
        println(s"*** Threw unexpected exception: ${e.getMessage}")
      }
    }
    val df1 = spark.read
      .format(source)
      .option("host", serverHost)
      .option("port", serverPort)
      .option("table", tableName)
      .load()

    println("*** SaveMode.ErrorIfExists: exception and no change")
    df1.printSchema()
    df1.show()
    RDDUtils.analyze(df1)

    //
    // SaveMode.Append
    //
    dfToWrite.write
      .format(source)
      .option("host", serverHost)
      .option("port", serverPort)
      .option("table", tableName)
      .mode(SaveMode.Append)
      .save()
    val df2 = spark.read
      .format(source)
      .option("host", serverHost)
      .option("port", serverPort)
      .option("table", tableName)
      .load()
    println("*** SaveMode.Append: rows are added")
    df2.printSchema()
    df2.show()
    RDDUtils.analyze(df2)

    //
    // SaveMode.Overwrite
    //
    dfToWrite.write
      .format(source)
      .option("host", serverHost)
      .option("port", serverPort)
      .option("table", tableName)
      .mode(SaveMode.Overwrite)
      .save()
    val df3 = spark.read
      .format(source)
      .option("host", serverHost)
      .option("port", serverPort)
      .option("table", tableName)
      .load()
    println("*** SaveMode.Overwrite: old rows are replaced")
    df3.printSchema()
    df3.show()
    RDDUtils.analyze(df3)

    //
    // SaveMode.Ignore
    //
    dfToWrite.write
      .format(source)
      .option("host", serverHost)
      .option("port", serverPort)
      .option("table", tableName)
      .mode(SaveMode.Ignore)
      .save()
    val df4 = spark.read
      .format(source)
      .option("host", serverHost)
      .option("port", serverPort)
      .option("table", tableName)
      .load()
    println("*** SaveMode.Ignore: no change")
    df4.printSchema()
    df4.show()
    RDDUtils.analyze(df4)

    spark.stop()

    server.stop()
  }
}