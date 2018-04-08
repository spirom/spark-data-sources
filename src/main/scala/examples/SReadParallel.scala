package examples

import edb.client.DBClient
import edb.common.{ExistingTableException, Row, Schema, UnknownTableException}
import edb.server.DBServer

import examples.utils.RDDUtils

import org.apache.spark.sql.SparkSession

import java.io.IOException

import scala.collection.JavaConverters._

object SReadParallel {

  @throws[IOException]
  @throws[InterruptedException]
  @throws[ExistingTableException]
  @throws[UnknownTableException]
  def main(args: Array[String]): Unit = {
    val serverHost = "localhost"
    val serverPort = 50199
    val server = new DBServer(serverPort)
    server.start()
    System.out.println("*** Example database server started")

    //
    // Since this DataSource doesn't support writing, we need to populate
    // ExampleDB with some data.
    //
    val schema = new Schema
    schema.addColumn("u", Schema.ColumnType.INT64)
    schema.addColumn("v", Schema.ColumnType.DOUBLE)

    val client = new DBClient(serverHost, serverPort)

    client.createTable("myTable", schema)

    val toInsert = for {
      i <- (0 to 19).toList
    } yield {
      val r = new Row
      r.addField(new Row.Int64Field("u", i * 100))
      r.addField(new Row.DoubleField("v", i + 0.2))
      r
    }
    client.bulkInsert("myTable", toInsert.asJava)
    System.out.println("*** Example database server populated with data")

    // By default this data source supports creating Datasets with four partitions.
    val dataSourceName = "datasources.ParallelRowDataSource"

    val spark = SparkSession.builder
      .appName("SReadParallel")
      .master("local[4]")
      .getOrCreate

    //
    // This is where we read from our DataSource. Notice how we use the
    // fully qualified class name and provide the information needed to connect to
    // ExampleDB using options. This time we'll use ExampleDB's default number of table
    // partitions, 4, so we don't need to specify it.
    //
    var data = spark.read
      .format(dataSourceName)
      .option("host", serverHost)
      .option("port", serverPort)
      .option("table", "myTable").load

    System.out.println("*** Schema: ")
    data.printSchema()
    System.out.println("*** Data: ")
    data.show()
    //
    // Since this DataSource supports reading from one executor,
    // there will be a multiple partitions.
    //
    RDDUtils.analyze(data)

    //
    // We can specify a different number of partitions too, overriding ExampleDB's default.
    //
    data = spark.read.format(dataSourceName)
      .option("host", serverHost)
      .option("port", serverPort)
      .option("table", "myTable")
      .option("partitions", 6).load // number of partitions specified here

    System.out.println("*** Schema: ")
    data.printSchema()
    System.out.println("*** Data: ")
    data.show()
    //
    // This time we see six partitions.
    //
    RDDUtils.analyze(data)

    spark.stop()

    server.stop()
  }
}