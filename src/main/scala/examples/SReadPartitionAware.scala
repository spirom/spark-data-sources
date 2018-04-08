package examples

import java.io.IOException
import java.util

import edb.client.DBClient
import edb.common.{ExistingTableException, Row, Schema, UnknownTableException}
import edb.server.DBServer

import examples.utils.RDDUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum}

import scala.collection.JavaConverters._


object SReadPartitionAware {

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
    schema.addColumn("g", Schema.ColumnType.STRING)
    schema.addColumn("u", Schema.ColumnType.INT64)

    val client = new DBClient(serverHost, serverPort)
    // Specify that the table is partitioned on column G
    client.createTable("myTable", schema, "g")

    val toInsert = for {
      i <- (0 to 19).toList
    } yield {
      val r = new Row
      r.addField(new Row.StringField("g", "G_" + (i % 4)))
      r.addField(new Row.Int64Field("u", i * 100))
      r
    }
    client.bulkInsert("myTable", toInsert.asJava)
    System.out.println("*** Example database server populated with data")

    // By default this data source supports creating Datasets with four partitions.
    val dataSourceName = "datasources.PartitioningRowDataSource"
    val spark = SparkSession.builder
      .appName("SReadPartitionAware")
      .master("local[4]")
      .getOrCreate

    //
    // This is where we read from our DataSource. Notice how we use the
    // fully qualified class name and provide the information needed to connect to
    // ExampleDB using options. We specify two partitions so that each can be expected
    // to contain two clusters.
    //
    val data = spark.read
      .format(dataSourceName)
      .option("host", serverHost)
      .option("port", serverPort)
      .option("table", "myTable")
      .option("partitions", 2)
      .load // number of partitions specified here

    System.out.println("*** Schema: ")
    data.printSchema()
    System.out.println("*** Data: ")
    data.show()
    RDDUtils.analyze(data)

    //
    // The following aggregation query needs each group to end up on the same partition so it can
    // aggregate in parallel, and the optimizer will insert a potentially expensive shuffle
    // in order to achieve this unless the data source tells it that necessary partitioning has
    // already been achieved
    //
    val aggregated = data.groupBy(col("g")).agg(sum(col("u")))

    System.out.println("*** Query result: ")
    aggregated.show()
    RDDUtils.analyze(aggregated)
    spark.stop()
    server.stop()
  }
}