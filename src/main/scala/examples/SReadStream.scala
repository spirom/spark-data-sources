package examples

import edb.client.DBClient
import edb.common.{Row, Schema}
import edb.server.DBServer
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

object SReadStream {
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
    val dataSourceName = "datasources.ReadStreamDataSource"

    val spark = SparkSession.builder
      .appName("SReadStream")
      .master("local[4]")
      .getOrCreate

    val streamDF = spark
      .readStream
      .option("host", serverHost)
      .option("port", serverPort)
      .option("table", "myTable")
      .format(dataSourceName)
      .load()

    // it has the schema we specified
    streamDF.printSchema()

    // every time a batch of records is received, dump the new records
    // to the console -- often this will just be the contents of a single file,
    // but sometimes it will contain mote than one file
    val query = streamDF.writeStream
      .outputMode("append")
      .format("console")
      .start()

    println("*** done setting up streaming")

    Thread.sleep(5000)

    client.bulkInsert("myTable", toInsert.asJava)

    Thread.sleep(5000)

    println("*** Stopping stream")
    query.stop()

    query.awaitTermination()
    println("*** Streaming terminated")



    spark.stop()

    server.stop()

  }


}
