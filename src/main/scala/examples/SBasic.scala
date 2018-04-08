package examples

import java.io.IOException

import edb.client.DBClient
import edb.common.{ExistingTableException, Row, Schema, UnknownTableException}
import edb.server.DBServer
import org.apache.spark.sql.SparkSession

object SBasic {

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  @throws(classOf[ExistingTableException])
  @throws(classOf[UnknownTableException])
  def main(args: Array[String]) {
    val serverHost: String = "localhost"
    val serverPort: Int = 50199
    val server: DBServer = new DBServer(serverPort)

    server.start
    System.out.println("*** Example database server started")

    val schema: Schema = new Schema
    schema.addColumn("i", Schema.ColumnType.INT64)
    schema.addColumn("j", Schema.ColumnType.INT64)

    val client: DBClient = new DBClient(serverHost, serverPort)

    client.createTable("theTable", schema)
    val toInsert = new java.util.ArrayList[Row]
    val r1: Row = new Row
    r1.addField(new Row.Int64Field("i", 100))
    r1.addField(new Row.Int64Field("j", 200))
    toInsert.add(r1)
    val r2: Row = new Row
    r2.addField(new Row.Int64Field("i", 300))
    r2.addField(new Row.Int64Field("j", 400))
    toInsert.add(r2)
    client.bulkInsert("theTable", toInsert)

    System.out.println("*** Example database server populated with data")

    val dataSourceName: String = "datasources.SimpleRowDataSource"

    val spark: SparkSession =
      SparkSession.builder().appName("JBasic").master("local[4]").getOrCreate()
    val data =
      spark.read
        .format(dataSourceName)
        .option("host", serverHost)
        .option("port", serverPort)
        .load()

    System.out.println("*** Schema: ")
    data.printSchema()

    System.out.println("*** Data: ")
    data.show()

    System.out.println("*** Number of partitions: " + data.rdd.partitions.length)

    spark.stop()
    server.stop()
  }
}
