package examples;

import edb.client.DBClient;
import edb.common.ExistingTableException;
import edb.common.Schema;
import edb.common.UnknownTableException;
import edb.server.DBServer;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Basic {
    public static void main(String[] args)
            throws IOException, InterruptedException,
            ExistingTableException, UnknownTableException
    {

        final String serverHost = "localhost";
        final int serverPort = 50199;

        DBServer server = new DBServer(serverPort);
        server.start();

        System.out.println("*** Example database server started");

        //
        // Since this DataSource doesn't support writing, we need to populate
        // ExampleDB with some data. Also, since the DataSource only uses a
        // fixed schema and reads from a single, fixed table, we need to make
        // sure that the data we create conforms to those.
        //

        Schema schema = new Schema();
        schema.addColumn("i", Schema.ColumnType.INT64);
        schema.addColumn("j", Schema.ColumnType.INT64);

        DBClient client = new DBClient(serverHost, serverPort);
        client.createTable("theTable", schema);

        List<edb.common.Row> toInsert = new ArrayList<>();
        edb.common.Row r1 = new edb.common.Row();
        r1.addField(new edb.common.Row.Int64Field("i", 100));
        r1.addField(new edb.common.Row.Int64Field("j", 200));
        toInsert.add(r1);
        edb.common.Row r2 = new edb.common.Row();
        r2.addField(new edb.common.Row.Int64Field("i", 300));
        r2.addField(new edb.common.Row.Int64Field("j", 400));
        toInsert.add(r2);

        client.bulkInsert("theTable", toInsert);

        System.out.println("*** Example database server populated with data");

        String dataSourceName = "datasources.SimpleRowDataSource";

        SparkSession spark = SparkSession
                .builder()
                .appName("Basic")
                .master("local[4]")
                .getOrCreate();

        //
        // This is where we read from our DataSource. Notice how we use the
        // fully qualified class name and provide the information needed to connect to
        // ExampleDB using options.
        //
        Dataset<Row> data = spark.read()
                .format(dataSourceName)
                .option("host", serverHost)
                .option("port", serverPort)
                .load();

        System.out.println("*** Schema: ");
        data.printSchema();

        System.out.println("*** Data: ");
        data.show();

        //
        // Since this DataSource only supports reading from one executor,
        // there will only be a single partition.
        //
        System.out.println("*** Number of partitions: " +
                        data.rdd().partitions().length);

        spark.stop();

        server.stop();
    }
}