package examples;

import edb.client.DBClient;
import edb.common.ExistingTableException;
import edb.common.Schema;
import edb.common.UnknownTableException;
import edb.server.DBServer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JReadNamedTable {
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
        // ExampleDB with some data. We'll use the same schema as before, but
        // this time it's not baked into the data source -- the latter will infer it.
        //

        Schema schema = new Schema();
        schema.addColumn("u", Schema.ColumnType.INT64);
        schema.addColumn("v", Schema.ColumnType.DOUBLE);

        DBClient client = new DBClient(serverHost, serverPort);
        client.createTable("myTable", schema);

        List<edb.common.Row> toInsert = new ArrayList<>();
        edb.common.Row r1 = new edb.common.Row();
        r1.addField(new edb.common.Row.Int64Field("u", 100));
        r1.addField(new edb.common.Row.DoubleField("v", 200.2));
        toInsert.add(r1);
        edb.common.Row r2 = new edb.common.Row();
        r2.addField(new edb.common.Row.Int64Field("u", 300));
        r2.addField(new edb.common.Row.DoubleField("v", 400.4));
        toInsert.add(r2);

        client.bulkInsert("myTable", toInsert);

        System.out.println("*** Example database server populated with data");

        String dataSourceName = "datasources.FlexibleRowDataSource";

        SparkSession spark = SparkSession
                .builder()
                .appName("JReadNamedTable")
                .master("local[4]")
                .getOrCreate();

        //
        // This is where we read from our DataSource. Notice how we use the
        // fully qualified class name and provide the information needed to connect to
        // ExampleDB using options. Also, notice we specify the name of the table
        // as an option.
        //
        Dataset<Row> data = spark.read()
                .format(dataSourceName)
                .option("host", serverHost)
                .option("port", serverPort)
                .option("table", "myTable")
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