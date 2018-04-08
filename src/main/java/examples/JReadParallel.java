package examples;

import edb.client.DBClient;
import edb.common.ExistingTableException;
import edb.common.Schema;
import edb.common.UnknownTableException;
import edb.server.DBServer;
import examples.utils.RDDUtils;
import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JReadParallel {
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
        // ExampleDB with some data.
        //

        Schema schema = new Schema();
        schema.addColumn("u", Schema.ColumnType.INT64);
        schema.addColumn("v", Schema.ColumnType.DOUBLE);

        DBClient client = new DBClient(serverHost, serverPort);
        client.createTable("myTable", schema);

        List<edb.common.Row> toInsert = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            edb.common.Row r = new edb.common.Row();
            r.addField(new edb.common.Row.Int64Field("u", i * 100));
            r.addField(new edb.common.Row.DoubleField("v", i + 0.2));
            toInsert.add(r);
        }

        client.bulkInsert("myTable", toInsert);

        System.out.println("*** Example database server populated with data");

        //
        // By default this data source supports creating Datasets with four partitions.
        //
        String dataSourceName = "datasources.ParallelRowDataSource";

        SparkSession spark = SparkSession
                .builder()
                .appName("JReadParallel")
                .master("local[4]")
                .getOrCreate();

        //
        // This is where we read from our DataSource. Notice how we use the
        // fully qualified class name and provide the information needed to connect to
        // ExampleDB using options. This time we'll ExampleDB's default number of table
        // partitions, 4, so we don't need to specify it.
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
        // Since this DataSource supports reading from one executor,
        // there will be a multiple partitions.
        //
        RDDUtils.analyze(data);


        //
        // We can specify a different number of partitions too, overriding ExampleDB's default.
        //
        data = spark.read()
                .format(dataSourceName)
                .option("host", serverHost)
                .option("port", serverPort)
                .option("table", "myTable")
                .option("partitions", 6) // number of partitions specified here
                .load();

        System.out.println("*** Schema: ");
        data.printSchema();

        System.out.println("*** Data: ");
        data.show();

        //
        // This time we see six partitions.
        //
        RDDUtils.analyze(data);

        spark.stop();

        server.stop();
    }
}