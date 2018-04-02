package examples;

import edb.client.DBClient;
import edb.common.ExistingTableException;
import edb.common.Schema;
import edb.common.UnknownTableException;
import edb.server.DBServer;
import examples.utils.RDDUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;


public class ReadPartitionAware_Mismatch {
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
        schema.addColumn("g", Schema.ColumnType.STRING);
        schema.addColumn("u", Schema.ColumnType.INT64);


        DBClient client = new DBClient(serverHost, serverPort);
        //
        // THis time the table is not clustered on any column
        //
        client.createTable("myTable", schema);

        List<edb.common.Row> toInsert = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            edb.common.Row r = new edb.common.Row();
            //
            // String column with four distinct values for clustering
            //
            r.addField(new edb.common.Row.StringField("g", "G_" + (i % 4)));
            r.addField(new edb.common.Row.Int64Field("u", i * 100));

            toInsert.add(r);
        }

        client.bulkInsert("myTable", toInsert);

        System.out.println("*** Example database server populated with data");

        //
        // By default this data source supports creating Datasets with four partitions.
        //
        String dataSourceName = "datasources.PartitioningRowDataSource";

        SparkSession spark = SparkSession
                .builder()
                .appName("ReadPartitionAware-Mismatch")
                .master("local[4]")
                .getOrCreate();

        //
        // This is where we read from our DataSource. Notice how we use the
        // fully qualified class name and provide the information needed to connect to
        // ExampleDB using options. We specify two partitions so that each can be expected
        // to contain two clusters. But the table wasn't set up with the column clustered, so
        // a shuffle will be needed.
        //
        Dataset<Row> data = spark.read()
                .format(dataSourceName)
                .option("host", serverHost)
                .option("port", serverPort)
                .option("table", "myTable")
                .option("partitions", 2) // number of partitions specified here
                .load();

        System.out.println("*** Schema: ");
        data.printSchema();

        System.out.println("*** Data: ");
        data.show();

        RDDUtils.analyze(data);

        Dataset<Row> aggregated = data.groupBy(col("g")).agg(sum(col("u")));

        //
        // Note: since a shuffle was required, the resulting table has the usual default
        // number of partitions -- 200 as of Spark 2.3.0
        //
        System.out.println("*** Query result: ");
        aggregated.show();

        RDDUtils.analyze(aggregated);

        spark.stop();

        server.stop();
    }
}