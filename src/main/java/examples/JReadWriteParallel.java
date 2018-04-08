package examples;

import edb.common.ExistingTableException;
import edb.common.UnknownTableException;
import edb.server.DBServer;
import examples.utils.RDDUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


import scala.collection.Seq;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

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
public class JReadWriteParallel {
    public static void main(String[] args)
            throws IOException, InterruptedException,
            ExistingTableException, UnknownTableException {

        String serverHost = "localhost";
        int serverPort = 50199;
        DBServer server = new DBServer(serverPort);
        server.start();
        System.out.println("*** Example database server started");

        SparkSession spark =
                SparkSession.builder()
                        .appName("ReadWriteParallel")
                        .master("local[4]")
                        .getOrCreate();

        //
        // Set up the data source
        //
        String source = "datasources.ParallelRowReadWriteDataSource";

        String tableName = "myTable";

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", LongType, true));
        fields.add(DataTypes.createStructField("count", LongType, true));
        fields.add(DataTypes.createStructField("group", StringType, true));
        StructType schema = DataTypes.createStructType(fields);


        //
        // insert some initial contents
        //

        List<Row> initialRowsToWrite = Arrays.asList(
                RowFactory.create(100l, 500l, "A"),
                RowFactory.create(200l, 50l, "B"),
                RowFactory.create(300l, 160l, "A"),
                RowFactory.create(400l, 100l, "B")
        );

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Row> initialRowsToWriteRDD = sc.parallelize(initialRowsToWrite, 6);
        Dataset<Row> initialDfToWrite = spark.createDataFrame(initialRowsToWriteRDD, schema);

        initialDfToWrite.write()
                .format(source)
                .option("host", serverHost)
                .option("port", serverPort)
                .option("table", tableName)
                .mode(SaveMode.Append)
                .save();

        System.out.println("*** Initial contents have been written to data source");

        Dataset<Row> df = spark.read()
                .format(source)
                .option("host", serverHost)
                .option("port", serverPort)
                .option("table", tableName)
                .load();

        System.out.println("*** Initial contents of data source");
        df.printSchema();
        df.show();
        RDDUtils.analyze(df);

        //
        // Set up another data frame to write to the above data source in
        // various values of SaveMode
        //
        List<Row> rowsToWrite = Arrays.asList(
                RowFactory.create(1000l, 500l, "A"),
                RowFactory.create(2000l, 150l, "C"),
                RowFactory.create(3000l, 160l, "A"),
                RowFactory.create(4000l, 1000l, "A")
        );
        JavaRDD<Row> rowsToWriteRDD = sc.parallelize(rowsToWrite, 4);
        Dataset<Row> dfToWrite = spark.createDataFrame(rowsToWriteRDD, df.schema());

        //
        // SaveMode.ErrorIfExists
        //
        try {
            dfToWrite.write()
                    .format(source)
                    .option("host", serverHost)
                    .option("port", serverPort)
                    .option("table", tableName)
                    .mode(SaveMode.ErrorIfExists)
                    .save();
            System.out.println("*** Write should have failed, but didn't!");
        } catch  (RuntimeException re) {
            System.out.println("*** Threw RuntimeException as expected: " + re.getMessage());
        } catch (Exception e) {
            System.out.println("*** Threw unexpected exception: " + e.getMessage());
        }

        Dataset<Row> df1 = spark.read()
                .format(source)
                .option("host", serverHost)
                .option("port", serverPort)
                .option("table", tableName)
                .load();

        System.out.println("*** SaveMode.ErrorIfExists: exception and no change");
        df1.printSchema();
        df1.show();
        RDDUtils.analyze(df1);

        //
        // SaveMode.Append
        //
        dfToWrite.write()
                .format(source)
                .option("host", serverHost)
                .option("port", serverPort)
                .option("table", tableName)
                .mode(SaveMode.Append)
                .save();
        Dataset<Row> df2 = spark.read()
                .format(source)
                .option("host", serverHost)
                .option("port", serverPort)
                .option("table", tableName)
                .load();
        System.out.println("*** SaveMode.Append: rows are added");
        df2.printSchema();
        df2.show();
        RDDUtils.analyze(df2);

        //
        // SaveMode.Overwrite
        //
        dfToWrite.write()
                .format(source)
                .option("host", serverHost)
                .option("port", serverPort)
                .option("table", tableName)
                .mode(SaveMode.Overwrite)
                .save();
        Dataset<Row> df3 = spark.read()
                .format(source)
                .option("host", serverHost)
                .option("port", serverPort)
                .option("table", tableName)
                .load();
        System.out.println("*** SaveMode.Overwrite: old rows are replaced");
        df3.printSchema();
        df3.show();
        RDDUtils.analyze(df3);

        //
        // SaveMode.Ignore
        //
        dfToWrite.write()
                .format(source)
                .option("host", serverHost)
                .option("port", serverPort)
                .option("table", tableName)
                .mode(SaveMode.Ignore)
                .save();
        Dataset df4 = spark.read()
                .format(source)
                .option("host", serverHost)
                .option("port", serverPort)
                .option("table", tableName)
                .load();
        System.out.println("*** SaveMode.Ignore: no change");
        df4.printSchema();
        df4.show();
        RDDUtils.analyze(df4);

        spark.stop();

        server.stop();

    }
}