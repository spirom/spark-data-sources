package examples.utils;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class RDDUtils {

    public static void analyze (Dataset<Row> ds) {
        RDD<Row> rdd = ds.rdd();

        Row[][] partitions = (Row[][]) rdd.glom().collect();

        System.out.println("*** Number of partitions: " + partitions.length);

        for (int i = 0; i < partitions.length; i++) {
            System.out.println("*** Partition [" + i + "] has [" +
                    partitions[i].length + "] elements");
        }
    }
}
