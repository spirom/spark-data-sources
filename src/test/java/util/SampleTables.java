package util;

import edb.common.*;
import edb.rpc.EDBProto;

import java.util.ArrayList;
import java.util.List;

public class SampleTables {

    public static void makeSimple(IExampleDB db, String tableName, int rowCount)
            throws ExistingTableException, UnknownTableException {
        Schema schema = new Schema();
        schema.addColumn("a", Schema.ColumnType.INT64);
        schema.addColumn("b", Schema.ColumnType.DOUBLE);

        db.createTable(tableName, schema);

        List<Row> toInsert = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            Row r = new Row();
            r.addField(new Row.Int64Field("a", i));
            r.addField(new Row.DoubleField("b", i + 0.5));
            toInsert.add(r);
        }

        db.bulkInsert(tableName, toInsert);
    }

    public static void makeClustered(IExampleDB db, String tableName,
                                     int rowCount, int clusterCount)
            throws ExistingTableException, UnknownTableException {
        Schema schema = new Schema();
        schema.addColumn("a", Schema.ColumnType.INT64);
        schema.addColumn("b", Schema.ColumnType.DOUBLE);
        schema.addColumn("c", Schema.ColumnType.STRING);

        db.createTable(tableName, schema, "c");

        List<Row> toInsert = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            Row r = new Row();
            r.addField(new Row.Int64Field("a", i));
            r.addField(new Row.DoubleField("b", i + 0.5));
            r.addField(new Row.StringField("c", "S_" + (i % clusterCount)));
            toInsert.add(r);
        }

        db.bulkInsert(tableName, toInsert);
    }
}
