package datasources.utils;

import edb.client.DBClient;
import edb.common.*;

import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class DBClientWrapper {

    static Logger log = Logger.getLogger(DBClientWrapper.class.getName());

    public DBClientWrapper(String host, int port) {
        _host = host;
        _port = port;
    }


    public void bulkInsertFromTables(String destination,
                                     boolean truncateDestination,
                                     List<String> sources) {
        try {
            _client.bulkInsertFromTables(destination, truncateDestination, sources);
        } catch (UnknownTableException ute) {
            throw new RuntimeException(ute);
        }
    }

    public static org.apache.spark.sql.Row dbToSparkRow(Row dbRow) {
        int fieldCount = dbRow.getFieldCount();
        Object[] values = new Object[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            Row.Field f = dbRow.getField(i);
            if (f instanceof Row.Int64Field) {
                Row.Int64Field i64f = (Row.Int64Field) f;
                values[i] = i64f.getValue();
            } else if (f instanceof Row.DoubleField) {
                Row.DoubleField df = (Row.DoubleField) f;
                values[i] = df.getValue();
            } else if (f instanceof Row.StringField) {
                Row.StringField df = (Row.StringField) f;
                values[i] = df.getValue();
            }
        }
        GenericRow row = new GenericRow(values);
        return row;
    }

    public String getHost() { return _host; }

    public int getPort() { return _port; }

    public void connect() {
        log.info("Connecting to DB [" + _host + ":" + _port + "]");
        _client = new DBClient(_host, _port);
    }

    public void createTable(String name, Schema schema) {
        try {
            _client.createTable(name, schema);
        } catch (ExistingTableException ete) {
            throw new RuntimeException(ete);
        }
    }

    public boolean tableExists(String name) {
        try {
            _client.getTableSchema(name);
            return true;
        } catch (UnknownTableException ute) {
            return false;
        }
    }

    public boolean tableHasCompatibleSchema(String name, Schema required) {
        try {
            Schema actual =_client.getTableSchema(name);
            return actual.isCompatible(required);
        } catch (UnknownTableException ute) {
            throw new RuntimeException(ute);
        }
    }

    public String saveToTempTable(List<edb.common.Row> rows, Schema schema) {
        String tableName = _client.createTemporaryTable(schema);

        try {
            _client.bulkInsert(tableName, rows);
        } catch (UnknownTableException ute) {
            // can't happen since we just created it as a temp table
            throw new RuntimeException(ute);
        }
        return tableName;

    }

    public DBTableReader getTableReader(String tableName, String[] columnNames)
    throws UnknownTableException
    {
        return new DBTableReader(tableName, _client, columnNames);
    }

    public DBTableReader getTableReader(String tableName, String[] columnNames, Split split)
            throws UnknownTableException
    {
        return new DBTableReader(tableName, _client, columnNames, split);
    }

    public void disconnect() {

    }

    public static Schema sparkToDbSchema(StructType st) {
        Schema schema = new Schema();
        for (StructField sf: st.fields()) {
            if (sf.dataType() == DataTypes.StringType) {
                schema.addColumn(sf.name(), Schema.ColumnType.STRING);
            } else if (sf.dataType() == DataTypes.DoubleType) {
                schema.addColumn(sf.name(), Schema.ColumnType.DOUBLE);
            } else if (sf.dataType() == DataTypes.LongType) {
                schema.addColumn(sf.name(), Schema.ColumnType.INT64);
            } else {
                // TODO: type leakage
            }
        }
        return schema;
    }

    public static StructType dbToSparkSchema(Schema schema) {
        List<StructField> fields = new ArrayList<>();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            String name = schema.getColumnName(i);
            switch (schema.getColumnType(i)) {
                case INT64:
                    fields.add(DataTypes.createStructField(name,
                            DataTypes.LongType, true));
                    break;
                case DOUBLE:
                    fields.add(DataTypes.createStructField(name,
                            DataTypes.DoubleType, true));
                    break;
                case STRING:
                    fields.add(DataTypes.createStructField(name,
                            DataTypes.StringType, true));
                    break;
                default:
            }
        }
        return DataTypes.createStructType(fields);
    }

    public static org.apache.spark.sql.Row dbToSparkRow(edb.common.Row dbRow, String[] colNames) {
        int fieldCount = dbRow.getFieldCount();
        Object[] values = new Object[fieldCount];
        for (int i = 0; i < colNames.length; i++) {
            edb.common.Row.Field f = dbRow.getField(colNames[i]);
            if (f instanceof edb.common.Row.Int64Field) {
                edb.common.Row.Int64Field i64f = (edb.common.Row.Int64Field) f;
                values[i] = i64f.getValue();
            } else if (f instanceof edb.common.Row.DoubleField) {
                edb.common.Row.DoubleField df = (edb.common.Row.DoubleField) f;
                values[i] = df.getValue();
            } else if (f instanceof edb.common.Row.StringField) {
                edb.common.Row.StringField df = (edb.common.Row.StringField) f;
                values[i] = df.getValue();
            }
        }
        GenericRow row = new GenericRow(values);
        return row;
    }

    public static edb.common.Row sparkToDBRow(org.apache.spark.sql.Row row, StructType type) {
        edb.common.Row dbRow = new edb.common.Row();
        StructField[] fields = type.fields();
        for (int i = 0; i < type.size(); i++) {
            StructField sf = fields[i];
            if (sf.dataType() == DataTypes.StringType) {
                dbRow.addField(new edb.common.Row.StringField(sf.name(), row.getString(i)));
            } else if (sf.dataType() == DataTypes.DoubleType) {
                dbRow.addField(new edb.common.Row.DoubleField(sf.name(), row.getDouble(i)));
            } else if (sf.dataType() == DataTypes.LongType) {
                dbRow.addField(new edb.common.Row.Int64Field(sf.name(), row.getLong(i)));
            } else {
                // TODO: type leakage
            }
        }

        return dbRow;
    }

    public static edb.common.Row sparkToDBRow(org.apache.spark.sql.Row row, Schema schema) {
        edb.common.Row dbRow = new edb.common.Row();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            if (schema.getColumnType(i) ==  Schema.ColumnType.STRING) {
                dbRow.addField(new edb.common.Row.StringField(schema.getColumnName(i),
                        row.getString(i)));
            } else if (schema.getColumnType(i) ==  Schema.ColumnType.DOUBLE) {
                dbRow.addField(new edb.common.Row.DoubleField(schema.getColumnName(i),
                        row.getDouble(i)));
            } else if (schema.getColumnType(i) ==  Schema.ColumnType.INT64) {
                dbRow.addField(new edb.common.Row.Int64Field(schema.getColumnName(i),
                        row.getLong(i)));
            } else {
                // TODO: type leakage
            }
        }

        return dbRow;
    }

    public StructType getSparkSchema(String table) throws UnknownTableException
    {

        Schema schema =_client.getTableSchema(table);
        return dbToSparkSchema(schema);
    }

    public Schema getDBSchema(String table) throws UnknownTableException
    {
        Schema schema =_client.getTableSchema(table);
        return schema;
    }

    public String getClusteredIndexColumn(String table) throws UnknownTableException {
        return _client.getTableClusteredIndexColumn(table);
    }

    public List<Split> getSplits(String table, int count) throws UnknownTableException {
        return _client.getSplits(table, count);
    }

    public List<Split> getSplits(String table) throws UnknownTableException {
        return _client.getSplits(table);
    }

    private String _host;
    private int _port;
    private DBClient _client;
    private String _tableName;

}
