package datasources.utils;

import edb.client.DBClient;
import edb.common.*;

import org.apache.log4j.Logger;
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

    public String getHost() { return _host; }

    public int getPort() { return _port; }

    public void connect() {
        log.info("Connecting to DB [" + _host + ":" + _port + "]");
        _client = new DBClient(_host, _port);
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

    public StructType getSchema(String table) throws UnknownTableException
    {
        List<StructField> fields = new ArrayList<>();
        Schema schema =_client.getTableSchema(table);
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
