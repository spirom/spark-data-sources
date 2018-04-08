package datasources.utils;

import edb.client.DBClient;
import edb.common.Schema;
import edb.common.Split;
import edb.common.UnknownTableException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class DBTableReader {
    public DBTableReader(String table, DBClient client, String[] columnNames)
            throws UnknownTableException {
        _table = table;
        _client = client;
        _rows = _client.getAllRows(table);
        _start = -1;
        _end = _rows.size();
        _columnNames = columnNames;
        _split = null;
    }

    public DBTableReader(String table, DBClient client, String[] columnNames, Split split)
            throws UnknownTableException {
        _table = table;
        _client = client;
        _rows = _client.getAllRows(table, split);
        _start = -1;
        _end = _rows.size();
        _columnNames = columnNames;
        _split = split;
    }

    public boolean next() {
        _start += 1;
        return _start < _end;
    }

    public Row get() {
        return DBClientWrapper.dbToSparkRow(_rows.get(_start), _columnNames);
    }



    private String _table;

    private DBClient _client;

    private int _start;
    private int _end;
    private List<edb.common.Row> _rows;
    private String[] _columnNames;
    private Split _split;
}
