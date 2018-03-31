package datasources.utils;

import edb.client.DBClient;
import edb.common.Split;
import edb.common.UnknownTableException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

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
        return convert(_rows.get(_start), _columnNames);
    }

    private Row convert(edb.common.Row dbRow) {
        int fieldCount = dbRow.getFieldCount();
        Object[] values = new Object[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            edb.common.Row.Field f = dbRow.getField(i);
            if (f instanceof edb.common.Row.Int64Field) {
                edb.common.Row.Int64Field i64f = (edb.common.Row.Int64Field) f;
                values[i] = i64f.getValue();
            } else if (f instanceof edb.common.Row.DoubleField) {
                edb.common.Row.DoubleField df = (edb.common.Row.DoubleField) f;
                values[i] = df.getValue();
            }
        }
        GenericRow row = new GenericRow(values);
        return row;
    }

    private Row convert(edb.common.Row dbRow, String[] colNames) {
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
            }
        }
        GenericRow row = new GenericRow(values);
        return row;
    }

    private String _table;

    private DBClient _client;

    private int _start;
    private int _end;
    private List<edb.common.Row> _rows;
    private String[] _columnNames;
    private Split _split;
}
