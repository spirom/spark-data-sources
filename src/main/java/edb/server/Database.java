package edb.server;


import edb.common.*;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class Database implements IExampleDB {



    public List<String> listTables() {
        return new ArrayList<>(_tables.keySet());
    }

    public void createTable(String name, Schema schema) throws ExistingTableException {

        boolean present = _tables.containsKey(name);

        if (present) {
            throw new ExistingTableException(name);
        } else {
            _tables.put(name, new SimpleTable(name, schema));
        }
    }

    public Schema getTableSchema(String name) throws UnknownTableException {

        boolean present = _tables.containsKey(name);
        if (present) {
            SimpleTable entry = _tables.get(name);
            return entry.getSchema();
        } else {
            throw new UnknownTableException(name);
        }
    }

    public void bulkInsert(String name, List<Row> rows) throws UnknownTableException {
        boolean present = _tables.containsKey(name);
        if (present) {
            SimpleTable entry = _tables.get(name);
            entry.addRows(rows);
        } else {
            throw new UnknownTableException(name);
        }
    }

    public List<Row> getAllRows(String name) throws UnknownTableException {
        boolean present = _tables.containsKey(name);
        if (present) {
            SimpleTable entry = _tables.get(name);
            return entry.getRows();
        } else {
            throw new UnknownTableException(name);
        }
    }

    public List<Row> getAllRows(String name, Split split) throws UnknownTableException {
        boolean present = _tables.containsKey(name);
        if (present) {
            SimpleTable entry = _tables.get(name);
            return entry.getRows(split);
        } else {
            throw new UnknownTableException(name);
        }
    }

    public List<Split> getSplits(String table) throws UnknownTableException {
        boolean present = _tables.containsKey(table);
        if (present) {
            SimpleTable entry = _tables.get(table);
            return entry.makeSplits();
        } else {
            throw new UnknownTableException(table);
        }
    }

    public List<Split> getSplits(String table, int count) throws UnknownTableException {
        boolean present = _tables.containsKey(table);
        if (present) {
            SimpleTable entry = _tables.get(table);
            return entry.makeSplits(count);
        } else {
            throw new UnknownTableException(table);
        }
    }

    Hashtable<String, SimpleTable> _tables = new Hashtable<>();

}
