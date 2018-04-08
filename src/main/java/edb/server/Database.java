package edb.server;


import edb.common.*;

import java.util.*;

public class Database implements IExampleDB {

    public List<String> listTables() {
        return new ArrayList<>(_tables.keySet());
    }

    public synchronized void createTable(String name, Schema schema) throws ExistingTableException {

        boolean present = _tables.containsKey(name);

        if (present) {
            throw new ExistingTableException(name);
        } else {
            _tables.put(name, new SimpleTable(name, schema, false));
        }
    }

    public synchronized void createTable(String name, Schema schema, String clusterColumn)
            throws ExistingTableException {

        boolean present = _tables.containsKey(name);

        if (present) {
            throw new ExistingTableException(name);
        } else {
            _tables.put(name, new ClusteredIndexTable(name, schema, clusterColumn));
        }
    }

    public synchronized String createTemporaryTable(Schema schema) {
        String name = "__TMP_" + _tempCounter++;
        _tables.put(name, new SimpleTable(name, schema, true));
        return name;
    }

    public synchronized Schema getTableSchema(String name) throws UnknownTableException {

        boolean present = _tables.containsKey(name);
        if (present) {
            ITable entry = _tables.get(name);
            return entry.getSchema();
        } else {
            throw new UnknownTableException(name);
        }
    }

    public synchronized String getTableClusteredIndexColumn(String name) throws UnknownTableException {
        boolean present = _tables.containsKey(name);
        if (present) {
            ITable entry = _tables.get(name);
            if (entry instanceof ClusteredIndexTable) {
                return ((ClusteredIndexTable) entry).getIndexColumn();
            } else {
                return null;
            }
        } else {
            throw new UnknownTableException(name);
        }
    }

    public synchronized void bulkInsert(String name, List<Row> rows) throws UnknownTableException {
        boolean present = _tables.containsKey(name);
        if (present) {
            ITable entry = _tables.get(name);
            entry.addRows(rows);
        } else {
            throw new UnknownTableException(name);
        }
    }

    public synchronized void bulkInsertFromTables(String destination,
                                                  boolean truncateDestination,
                                                  List<String> sourceTables) throws UnknownTableException
    {
        // TODO: should also throw something like IncompatibleSchemaException here
        // TODO: and a few other places
        boolean present = _tables.containsKey(destination);
        List<Row> rowsToInsert = new ArrayList<>();
        if (!present) {
            throw new UnknownTableException(destination);
        }
        ITable destinationTable = _tables.get(destination);
        for (String sourceName: sourceTables) {
            if (!_tables.containsKey(sourceName)) {
                throw new UnknownTableException(sourceName);
            }
            rowsToInsert.addAll(_tables.get(sourceName).getRows());
        }
        // about to commit
        if (truncateDestination) destinationTable.truncate();
        destinationTable.addRows(rowsToInsert);
        // committed -- now delete any temporary tables if possible
        // NOTE: they may not be temporary
        for (String sourceName: sourceTables) {
            if (_tables.containsKey(sourceName) && _tables.get(sourceName).isTemporary()) {
                _tables.remove(sourceName);
            }
        }
    }

    public synchronized List<Row> getAllRows(String name) throws UnknownTableException {
        boolean present = _tables.containsKey(name);
        if (present) {
            ITable entry = _tables.get(name);
            return entry.getRows();
        } else {
            throw new UnknownTableException(name);
        }
    }

    public synchronized List<Row> getAllRows(String name, List<String> columns) throws UnknownTableException {
        boolean present = _tables.containsKey(name);
        if (present) {
            Rowset rowset = new Rowset(_tables.get(name).getRows());
            rowset.applyProjection(columns);
            return rowset.getRows();
        } else {
            throw new UnknownTableException(name);
        }
    }

    public synchronized List<Row> getAllRows(String name, Split split) throws UnknownTableException {
        boolean present = _tables.containsKey(name);
        if (present) {
            ITable entry = _tables.get(name);
            return entry.getRows(split);
        } else {
            throw new UnknownTableException(name);
        }
    }

    public synchronized List<Row> getAllRows(String name, Split split, List<String> columns) throws UnknownTableException {
        boolean present = _tables.containsKey(name);
        if (present) {
            Rowset rowset = new Rowset(_tables.get(name).getRows(split));
            rowset.applyProjection(columns);
            return rowset.getRows();
        } else {
            throw new UnknownTableException(name);
        }
    }

    public synchronized List<Split> getSplits(String table) throws UnknownTableException {
        boolean present = _tables.containsKey(table);
        if (present) {
            ITable entry = _tables.get(table);
            return entry.makeSplits();
        } else {
            throw new UnknownTableException(table);
        }
    }

    public synchronized List<Split> getSplits(String table, int count) throws UnknownTableException {
        boolean present = _tables.containsKey(table);
        if (present) {
            ITable entry = _tables.get(table);
            return entry.makeSplits(count);
        } else {
            throw new UnknownTableException(table);
        }
    }

    Hashtable<String, ITable> _tables = new Hashtable<>();

    private long _tempCounter = 0;

}
