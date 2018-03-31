package edb.server;


import edb.common.*;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class Database implements IExampleDB {

    private static class TableEntry {
        public TableEntry(String name, Schema schema) {
            _name = name;
            _schema = schema;
        }

        public String getName() { return _name; }

        public Schema getSchema() { return _schema; }

        public void addRows(List<Row> rows) { _rows.addAll(rows); }

        public List<Row> getRows() { return new ArrayList<>(_rows); }

        public List<Row> getRows(Split split) {
            List<Row> rows = new ArrayList<>();
            if (split.isEmpty()) {
                return rows;
            }
            for (int i = 0; i < _rows.size(); i++) {
                if (i >= split.firstRow() && i < split.lastRow()) {
                    rows.add(_rows.get(i));
                }
            }
            return rows;
        }

        public long getRowCount() { return _rows.size(); }

        public List<Split> makeSplits() {
            return makeSplits(4);
        }

        public List<Split> makeSplits(int countDesired) {
            long rowCount = getRowCount();
            List<Split> splits = new ArrayList<>();
            if (rowCount < countDesired) {
                // one row per split, the last few empty
                for (long i = 0; i < rowCount; i++) {
                    splits.add(new Split(i, i + 1));
                }
                for (long i = rowCount; i < countDesired; i++) {
                    splits.add(new Split(i, i));
                }
            } else {
                // make the last one short if needed, and the rest even
                long regularSize = (long)Math.ceil((double)rowCount / (double)countDesired);
                long splitStart = 0;
                for (int i = 0; i < countDesired; i++) {
                    if (splitStart >= rowCount) {
                        splits.add(new Split(rowCount, rowCount));
                    } else if (splitStart + regularSize > rowCount) {
                        splits.add(new Split(splitStart, rowCount));
                    } else {
                        splits.add(new Split(splitStart, splitStart + regularSize));
                    }
                    splitStart += regularSize;
                }
            }
            return splits;
        }

        private String _name;

        private Schema _schema;

        private List<Row> _rows = new ArrayList<>();

    }

    public List<String> listTables() {
        return new ArrayList<>(_tables.keySet());
    }

    public void createTable(String name, Schema schema) throws ExistingTableException {

        boolean present = _tables.containsKey(name);

        if (present) {
            throw new ExistingTableException(name);
        } else {
            _tables.put(name, new TableEntry(name, schema));
        }
    }

    public Schema getTableSchema(String name) throws UnknownTableException {

        boolean present = _tables.containsKey(name);
        if (present) {
            TableEntry entry = _tables.get(name);
            return entry.getSchema();
        } else {
            throw new UnknownTableException(name);
        }
    }

    public void bulkInsert(String name, List<Row> rows) throws UnknownTableException {
        boolean present = _tables.containsKey(name);
        if (present) {
            TableEntry entry = _tables.get(name);
            entry.addRows(rows);
        } else {
            throw new UnknownTableException(name);
        }
    }

    public List<Row> getAllRows(String name) throws UnknownTableException {
        boolean present = _tables.containsKey(name);
        if (present) {
            TableEntry entry = _tables.get(name);
            return entry.getRows();
        } else {
            throw new UnknownTableException(name);
        }
    }

    public List<Row> getAllRows(String name, Split split) throws UnknownTableException {
        boolean present = _tables.containsKey(name);
        if (present) {
            TableEntry entry = _tables.get(name);
            return entry.getRows(split);
        } else {
            throw new UnknownTableException(name);
        }
    }

    public List<Split> getSplits(String table) throws UnknownTableException {
        boolean present = _tables.containsKey(table);
        if (present) {
            TableEntry entry = _tables.get(table);
            return entry.makeSplits();
        } else {
            throw new UnknownTableException(table);
        }
    }

    public List<Split> getSplits(String table, int count) throws UnknownTableException {
        boolean present = _tables.containsKey(table);
        if (present) {
            TableEntry entry = _tables.get(table);
            return entry.makeSplits(count);
        } else {
            throw new UnknownTableException(table);
        }
    }

    Hashtable<String, TableEntry> _tables = new Hashtable<>();

}
