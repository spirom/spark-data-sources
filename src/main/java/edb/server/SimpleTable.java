package edb.server;

import edb.common.Row;
import edb.common.Schema;
import edb.common.SimpleSplit;
import edb.common.Split;

import java.util.ArrayList;
import java.util.List;

class SimpleTable implements ITable {
    public SimpleTable(String name, Schema schema, boolean isTemporary) {
        _name = name;
        _schema = schema;
        _isTemporary = isTemporary;
    }

    public String getName() { return _name; }

    public Schema getSchema() { return _schema; }

    public void addRows(List<Row> rows) { _rows.addAll(rows); }

    public List<Row> getRows() { return new ArrayList<>(_rows); }

    public List<Row> getRows(Split split) {
        SimpleSplit simpleSplit = (SimpleSplit) split;
        List<Row> rows = new ArrayList<>();
        if (split.isEmpty()) {
            return rows;
        }
        for (int i = 0; i < _rows.size(); i++) {
            if (i >= simpleSplit.firstRow() && i < simpleSplit.lastRow()) {
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
                splits.add(new SimpleSplit(i, i + 1));
            }
            for (long i = rowCount; i < countDesired; i++) {
                splits.add(new SimpleSplit(i, i));
            }
        } else {
            // make the last one short if needed, and the rest even
            long regularSize = (long)Math.ceil((double)rowCount / (double)countDesired);
            long splitStart = 0;
            for (int i = 0; i < countDesired; i++) {
                if (splitStart >= rowCount) {
                    splits.add(new SimpleSplit(rowCount, rowCount));
                } else if (splitStart + regularSize > rowCount) {
                    splits.add(new SimpleSplit(splitStart, rowCount));
                } else {
                    splits.add(new SimpleSplit(splitStart, splitStart + regularSize));
                }
                splitStart += regularSize;
            }
        }
        return splits;
    }

    public boolean isTemporary() { return _isTemporary; }

    public void truncate() {
        _rows.clear();
    }

    private String _name;

    private Schema _schema;

    private List<Row> _rows = new ArrayList<>();

    private boolean _isTemporary;

}
