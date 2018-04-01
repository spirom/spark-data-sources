package edb.server;

import edb.common.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;

/**
 * A table clustered by a single column, which must be a string column.
 * Splits work on clusters instead of rows, which may create skew. (That is, splits tend to
 * keep the number of clusters even, not the number of rows.)
 */
class ClusteredIndexTable {
    public ClusteredIndexTable(String name, Schema schema, String indexColumn) {
        if (schema.getColumnType(indexColumn) != Schema.ColumnType.STRING) {
            throw new IllegalArgumentException("Index column [" + indexColumn +
                    "] is not a STRING column");
        }
        _name = name;
        _schema = schema;
        _indexColumn = indexColumn;
    }

    public String getName() { return _name; }

    public Schema getSchema() { return _schema; }

    public void addRows(List<Row> rows) {
        for (Row row : rows) {
            try {
                String indexVal = row.getField(_indexColumn).getStringValue();
                if (_rows.containsKey(_indexColumn)) {
                    _rows.get(_indexColumn).add(row);
                } else {
                    List<Row> newList = Collections.singletonList(row);
                    _rows.put(_indexColumn, newList);
                    _orderedClusters.add(newList);
                }
            } catch (InvalidTypeException ite) {
            }
        }
    }

    public List<Row> getRows() {
        List<Row> rows = new ArrayList<>();
        for (List<Row> someRows : _rows.values()) {
            rows.addAll(someRows);
        }
        return rows;
    }

    public List<Row> getRows(Split split) {
        IndexSplit indexSplit = (IndexSplit) split;
        List<Row> rows = new ArrayList<>();
        if (split.isEmpty()) {
            return rows;
        }
        for (int i = 0; i < _orderedClusters.size(); i++) {
            if (i >= indexSplit.firstRow() && i < indexSplit.lastRow()) {
                rows.addAll(_orderedClusters.get(i));
            }
        }
        return rows;
    }

    public long getRowCount() {
        long total = 0;
        for (List<Row> rows : _orderedClusters) {
            total += rows.size();
        }
        return total;
    }

    public List<Split> makeSplits() {
        return makeSplits(4);
    }

    public List<Split> makeSplits(int countDesired) {
        long clusterCount = _orderedClusters.size();
        List<Split> splits = new ArrayList<>();
        if (clusterCount < countDesired) {
            // one row per split, the last few empty
            for (long i = 0; i < clusterCount; i++) {
                splits.add(new IndexSplit(i, i + 1));
            }
            for (long i = clusterCount; i < countDesired; i++) {
                splits.add(new IndexSplit(i, i));
            }
        } else {
            // make the last one short if needed, and the rest even
            long regularSize = (long)Math.ceil((double)clusterCount / (double)countDesired);
            long splitStart = 0;
            for (int i = 0; i < countDesired; i++) {
                if (splitStart >= clusterCount) {
                    splits.add(new IndexSplit(clusterCount, clusterCount));
                } else if (splitStart + regularSize > clusterCount) {
                    splits.add(new IndexSplit(splitStart, clusterCount));
                } else {
                    splits.add(new IndexSplit(splitStart, splitStart + regularSize));
                }
                splitStart += regularSize;
            }
        }
        return splits;
    }

    private String _name;

    private Schema _schema;

    private String _indexColumn;

    private Hashtable<String, List<Row>> _rows = new Hashtable<>();

    /**
     * So we can refer to cluster ranges in splits
     */
    private List<List<Row>> _orderedClusters = new ArrayList<>();

}
