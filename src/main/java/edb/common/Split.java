package edb.common;

import java.io.Serializable;

/**
 * Denotes a subset of table rows via a pair of zero-based first and last rows.
 */
public class Split implements Serializable {
    /**
     * If the two are equal, this split is empty
     * @param firstRow (inclusive)
     * @param lastRow (exclusive)
     */
    public Split(long firstRow, long lastRow) {
        _firstRow = firstRow;
        _lastRow = lastRow;
    }

    public boolean isEmpty() { return _lastRow <= _firstRow; }

    public long firstRow() { return _firstRow; }

    public long lastRow() { return _lastRow; }

    private long _firstRow;
    private long _lastRow;
}
