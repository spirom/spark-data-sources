package edb.common;

import edb.common.Split;

import java.io.*;

/**
 * Denotes a subset of table rows via a pair of zero-based first and last rows.
 */
public class IndexSplit implements Split, Serializable {
    /**
     * If the two are equal, this split is empty
     * @param firstRow (inclusive)
     * @param lastRow (exclusive)
     */
    public IndexSplit(long firstRow, long lastRow) {
        _firstRow = firstRow;
        _lastRow = lastRow;
    }

    public boolean isEmpty() { return _lastRow <= _firstRow; }

    public long firstRow() { return _firstRow; }

    public long lastRow() { return _lastRow; }

    public byte[] serialize() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(SplitKind.INDEX);
            out.writeObject(_firstRow);
            out.writeObject(_lastRow);
            out.flush();
            return bos.toByteArray();
        } catch (IOException ex) {
            // ignore exception
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return null;
    }

    private long _firstRow;
    private long _lastRow;
}
