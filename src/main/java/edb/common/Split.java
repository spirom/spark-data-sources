package edb.common;

import java.io.*;

enum SplitKind {
    SIMPLE, INDEX
};

/**
 * Denotes a (possibly empty) subset of table rows.
 */
public interface Split {

    /**
     * If true, there are no rows int his split. If false, there may or may not be rows.
     * @return
     */
    boolean isEmpty();

    static Split deserialize(byte[] splitBytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(splitBytes);
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            SplitKind k = (SplitKind) in.readObject();
            switch (k) {
                case SIMPLE: {
                    Object first = in.readObject();
                    Object last = in.readObject();
                    return new SimpleSplit(
                            ((Long)first).longValue(),
                            ((Long)last).longValue());
                }
                case INDEX: {
                    Object first = in.readObject();
                    Object last = in.readObject();
                    return new IndexSplit(
                            ((Long)first).longValue(),
                            ((Long)last).longValue());
                }
                default:
                    return null;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return null;
    }

    byte[] serialize();

}
