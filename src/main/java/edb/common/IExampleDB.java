package edb.common;

import java.util.List;

public interface IExampleDB {
    List<String> listTables();

    void createTable(String name, Schema schema) throws ExistingTableException;

    void createTable(String name, Schema schema, String clusterColumn)
            throws ExistingTableException;

    Schema getTableSchema(String name) throws UnknownTableException;

    void bulkInsert(String name, List<Row> rows) throws UnknownTableException;

    List<Row> getAllRows(String name) throws UnknownTableException;

    List<Row> getAllRows(String name, Split split) throws UnknownTableException;

    List<Split> getSplits(String table) throws UnknownTableException;

    List<Split> getSplits(String table, int count) throws UnknownTableException;
}
