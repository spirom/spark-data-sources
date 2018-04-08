package edb.common;

import java.util.List;

/**
 * ExampleDB API -- observed on botht he client library and the server implementation.
 *
 * Some current limitations:
 * -- no real transactions yet
 * -- no nulls
 * -- no complex types -- tables are truly relational
 * -- only types are string, long and double
 * -- only filtering is boolean combinations of equality with a literal
 */
public interface IExampleDB {

    /**
     * Get names oll existing tables.
     * @return All table names in no particular order.
     */
    List<String> listTables();

    /**
     * Create a table with the specified schema.
     * @param name Table name
     * @param schema Table schema
     * @throws ExistingTableException If a table of the same name already exists
     */
    void createTable(String name, Schema schema) throws ExistingTableException;

    /**
     * Create a table with the specified schema.
     * @param name Table name
     * @param schema Table schema
     * @param clusterColumn One column of the schema to be used for a clustered index and
     *                      for split computation so each cluster is entirely within one split.
     * @throws ExistingTableException If a table of the same name already exists
     */
    void createTable(String name, Schema schema, String clusterColumn)
            throws ExistingTableException;

    /**
     * Create a temporary table with a unique name and the given schema. This table cannot
     * have a clustered index.
     * @param schema THe schema of the table to be created
     * @return THe unique name of the table created
     */
    String createTemporaryTable(Schema schema);

    /**
     * Get the schema of the given table
     * @param name The table name
     * @return THe table's schema, if it exists
     * @throws UnknownTableException If a table with the given name does not exist
     */
    Schema getTableSchema(String name) throws UnknownTableException;

    /**
     * Get the schema of the given table
     * @param name The table name
     * @return THe table's cluster column if there is one, or null if there isn't
     * @throws UnknownTableException If a table with the given name does not exist
     */
    String getTableClusteredIndexColumn(String name) throws UnknownTableException;

    /**
     * Insert the given rows into the named table in a single operation
     * @param name The table name
     * @param rows The rows to be inserted
     * @throws UnknownTableException If a table of the given name does not exist
     */
    void bulkInsert(String name, List<Row> rows) throws UnknownTableException;

    /**
     * Insert all rows from all given tables in a single transaction, deleting any tables that
     * happened to be temporary.
     * @param name Name of destination table
     * @param truncateDestination Whether to truncate the destination table before inserting
     * @param tables Name of source tables -- each of which may or may not be temporary
     * @throws UnknownTableException If any of the tables cannot be found
     */
    void bulkInsertFromTables(String name, boolean truncateDestination, List<String> tables) throws UnknownTableException;

    /**
     * Fetch all columns from rows from the specified table.
     * @param name The table name
     * @return ALl rows int he table, which may be none as the table may be empty
     * @throws UnknownTableException
     */
    List<Row> getAllRows(String name) throws UnknownTableException;

    /**
     * Fetch specified columns from all rows from the specified table.
     * @param name The table name
     * @param columns The columns to include, in order
     * @return ALl rows int he table, which may be none as the table may be empty
     * @throws UnknownTableException
     */
    List<Row> getAllRows(String name, List<String> columns) throws UnknownTableException;

    /**
     * Fetch all rows for the specified split from the specified table.
     * @param name Tha name of the table
     * @param split The split for which rows should be obtained
     * @return The rows for the split, which may be none of this split is empty
     * @throws UnknownTableException
     */
    List<Row> getAllRows(String name, Split split) throws UnknownTableException;

    /**
     * Fetch specified columns from all rows for the specified split from the specified table.
     * @param name Tha name of the table
     * @param split The split for which rows should be obtained
     * @param columns The columns to include, in order
     * @return The rows for the split, which may be none of this split is empty
     * @throws UnknownTableException
     */
    List<Row> getAllRows(String name, Split split, List<String> columns) throws UnknownTableException;

    /**
     * Get the default number of splits for the specified table. One or more splits may be empty.
     * @param table The table name
     * @return The splits if the table exists, in no particular order
     * @throws UnknownTableException If no table with the specified name exists
     */
    List<Split> getSplits(String table) throws UnknownTableException;

    /**
     * Get the specified number of splits for the specified table. One or more splits may be empty.
     * @param table The table name
     * @param count The required number of splits
     * @return The splits if the table exists, in no particular order
     * @throws UnknownTableException
     */
    List<Split> getSplits(String table, int count) throws UnknownTableException;




}
