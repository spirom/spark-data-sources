package edb.common;


public class ExistingTableException extends Exception {

    public ExistingTableException(String name) {
        super("Table [" + name + "] already exists");
        _tableName = name;
    }

    public String getTableName() { return _tableName; }

    private String _tableName;

}
