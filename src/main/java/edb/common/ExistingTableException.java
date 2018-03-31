package edb.common;


public class ExistingTableException extends Exception {

    public ExistingTableException(String name) {
        super("Table [" + name + "] already exists");
    }

}
