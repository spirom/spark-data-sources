package edb.common;


public class UnknownTableException extends Exception {

    public UnknownTableException(String name) {
        super("Table [" + name + "] does not exist");
    }

}
