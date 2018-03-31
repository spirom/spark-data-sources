package edb.common;

public class InvalidTypeException extends Exception {

    public InvalidTypeException(String name) {
        super("Field [" + name + "] is of the wrong type");
    }

}
