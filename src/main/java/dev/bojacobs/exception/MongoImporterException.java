package dev.bojacobs.exception;

/**
 * @author Bo Jacobs
 */
public class MongoImporterException extends Exception {

    public MongoImporterException(String message) {
        super(message);
    }

    public MongoImporterException(String message, Throwable cause) {
        super(message, cause);
    }
}
