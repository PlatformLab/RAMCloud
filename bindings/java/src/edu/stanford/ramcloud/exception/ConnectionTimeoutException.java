package edu.stanford.ramcloud.exception;

public class ConnectionTimeoutException extends Exception {
    public ConnectionTimeoutException(String message) {
        super(message);
    }
}
