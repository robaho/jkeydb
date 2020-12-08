package com.robaho.jkeydb;

public class DatabaseAsyncException extends DatabaseException {
    public DatabaseAsyncException(Exception error) {
        super(error);
    }
}
