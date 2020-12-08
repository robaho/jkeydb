package com.robaho.jkeydb;

import java.io.IOException;

public class DatabaseOpenFailed extends DatabaseException {
    public DatabaseOpenFailed(IOException e) {
        super(e);
    }
}
