package com.robaho.jkeydb;

import java.io.IOException;

public interface Transaction {
    long getID();
    byte[] get(byte[] key) throws IOException;
    void put(byte[] key, byte[] value) throws IOException;
    byte[] remove(byte[] key) throws IOException;
    void rollback();
    void commit();
    void commitSync() throws IOException;
    LookupIterator lookup(byte[] lower,byte[] upper) throws IOException;
}
