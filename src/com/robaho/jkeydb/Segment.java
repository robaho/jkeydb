package com.robaho.jkeydb;

import java.io.IOException;

interface Segment {
    void put(byte[] key,byte[] value) throws IOException;
    byte[] get(byte[] key) throws IOException;
    byte[] remove(byte[] key) throws IOException;
    void close() throws IOException;
    LookupIterator lookup(byte[] lower,byte[] upper) throws IOException;
}
