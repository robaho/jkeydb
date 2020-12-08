package com.robaho.jkeydb;

import java.io.IOException;

interface LookupIteratorInternal {
    static final byte[] EMPTY_KEY = new byte[0];

    /**
     * @return null if there are no more keys, EMPTY_KEY if the current key is deleted and should be skipped
     * @throws IOException
     */
    byte[] peekKey() throws IOException;
}
public interface LookupIterator extends LookupIteratorInternal {
    /**
     * @return null if there are no more keys
     * @throws IOException
     */
    KeyValue next() throws IOException;
}

