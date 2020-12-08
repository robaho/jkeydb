package com.robaho.jkeydb;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

class MemorySegmentIterator implements LookupIterator {
    private Iterator<Map.Entry<byte[],byte[]>> itr;
    private Map.Entry<byte[],byte[]> next;

    public MemorySegmentIterator(SortedMap<byte[], byte[]> tree) {
        itr = tree.entrySet().iterator();
    }

    @Override
    public byte[] peekKey() throws IOException {
        if(next!=null)
            return next.getKey();
        if(!itr.hasNext())
            return null;
        next = itr.next();
        return next.getKey();
    }

    @Override
    public KeyValue next() throws IOException {
        try {
            if (next != null)
                return new KeyValue(next.getKey(),next.getValue());
            if (!itr.hasNext())
                return null;
            next = itr.next();
            return new KeyValue(next.getKey(),next.getValue());
        } finally {
            next = null;
        }
    }
}
