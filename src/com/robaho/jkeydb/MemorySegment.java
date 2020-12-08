package com.robaho.jkeydb;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeMap;

class MemorySegment implements Segment {
    TreeMap<byte[],byte[]> tree = new TreeMap<>(new Comparator<>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
            return Arrays.compare(o1,o2);
        }
    });

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        tree.put(key, value);
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        byte[] value = tree.get(key);
        if(value==LookupIterator.EMPTY_KEY)
            return null;
        return value;
    }

    @Override
    public byte[] remove(byte[] key) throws IOException {
        return tree.put(key,LookupIterator.EMPTY_KEY);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public LookupIterator lookup(byte[] lower, byte[] upper) throws IOException {
        if(lower==null && upper==null)
            return new MemorySegmentIterator(tree);

        if(lower==null){
            return new MemorySegmentIterator(tree.headMap(upper,true));
        } else if(upper==null) {
            return new MemorySegmentIterator(tree.tailMap(lower,true));
        } else {
            return new MemorySegmentIterator(tree.subMap(lower,true,upper,true));
        }
    }
}

