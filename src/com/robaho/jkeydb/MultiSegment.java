package com.robaho.jkeydb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class MultiSegment implements Segment {
    private final List<Segment> segmentList;

    public MultiSegment(List<Segment> segmentList) {
        this.segmentList=segmentList;
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        throw new IllegalStateException("Put called on multiSegmentIterator");
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        // segments are in chronological order, so search in reverse
        for (int i = segmentList.size()-1; i >=0; i--) {
            Segment s = segmentList.get(i);
            byte[] val = s.get(key);
            if (val!=null) {
                return val;
            }
        }
        return null;
    }

    @Override
    public byte[] remove(byte[] key) throws IOException {
        throw new IllegalStateException("Remove called on multiSegmentIterator");
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public LookupIterator lookup(byte[] lower, byte[] upper) throws IOException {
        var iterators = new ArrayList<LookupIterator>();
        for (var v : segmentList) {
            LookupIterator i = v.lookup(lower, upper);
            iterators.add(i);
        }
        return new MultiSegmentIterator(iterators);
    }
}
