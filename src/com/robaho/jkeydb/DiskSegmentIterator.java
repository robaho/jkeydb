package com.robaho.jkeydb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.robaho.jkeydb.Constants.removedKeyLen;

class DiskSegmentIterator implements LookupIterator{
    private boolean isValid = false;
    private boolean finished = false;

    byte[] key;
    byte[] data;

    final DiskSegment segment;
    ByteBuffer buffer;

    final byte[] lower,upper;

    long block;

    DiskSegmentIterator(DiskSegment segment,byte[] lower,byte[] upper,ByteBuffer buffer,long block){
        this.segment = segment;
        this.lower = lower;
        this.upper = upper;
        this.buffer = buffer;
        this.block = block;
    }

    @Override
    public byte[] peekKey() throws IOException {
        if(isValid){
            return key;
        }
        if(nextKeyValue()) {
            return null;
        }
        return key;
    }

    @Override
    public KeyValue next() throws IOException {
        if(isValid) {
            isValid = false;
            return new KeyValue(key,data);
        }
        try {
            if (nextKeyValue())
                return null;

            return new KeyValue(key, data);
        } finally {
            isValid = false;
        }
    }

    /** returns true if no more values */
    private boolean nextKeyValue() throws IOException {
        if(finished) {
            return true;
        }
        var prevKey = key;

        while(true) {
            int keylen = buffer.getShort() & 0xFFFF;
            if(keylen == Constants.endOfBlock) {
                block++;
                if (block == segment.keyBlocks) {
                    finished = true;
                    key = null;
                    data = null;
                    isValid = true;
                    return true;
                }
                buffer.clear();
                int n = segment.keyFile.getChannel().read(buffer,block*Constants.keyBlockSize);
                if(n!=buffer.capacity())
                    throw new IOException("unable to read keyfile");
                buffer.flip();
                prevKey = null;
                continue;
            }

            key = CompressedKey.decodeKey(keylen,prevKey,buffer);
            prevKey = key;

            long dataoffset = buffer.getLong();
            int datalen = buffer.getInt();

            if(lower != null) {
                if (Arrays.compare(key, lower)<0) {
                    continue;
                }
            }
            if(upper != null) {
                if(Arrays.compare(key, upper)>0) {
                    finished = true;
                    isValid = true;
                    key = null;
                    data = null;
                    return true;
                }
            }
            found:

            if (datalen == removedKeyLen) {
                data = null;
            } else {
                data = new byte[datalen];
                int n = segment.dataFile.getChannel().read(ByteBuffer.wrap(data),dataoffset);
                if(n!=datalen)
                    throw new IOException("unable to read data file, expecting "+datalen+", read "+n);
            }
            isValid = true;
            return false;
        }
    }


}


