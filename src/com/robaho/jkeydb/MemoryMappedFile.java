package com.robaho.jkeydb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MemoryMappedFile {
    private final int MAX_MAP_SIZE = 1024*1024*1024; // 1GB
    private final long length;
    private final MappedByteBuffer[] buffers;
    public MemoryMappedFile(RandomAccessFile file) throws IOException {
        this.length = file.length();
        FileChannel ch = file.getChannel();
        buffers = new MappedByteBuffer[(int)((length / MAX_MAP_SIZE)+1)];
        long temp = length;
        for(int i=0;i< buffers.length;i++){
            buffers[i] = ch.map(FileChannel.MapMode.READ_ONLY,i* MAX_MAP_SIZE,Math.min(temp, MAX_MAP_SIZE));
            temp -= MAX_MAP_SIZE;
        }
    }
    long length(){
        return length;
    }

    public void readAt(ByteBuffer bbuffer, long position) {
       while(bbuffer.remaining()>0) {
           MappedByteBuffer b = buffers[(int) (position / MAX_MAP_SIZE)];
           byte[] buffer = bbuffer.array();
           ByteBuffer b0 = b.duplicate();
           b0.position((int) (position % MAX_MAP_SIZE));
           int len = Math.min(bbuffer.remaining(), b0.remaining());
           int offset = bbuffer.position();
           b0.get(buffer, bbuffer.position(), len);
           bbuffer.position(offset+len);
       }
    }

    public void close() {

    }
}
