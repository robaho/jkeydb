package com.robaho.jkeydb;

import sun.misc.Unsafe;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

final class UnsafeUtils {
    private static final Unsafe unsafe;

    public static Unsafe getUnsafe() {
        return unsafe;
    }

    static {
        Field f;
        try {
            f = Unsafe.class.getDeclaredField("theUnsafe");
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException("unable to get Unsafe",e);
        }

        f.setAccessible(true);

        try {
            unsafe = (Unsafe)f.get((Object)null);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("unable to get Unsafe",e);
        }
    }
}

public class MemoryMappedFile {
    private final int MAX_MAP_SIZE = 1024*1024*1024; // 1GB
    private final long length;
    private final MappedByteBuffer[] buffers;
    private boolean closed;

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

    public void readAt(ByteBuffer bbuffer, long position) throws IOException {
        if(closed)
            throw new IOException("memory mapped file is closed");

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
        for(MappedByteBuffer buffer : buffers) {
            UnsafeUtils.getUnsafe().invokeCleaner(buffer);
        }
        closed = true;
    }
}
