package com.robaho.jkeydb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static com.robaho.jkeydb.Constants.*;

class DiskSegment implements Segment {
    final MemoryMappedFile keyFile;
    long keyBlocks;
    final MemoryMappedFile dataFile;
    final long id;

    final private String keyfilename;
    final private String datafilename;

    // nil for segments loaded during initial open
    // otherwise holds the key for every keyIndexInterval block
    final List<byte[]> keyIndex;

    public DiskSegment(String keyFilename, String dataFilename, List<byte[]> keyIndex) throws IOException {
        this.keyfilename = keyFilename;
        this.datafilename = dataFilename;

        keyFile = new MemoryMappedFile(new RandomAccessFile(new File(keyFilename),"r"));
        dataFile = new MemoryMappedFile(new RandomAccessFile(dataFilename,"r"));
        this.id = getSegmentID(keyFilename);
        this.keyBlocks = (keyFile.length()-1)/keyBlockSize + 1;

        if(keyIndex == null) {
            // TODO maybe load this in the background
            keyIndex = loadKeyIndex(keyFile, keyBlocks);
        }
        this.keyIndex = keyIndex;

    }

    static List<byte[]> loadKeyIndex(MemoryMappedFile keyFile,long keyBlocks) throws IOException {
        byte[] buffer = new byte[keyBlockSize];
        ByteBuffer bb = ByteBuffer.wrap(buffer).order(ByteOrder.BIG_ENDIAN);
        List<byte[]> keyIndex = new ArrayList<byte[]>();
        long block;

        for(block = 0; block < keyBlocks; block += keyIndexInterval) {
            bb.clear();
            keyFile.readAt(bb,block*keyBlockSize);
            int keylen = bb.getShort(0) & 0xFFFF;
            if(keylen == endOfBlock) {
                break;
            }
            byte[] keycopy = new byte[keylen];
            System.arraycopy(buffer,2,keycopy,0,keylen);
            keyIndex.add(keycopy);
        }
        return keyIndex;
    }

    static List<DiskSegment> loadDiskSegments(String path, String table) throws IOException {
        List<DiskSegment> segments = new ArrayList();
        File dir = new File(path);
        if(!dir.isDirectory())
            return segments;

        for (var file : dir.listFiles()) {
            if (file.getName().endsWith(".tmp")) {
                throw new IllegalStateException("tmp files in " + path);
            }
            if (file.getName().startsWith(table+".")) {
                int index = file.getName().indexOf(".keys.");
                if(index < 0) {
                    continue;
                }
                String base = file.getName().substring(0,index);
                long id = getSegmentID(file.getName());
                String keyFilename = path + "/" +  base+".keys."+id;
                String dataFilename = path + "/" + base+".data."+id;
                segments.add(new DiskSegment(keyFilename, dataFilename, null)); // don't have keyIndex
            }
        }
        Collections.sort(segments,
                new Comparator<DiskSegment>() {
                    @Override
                    public int compare(DiskSegment o1, DiskSegment o2) {
                        return Long.compare(o1.id,o2.id);
                    }
                });
        return segments;
    }

    private static long getSegmentID(String name) {
        int index = name.lastIndexOf('.');
        if(index >= 0) {
            return Long.parseLong(name.substring(index+1));
        }
        return 0;
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        throw new IllegalStateException("disk segments are immutable");
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        OffsetLen ol = binarySearch(key);
        if(ol==null)
            return null;

        byte[] buffer = new byte[ol.len];
        dataFile.readAt(ByteBuffer.wrap(buffer).order(ByteOrder.BIG_ENDIAN),ol.offset);
        return buffer;
    }

    @Override
    public byte[] remove(byte[] key) throws IOException {
        throw new IllegalStateException("disk segments are immutable");
    }

    @Override
    public void close() throws IOException {
        keyFile.close();
        dataFile.close();
    }

    public void delete() throws IOException {
        Files.delete(Path.of(keyfilename));
        Files.delete(Path.of(datafilename));
    }

    @Override
    public LookupIterator lookup(byte[] lower, byte[] upper) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(keyBlockSize).order(ByteOrder.BIG_ENDIAN);
        long block = 0;
        if(lower != null) {
            long startBlock = binarySearch0(0, keyBlocks-1, lower, buffer);
            if(startBlock<0)
                return null;
            block = startBlock;
        }
        buffer.clear();
        keyFile.readAt(buffer,block* keyBlockSize);
        buffer.flip();
        return new DiskSegmentIterator(this,lower,upper,buffer,block);
    }

    private static class OffsetLen {
        final long offset;
        final int len;
        OffsetLen(long offset,int len){
            this.offset = offset;
            this.len = len;
        }
    }

    private OffsetLen binarySearch(byte[] key) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(maxKeySize+2).order(ByteOrder.BIG_ENDIAN); // enough to hold the first key of key block

        long lowblock = 0;
        long highblock = keyBlocks - 1;

        if (keyIndex != null) { // we have memory index, so narrow block range down
//            System.out.println("looking for "+new String(key));
//            for(byte[] b : keyIndex) {
//                System.out.println(new String(b));
//            }
            int index = Collections.binarySearch(keyIndex,key,new Comparator<byte[]>() {
                @Override
                public int compare(byte[] o1, byte[] o2) {
                    return Arrays.compare(o1,o2);
                }
            });
            if(index>=0) {
                highblock = lowblock = index* keyIndexInterval;
            } else {
                index = (index*-1) - 1;
                if(index == 0)  {
                    return null;
                }
                index--;

                lowblock = index * keyIndexInterval;
                highblock = lowblock + keyIndexInterval;
            }

            if(highblock >= keyBlocks) {
                highblock = keyBlocks - 1;
            }
        }

        long block = binarySearch0(lowblock, highblock, key, buffer);
        return scanBlock(block, key);
    }

    private static int compareKeys(byte[] b,ByteBuffer bb){
        short len = bb.getShort();
        for(int i=0;i<len;i++){
            int result = Byte.compare(b[i],bb.get());
            if(result!=0) {
                bb.position(bb.position()+len-i-1);
                return result;
            }
        }
        return 0;
    }

    long binarySearch0(long lowBlock,long highBlock,byte[] key,ByteBuffer buffer) throws IOException {
        if(highBlock-lowBlock <= 1) {
            // the key is either in low block or high block, or does not exist, so check high block
            buffer.clear();
            keyFile.readAt(buffer, highBlock* keyBlockSize);
            buffer.flip();

            if(compareKeys(key,buffer)<0) {
                return lowBlock;
            } else {
                return highBlock;
            }
        }

        long block = (highBlock-lowBlock)/2 + lowBlock;

        buffer.clear();
        keyFile.readAt(buffer, block*Constants.keyBlockSize);
        buffer.flip();

        if(compareKeys(key,buffer)<0) {
            return binarySearch0(lowBlock, block, key, buffer);
        } else {
            return binarySearch0(block, highBlock, key, buffer);
        }
    }

    static final ThreadLocal<ByteBuffer> bufferCache = new ThreadLocal<>(){
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocateDirect(keyBlockSize);
        }
    };
    OffsetLen scanBlock(long block,byte[] key) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[keyBlockSize]);

        buffer.clear();
        keyFile.readAt(buffer, block*Constants.keyBlockSize);
        buffer.flip();

        byte[] prevKey = null;
        for(;;) {
            int keylen = buffer.getShort() & 0xFFFF;
            if(keylen == endOfBlock) {
                return null;
            }
            byte[] _key = CompressedKey.decodeKey(keylen,prevKey,buffer);
            prevKey = _key;

            long offset = buffer.getLong();
            int len = buffer.getInt();

            int result = Arrays.compare(_key,key);
            if(result==0) {
                if(len == Constants.removedKeyLen) {
                    return null;
                }
                return new OffsetLen(offset,len);
            }
            if(result>0) {
                return null;
            }
        }
    }

}
