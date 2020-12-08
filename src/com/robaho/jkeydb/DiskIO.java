package com.robaho.jkeydb;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

class DiskIO {
    // called to write a memory segment to disk
    static void writeSegmentToDisk(Database db,String table,Segment seg) throws IOException {
        var itr = seg.lookup(null,null);
        var id = db.nextSegmentID();

        var keyFilename = Path.of(db.path,table+".keys."+id).toString();
        var dataFilename = Path.of(db.path,table+".data."+id).toString();

        DiskSegment ds = writeAndLoadSegment(keyFilename, dataFilename, itr);

//            if err != nil && err != errEmptySegment {
//                return err
//            }

        InternalTable it = db.tables.get(table);
        it.Lock();
        try {
            List<Segment> segments = new ArrayList<>();
            for(var v : it.segments) {
                if(v == seg) {
                    if(ds != null) {
                        segments.add(ds);
                    }
                } else{
                    segments.add(v);
                }
            }

            it.segments = segments;
        } finally {
            it.Unlock();
        }
    }

    static DiskSegment writeAndLoadSegment(String keyFilename, String dataFilename,LookupIterator itr) throws IOException {
        var keyFileTmp = new File(keyFilename + ".tmp");
        var dataFileTmp = new File(dataFilename + ".tmp");

        List<byte[]> keyIndex = null;
        try {
            keyIndex = writeSegmentFiles(keyFileTmp, dataFileTmp, itr);
        } catch (IOException e) {
            keyFileTmp.delete();
            dataFileTmp.delete();
            throw e;
        }

        keyFileTmp.renameTo(new File(keyFilename));
        dataFileTmp.renameTo(new File(dataFilename));

        return new DiskSegment(keyFilename, dataFilename, keyIndex);
    }

    static List<byte[]> writeSegmentFiles(File keyFile, File dataFile,LookupIterator itr) throws IOException {
        var keyW = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(keyFile)));
        var dataW = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dataFile)));

        long dataOffset=0;
        int keyBlockLen=0;
        int keyCount=0;
        int block=0;

        byte[] zeros = new byte[Constants.keyBlockSize];
        byte[] prevKey = null;

        List<byte[]> keyIndex = new ArrayList<>();

        while (true) {
            KeyValue kv = itr.next();
            if(kv==null)
                break;

            byte[] value = kv.value;
            byte[] key = kv.key;

            keyCount++;

            dataW.write(value);
            if (keyBlockLen + 2 + key.length + 8 + 4 >= Constants.keyBlockSize - 2) {
                // need to leave room for 'end of block marker'
                // key won't fit in block so move to next
                keyW.writeShort(Constants.endOfBlock);
                keyBlockLen += 2;
                keyW.write(zeros, 0, Constants.keyBlockSize - keyBlockLen);
                keyBlockLen = 0;
                prevKey = null;
            }

            if (keyBlockLen == 0) {
                if (block % Constants.keyIndexInterval == 0) {
                    keyIndex.add(key.clone());
                }
                block++;
            }

            int dataLen;
            if (value == null) {
                dataLen = Constants.removedKeyLen;
            } else {
                dataLen = value.length;
            }

            var dk = encodeKey(key, prevKey);
            prevKey = key.clone();
            keyW.writeShort(dk.keylen);
            keyW.write(dk.compressedKey);
            keyW.writeLong(dataOffset);
            keyW.writeInt(dataLen);

            keyBlockLen += 2 + dk.compressedKey.length + 8 + 4;
            if (value != null) {
                dataOffset += dataLen;
            }
        }

        // pad key file to block size
        if (keyBlockLen > 0 && keyBlockLen < Constants.keyBlockSize) {
            // key won't fit in block so move to next
            keyW.writeShort(Constants.endOfBlock);
            keyBlockLen += 2;
            keyW.write(zeros, 0, Constants.keyBlockSize - keyBlockLen);
            keyBlockLen = 0;
        }

        keyW.flush();
        dataW.flush();

        if (keyCount == 0) {
            return null;
        }

        return keyIndex;
    }

    private static DiskKey encodeKey(byte[] key,byte[] prevKey) {
       int  prefixLen = calculatePrefixLen(prevKey, key);
        if (prefixLen > 0) {
            var key_ = new byte[key.length-prefixLen];
            System.arraycopy(key,prefixLen,key_,0,key_.length);
            return new DiskKey(Constants.compressedBit | (prefixLen<<8) | key_.length,key_);
        }
        return new DiskKey(key.length,key);
    }

    private static class DecodedKeyLen {
        final int prefixLen;
        final int compressedLen;

        public DecodedKeyLen(int prefixLen, int compressedLen) {
            this.prefixLen = prefixLen;
            this.compressedLen = compressedLen;
        }
    }

    private static DecodedKeyLen decodeKeyLen(short keylen) {
        int prefixLen=0,compressedLen=0;
        if ((keylen & Constants.compressedBit) != 0) {
            prefixLen = (keylen >> 8) & Constants.maxPrefixLen;
            compressedLen = keylen & Constants.maxCompressedLen;
            if (prefixLen > Constants.maxPrefixLen || compressedLen > Constants.maxCompressedLen) {
                throw new IllegalStateException("invalid prefix/compressed length,"+prefixLen+" "+compressedLen);
            }
        } else {
            if(keylen > Constants.maxKeySize) {
                throw new IllegalStateException("key > 1024");
            }
            compressedLen = keylen;
        }
        if(compressedLen == 0) {
            throw new IllegalStateException("decoded key length is 0");
        }
        return new DecodedKeyLen(prefixLen,compressedLen);
    }

    private static class DiskKey {
        final int keylen;
        final byte[] compressedKey;

        public DiskKey(int keylen, byte[] key) {
            this.keylen = keylen;
            this.compressedKey = key;
        }
    };


    private static byte[] decodeKey(byte[] key,byte[] prevKey,int prefixLen) {
        if(prefixLen==0)
            return key;
        byte[] key_ = new byte[prefixLen+key.length];
        System.arraycopy(prevKey,0,key_,0,prefixLen);
        System.arraycopy(key,0,key_,prefixLen,key.length);
        return key_;
    }

    private static int calculatePrefixLen(byte[] prevKey,byte[] key) {
        if(prevKey == null) {
            return 0;
        }
        int length = 0;

        for( ; length < prevKey.length && length < key.length; length++) {
            if(prevKey[length] != key[length]) {
                break;
            }
        }
        if(length > Constants.maxPrefixLen || key.length-length > Constants.maxCompressedLen) {
            length = 0;
        }
        return length;
    }
}
