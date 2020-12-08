package com.robaho.jkeydb;

import java.nio.ByteBuffer;

class CompressedKey {
    static byte[] decodeKey(int keylen,byte[] prevKey,ByteBuffer buffer){
        DecodedKeyLen dkyl= decodeKeyLen(keylen);
        byte[] key = new byte[dkyl.compressedLen];
        buffer.get(key);
        return decodeKey(key, prevKey, dkyl.prefixLen);
    }

    private static class DecodedKeyLen {
        final int prefixLen,compressedLen;
        DecodedKeyLen(int prefixLen,int compressedLen){
            this.prefixLen = prefixLen;
            this.compressedLen = compressedLen;
        }
    }

    private static DecodedKeyLen decodeKeyLen(int keylen) {

        if ((keylen & Constants.compressedBit) != 0) {
            return new DecodedKeyLen(
                    ((keylen >> 8) & 0xFFFF & Constants.maxPrefixLen) ,
                    (keylen & Constants.maxCompressedLen));
        } else {
            if (keylen > Constants.maxKeySize) {
                throw new IllegalStateException("key > 1024");
            }
            if (keylen <= 0) {
                throw new IllegalStateException("key <= 0");
            }
            return new DecodedKeyLen(0,keylen);
        }
    }
    private static byte[] decodeKey(byte[] key, byte[] prevKey,int prefixLen) {
        if (prefixLen != 0) {
            byte[] newkey = new byte[prefixLen+key.length];
            System.arraycopy(prevKey,0,newkey,0,prefixLen);
            System.arraycopy(key,0,newkey,prefixLen,key.length);
            return newkey;
        }
        return key;
    }

}
