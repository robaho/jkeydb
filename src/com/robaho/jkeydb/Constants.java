package com.robaho.jkeydb;

class Constants {
    static final int keyBlockSize = 4096;
    static final int maxKeySize = 1000;
    static final int endOfBlock = 0x8000;
    static final int compressedBit = 0x8000;
    static final int maxPrefixLen = 0xFF ^ 0x80;
    static final int maxCompressedLen = 0xFF;
    static final int keyIndexInterval = 16;
    static final int removedKeyLen = 0xFFFFFFFF;
    static final int maxSegments = 8;
}
