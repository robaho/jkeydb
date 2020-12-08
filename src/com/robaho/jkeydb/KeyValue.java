package com.robaho.jkeydb;

public class KeyValue {
    public final byte[] key;
    public final byte[] value;

    public KeyValue(byte[] key, byte[] value) {
        this.key=key;
        this.value=value;
    }
}
