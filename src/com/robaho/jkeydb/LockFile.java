package com.robaho.jkeydb;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class LockFile {
    private Path path;
    FileChannel fch;
    private FileLock lock;

    public LockFile(String path) throws IOException {
        this.path = Path.of(path);
    }

    public boolean tryLock() {
        try {
            fch = FileChannel.open(path, StandardOpenOption.WRITE,StandardOpenOption.CREATE);
            lock = fch.tryLock();
            return lock!=null;
        } catch (IOException e) {
            return false;
        }
    }

    public void unlock() {
        if(lock!=null) {
            try {
                lock.release();
                fch.close();
            } catch (IOException ignore) {
            }
        }
    }
}
