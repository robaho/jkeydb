package com.robaho.jkeydb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

class TransactionImpl implements Transaction {
    private final Database db;
    private final MultiSegment multi;
    private final MemorySegment memory;
    private final String table;

    static final AtomicLong txID = new AtomicLong(1);

    long id;
    boolean open;

    TransactionImpl(Database db, String table, List<Segment> segments){
        this.db = db;
        this.table = table;
        this.memory = new MemorySegment();
        List<Segment> newSegments = new ArrayList<>(segments);
        newSegments.add(memory);
        this.multi = new MultiSegment(newSegments);
        this.id = txID.getAndIncrement();
        this.open=true;
    }

    @Override
    public long getID() {
        return id;
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        if(!open) {
            throw new IllegalStateException("transaction closed");
        }
        if( key.length > 1024 ){
            throw new IllegalArgumentException("key > 1024");
        }
        return multi.get(key);
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        if(!open) {
            throw new IllegalStateException("transaction closed");
        }
        if( key.length > 1024 ){
            throw new IllegalArgumentException("key > 1024");
        }
        if( key.length <= 0 ){
            throw new IllegalArgumentException("empty key");
        }
        memory.put(key, value);
    }

    @Override
    public byte[] remove(byte[] key) throws IOException {
        if(!open) {
            throw new IllegalStateException("transaction closed");
        }
        if( key.length > 1024 ){
            throw new IllegalArgumentException("key > 1024");
        }
        if( key.length <= 0 ){
            throw new IllegalArgumentException("empty key");
        }
        byte[] value = get(key);
        if(value==null)
            return null;
        memory.remove(key);
        return value;
    }

    @Override
    public void rollback() {
        if(!open)
            throw new IllegalStateException("transaction closed");

        db.dblock.acquireUninterruptibly();
        try {
            var it = db.tables.get(table);
            it.Lock();
            try{
                db.transactions.remove(id);
                it.transactions--;
            }finally {
                it.Unlock();
            }
        } finally {
            open = false;
            db.dblock.release();
        }
    }

    @Override
    public void commit() {
        try {
            commitImpl(false);
        } catch (IOException ignore) {
            // writing is async so no exception here
        }
    }

    @Override
    public void commitSync() throws IOException {
        commitImpl(true);
    }

    private void commitImpl(boolean sync) throws IOException {
        if(!open)
            throw new IllegalStateException("transaction closed");

        db.dblock.acquireUninterruptibly();
        try {
            db.transactions.remove(id);
            open = false;
            InternalTable it = db.tables.get(table);
            it.Lock();
            try {
                it.transactions--;
                it.segments.add(memory);
            } finally {
                it.Unlock();
            }

            if(sync)
                DiskIO.writeSegmentToDisk(db, table, memory);
            else{
                Database.executor.submit(() -> {
                    try {
                        DiskIO.writeSegmentToDisk(db, table, memory);
                    } catch (Exception e) {
                        db.dblock.acquireUninterruptibly();
                        db.error = e;
                        db.dblock.release();
                    } finally {
                        db.wg.done();
                    }
                });
            }
        } finally {
            db.dblock.release();
        }
    }


    @Override
    public LookupIterator lookup(byte[] lower, byte[] upper) throws IOException {
        if(!open) {
            throw new IllegalStateException("transaction closed");
        }
        var itr = multi.lookup(lower, upper);
        return new TransactionLookup(itr);
    }

    private static class TransactionLookup implements LookupIterator {
        private final LookupIterator itr;
        public TransactionLookup(LookupIterator itr) {
            this.itr = itr;
        }

        @Override
        public byte[] peekKey() throws IOException {
            throw new IllegalStateException("should never be called");
        }

        @Override
        public KeyValue next() throws IOException {
            while(true) {
                KeyValue kv = itr.next();
                if(kv!=null && kv.key == LookupIterator.EMPTY_KEY) {
                    continue;
                }
                return kv;
            }
        }
    }
}
