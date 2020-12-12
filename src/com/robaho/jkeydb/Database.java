package com.robaho.jkeydb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

public class Database {
    static final Object global_lock = new Object();
    final Semaphore db_lock = new Semaphore(1);
    static final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        }
    });

    final Map<String,InternalTable> tables = new HashMap();
    boolean open;
    boolean closing;
    final Map<Long,Transaction> transactions = new HashMap();
    String path;
    final AtomicLong nextSeqID = new AtomicLong();
    LockFile lockFile;
    final WaitGroup wg = new WaitGroup();
    Exception error; // if non-null and async error has occurred

    public static Database open(String path,boolean createIfNeeded) throws DatabaseException {
        synchronized(global_lock){
            try {
                return openImpl(path);
            } catch(DatabaseNotFound e){
                if(createIfNeeded)
                    return create(path);
                throw e;
            }
        }
    }

    private static Database create(String path) throws DatabaseException {
        File dir = new File(path);
        if(!dir.mkdirs())
            throw new DatabaseException("unable to create directories");

        return openImpl(path);
    }

    static Database openImpl(String path) throws DatabaseInvalid,DatabaseNotFound,DatabaseOpenFailed, DatabaseInUseException {
        checkValidDatabase(path);

        String lockFilePath = path+"/lockfile";
        LockFile lockFile = null;
        try {
            lockFile = new LockFile(lockFilePath);
        } catch (IOException e) {
            throw new DatabaseOpenFailed(e);
        }
        if(!lockFile.tryLock())
            throw new DatabaseInUseException();

        Database db = new Database();
        try {
            db.path = new File(path).getCanonicalPath();
        } catch (IOException e) {
            throw new DatabaseOpenFailed(e);
        }
        db.lockFile = lockFile;
        db.open = true;
        db.wg.add(1);
        executor.submit(new Runnable() {
            @Override
            public void run() {
                Merger.mergeDiskSegments(db);
            }
        });
        return db;
    }

    private static void checkValidDatabase(String path) throws DatabaseNotFound,DatabaseInvalid {
        File f = new File(path);
        if(!f.exists()) // empty directories are valid
            throw new DatabaseNotFound();
        if(!f.isDirectory())
            throw new DatabaseInvalid();

        for (File file : f.listFiles()) {
            if(file.getName().equals("lockfile")) {
                continue;
            }
            if(file.getName().equals(f.getName()))
                continue;

            if(!file.getName().matches(".*\\.(keys|data)\\..*"))
                throw new DatabaseInvalid();
        }
    }

    public static void remove(String path) throws DatabaseException {
        synchronized (global_lock){
            checkValidDatabase(path);
            String lockFilePath = path+"/lockfile";
            LockFile lockFile = null;
            try {
                lockFile = new LockFile(lockFilePath);
            } catch (IOException e) {
                throw new DatabaseException(e);
            }
            if(!lockFile.tryLock())
                throw new DatabaseInUseException();
            File dir = new File(path);
            IOUtils.purgeDirectory(dir);
        }
    }

    public Transaction beginTX(String table) throws DatabaseException, IOException {
        db_lock.acquireUninterruptibly();

        try {
            if (error != null) {
                throw new DatabaseAsyncException(error);
            }
            if (closing) {
                throw new DatabaseException("database closed");
            }
            InternalTable it = tables.get(table);
            if (it == null) {
                it = new InternalTable(table, DiskSegment.loadDiskSegments(path, table));
                tables.put(table, it);
            }

            while (true) { // wait to start transaction if table has too many segments
                if (it.segments.size() > Constants.maxSegments * 10) {
                    db_lock.release();
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                    db_lock.acquireUninterruptibly();
                } else {
                    break;
                }
            }

            it.Lock();
            try {
                it.transactions++;
                Transaction tx = new TransactionImpl(this, table, it.segments);
                transactions.put(tx.getID(), tx);
                return tx;
            } finally {
                it.Unlock();
            }

        } finally {
            db_lock.release();
        }

    }

    public void close() throws DatabaseException, IOException {
        closeWithMerge(0);
    }

    public void closeWithMerge(int numberOfSegments) throws DatabaseException, IOException {
        synchronized(global_lock) {
            db_lock.acquireUninterruptibly();
            try {
                if (!open) {
                    throw new DatabaseException("already closed");
                }
                if (transactions.size() > 0) {
                    throw new DatabaseException("database has open transactions");
                }

                closing = true;

                db_lock.release();

                wg.waitEmpty();

                if (numberOfSegments > 0) {
                    Merger.mergeDiskSegments0(this, numberOfSegments);
                }

                for (var table : tables.values()) {
                    for (var segment : table.segments) {
                        segment.close();
                    }
                }

                open = false;
                lockFile.unlock();
            } finally {
                db_lock.release();
            }
        }
    }

    public long nextSegmentID() {
        return nextSeqID.incrementAndGet();
    }
}

class InternalTable {
    final String name;
    volatile List<Segment> segments = new CopyOnWriteArrayList<>();
    long transactions;
    private final Lock lock = new ReentrantLock();

    public InternalTable(String table, List<? extends Segment> segments) {
        this.name = table;
        this.segments.addAll(segments);
    }

    public void Lock() {
        lock.lock();
    }
    public void Unlock() {
        lock.unlock();
    }
}

