package com.robaho.jkeydb;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static com.robaho.jkeydb.DiskIO.writeAndLoadSegment;

class Merger {
    static AtomicLong mergeSeq = new AtomicLong();

    static void mergeDiskSegments(Database db) {
        try {
            for(;;) {
                db.db_lock.acquireUninterruptibly();
                if(db.closing || db.error != null) {
                    db.db_lock.release();
                    return;
                }

                // the following prevents a Close from occurring while this
                // routine is running

                db.db_lock.release();

                try {
                    mergeDiskSegments0(db, Constants.maxSegments);
                } catch (Exception e) {
                    e.printStackTrace();
                    db.db_lock.acquireUninterruptibly();
                    db.error = e;
                    db.db_lock.release();
                }

                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            }
        } finally {
            db.wg.done();
        }
    }

    static void mergeDiskSegments0(Database db, int segmentCount) throws IOException {
        db.db_lock.acquireUninterruptibly();
        List<InternalTable> copy = new ArrayList<>(db.tables.values());
        db.db_lock.release();

        for(var table : copy){
            mergeTableSegments(db,table,segmentCount);
        }
    }

    static void mergeTableSegments(Database db,InternalTable table,int segmentCount) throws IOException {

        var index = 0;

        while(true) {

            List<Segment> segments = table.segments;
            if (segments.size() <= segmentCount) {
                return;
            }

            int maxMergeSize = segments.size() / 2;
            if(maxMergeSize < 4) {
                maxMergeSize = 4;
            }

            // ensure that only valid disk segments are merged

            ArrayList<DiskSegment> mergable = new ArrayList();

            for (Segment s : segments.subList(index,segments.size())){
                if(s instanceof DiskSegment) {
                    mergable.add((DiskSegment)s);
                    if(mergable.size() == maxMergeSize) {
                        break;
                    }
                } else {
                    break;
                }
            }

            if(mergable.size()<2) {
                index = 0;
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                continue;
            }

            long id = mergable.get(mergable.size()-1).id;
            segments = new ArrayList(segments.subList(index, index+mergable.size()));

            Segment newseg = mergeDiskSegments1(db.path, table.name, id, segments);

            table.Lock();
            while(table.transactions > 0) {
                table.Unlock();
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                table.Lock();
            }

            segments = table.segments;

            for(int i=0;i<mergable.size();i++) {
                Segment s = mergable.get(i);
                if(s != segments.get(i+index)) {
                    throw new IOException("unexpected segment change, "+ s+ " "+segments.get(i+index));
                }
            }

            for(DiskSegment s : mergable) {
                s.close();
                s.delete();
            }

            List<Segment> newsegments = new ArrayList();

            newsegments.addAll(segments.subList(0,index));
            newsegments.add(newseg);
            newsegments.addAll(segments.subList(index+mergable.size(),segments.size()));

            table.segments = newsegments;

            index++;
            table.Unlock();
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        }
    }

    private static Segment mergeDiskSegments1(String dbpath,String table,long id,List<Segment> segments) throws IOException {

        String base = Path.of(dbpath, table+".merged.").toString();

        String sid = Long.toString(id);

        long seq = mergeSeq.getAndIncrement();
        String sseq = Long.toString(seq);

        String keyFilename = base + "." + sseq + ".keys." + sid;
        String dataFilename = base + "." + sseq + ".data." + sid;

        MultiSegment ms = new MultiSegment(segments);
        LookupIterator itr = ms.lookup(null,null);
        return writeAndLoadSegment(keyFilename, dataFilename, itr);
    }
}
