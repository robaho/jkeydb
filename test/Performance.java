import com.robaho.jkeydb.*;

import java.io.IOException;
import java.util.Random;

public class Performance {
    static final int nr = 10000000;

    public static void main(String[] args) throws DatabaseException, IOException {

        try {
            Database.remove("test/mydb");
        } catch(DatabaseException ignore){}

        Database db = Database.open("test/mydb", true);

        var start = System.currentTimeMillis();
        Transaction tx = db.beginTX("main");
        for (int i = 0; i < nr; i++ ){
            tx.put(String.format("mykey%7d", i).getBytes(), String.format("myvalue", i).getBytes());
            if(i%10000 == 0) {
                tx.commitSync();
                tx = db.beginTX("main");
            }
        }
        tx.commitSync();

        var end = System.currentTimeMillis();
        var duration = end-start;

        System.out.println("insert time " + nr + " records = " + duration + "ms, usec per op "+ (duration*1000.0)/nr);
        start = System.currentTimeMillis();
        db.close();
        end = System.currentTimeMillis();
        duration = end-start;

        System.out.println("close time "+duration+"ms");
        testRead();

        db = Database.open("test/mydb", false);
        start = System.currentTimeMillis();
        db.closeWithMerge(1);
        end = System.currentTimeMillis();
        duration = end-start;

        System.out.println("close with merge 1 time "+duration+"ms");

        testRead();
    }

    private static void testRead() throws IOException, DatabaseException {
        Database db = Database.open("test/mydb", false);
        var start = System.currentTimeMillis();
        Transaction tx = db.beginTX("main");

        LookupIterator itr = tx.lookup(null,null);
        int count = 0;
        while(true) {
            if(itr.next()==null)
                break;
            count++;
        }
        if(count != nr) {
            throw new IllegalStateException("incorrect count != "+ nr + ", count is "+ count);
        }
        var end = System.currentTimeMillis();
        var duration = end-start;

        System.out.println("scan time "+ duration+ "ms, usec per op "+ (duration*1000.0)/nr);

        start = System.currentTimeMillis();
        itr = tx.lookup("mykey 300000".getBytes(),"mykey 799999".getBytes());
        count = 0;
        while(true) {
            if(itr.next()==null)
                break;
            count++;
        }
        if(count != 500000) {
            throw new IllegalStateException("incorrect count != 500000, count is "+ count);
        }
        end = System.currentTimeMillis();
        duration = end-start;

        System.out.println("scan time 50% "+duration+"ms, usec per op "+ (duration*1000.0)/500000);

        start = System.currentTimeMillis();

        testRandom(tx);

        end = System.currentTimeMillis();
        duration = end-start;

        System.out.println("random access time "+(duration*1000.0)/(nr/10.0)+ "us per get");

        tx.rollback();
        db.close();

    }

    private static void testRandom(Transaction tx) throws IOException {

        var r = new Random();

        for(int i = 0; i < nr/10; i++) {
            int index = r.nextInt(nr / 10);
            if(tx.get(String.format("mykey%7d", index).getBytes())==null) {
                throw new IllegalStateException("key not found");
            }
        }
    }

}
