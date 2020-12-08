package com.robaho.jkeydb;

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class DiskSegmentTest extends TestCase {
    public void testDiskSegment() throws IOException {
        File dir = new File("test_db");
        IOUtils.purgeDirectory(dir);
        dir.mkdirs();
        
        var m = new MemorySegment();

        m.put("mykey".getBytes(), "myvalue".getBytes());
        m.put("mykey2".getBytes(), "myvalue2".getBytes());
        m.put("mykey3".getBytes(), "myvalue3".getBytes());

        var itr = m.lookup(null, null);

        var ds = DiskIO.writeAndLoadSegment("test_db/keyfile", "test_db/datafile", itr);

        itr = ds.lookup(null,null);
        int count =0;
        while(true) {
            if(itr.next()==null)
                break;
            count++;
        }
        if (count != 3) {
            fail("incorrect count "+ count);
        }

        var value = ds.get("mykey".getBytes());
        if (!Arrays.equals(value,"myvalue".getBytes())) {
            fail("incorrect values");
        }
        value = ds.get("mykey2".getBytes());
        if (!Arrays.equals(value,"myvalue2".getBytes())) {
            fail("incorrect values");
        }
        value = ds.get("mykey3".getBytes());
        if(!Arrays.equals(value,"myvalue3".getBytes())) {
            fail("incorrect values");
        }
        value = ds.get("mykey4".getBytes());
        if(value!=null) {
            fail("key should not be found");
        }
    }
}
