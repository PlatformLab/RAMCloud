/* Copyright (c) 2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package edu.stanford.ramcloud;

import static edu.stanford.ramcloud.ClientException.*;
import edu.stanford.ramcloud.multiop.*;

import java.util.*;

/**
 * A Java RAMCloud client used for testing. Will contain sample code eventually.
 */
public class TestClient {
    private RAMCloud ramcloud;
    private long tableId;

    static {
        // Load C++ shared library for JNI
        System.loadLibrary("edu_stanford_ramcloud_TestClient");
    }

    /**
     * A simple end-to-end test of the java bindings.
     */
    public static void main(String argv[]) {
        new TestClient().go(argv);
    }

    private void go(String[] argv) {
        // Include a pause to add gdb if need to debug c++ code
        boolean debug = false;
        if (debug) {
            System.out.print("Press [Enter] to continue:");
            Scanner scn = new Scanner(System.in);
            scn.nextLine();
        }

        ramcloud = new RAMCloud(argv[0]);
        // System.out.println("Created RamCloud object");
        tableId = ramcloud.createTable("hi");

        // Run whatever here
        // enumerationTest();
        // basicSpeedTest();
        // multiReadTest();
        for (int i = 0; i < 100; i++) {
            multiWriteTest();
        }
        // multiRemoveTest();
        // test();

        ramcloud.dropTable("hi");
        
        ramcloud.disconnect();
    }

    private void multiReadTest() {
        int numTimes = 5000;

        MultiReadObject[] reads = new MultiReadObject[numTimes];
        for (int i = 0; i < numTimes; i++) {
            byte[] key = new byte[30];
            for (int j = 0; j < 4; j++) {
                key[j] = (byte) ((i >> (j * 8)) & 0xFF);
            }
            byte[] value = new byte[100];
            ramcloud.write(tableId, key, value, null);
            // System.out.println("Wrote:" + i);
            reads[i] = new MultiReadObject(tableId, key);
        }
        // System.out.println("filled table");

        long start = System.nanoTime();
        ramcloud.read(reads);
        long time = System.nanoTime() - start;

        System.out.println("Average multiread time per object: " + ((double) time / numTimes / 1000.0));
    }

    private void multiWriteTest() {
        int numTimes = 5000;

        MultiWriteObject[] writes = new MultiWriteObject[numTimes];
        for (int i = 0; i < numTimes; i++) {
            byte[] key = new byte[30];
            for (int j = 0; j < 4; j++) {
                key[j] = (byte) ((i >> (j * 8)) & 0xFF);
            }
            byte[] value = new byte[100];
            writes[i] = new MultiWriteObject(tableId, key, value);
        }
        // System.out.println("filled table");

        long start = System.nanoTime();
        ramcloud.write(writes);
        long time = System.nanoTime() - start;
        
        //System.out.printf("%d,%f\n", limit, ((double) time / numTimes / 1000.0));
        System.out.println("Average multiwrite time per object: " + ((double) time / numTimes / 1000.0));
    }

    private void multiRemoveTest() {
        int numTimes = 5000;

        MultiRemoveObject[] removes = new MultiRemoveObject[numTimes];
        for (int i = 0; i < numTimes; i++) {
            byte[] key = new byte[30];
            for (int j = 0; j < 4; j++) {
                key[j] = (byte) ((i >> (j * 8)) & 0xFF);
            }
            byte[] value = new byte[100];
            removes[i] = new MultiRemoveObject(tableId, key);
        }
        // System.out.println("filled table");

        long start = System.nanoTime();
        ramcloud.remove(removes);
        long time = System.nanoTime() - start;
        //System.out.printf("%d,%f\n", limit, ((double) time / numTimes / 1000.0));
        System.out.println("Average multiremove time per object: " + ((double) time / numTimes / 1000.0));
    }

    private void test(){ 
        int numTimes = 100;
        long before, elapsed;
        before = System.nanoTime();
        for (int i = 0; i < numTimes; i++) {
        }
        elapsed = System.nanoTime() - before;
        System.out.printf("Average create object time: %.3f\n", elapsed / 1000.0 / numTimes);
    }

    private void basicSpeedTest() {
        int numTimes = 100000;
        long before, elapsed;
          
        // Read tests
        byte[] key = new byte[30];
        byte[] value = new byte[100];
        ramcloud.write(tableId, key, value, null);
        double[] times = new double[numTimes];
        System.out.println("time");
        for (int i = 0; i < numTimes; i++) {
            before = System.nanoTime();
            RAMCloudObject unused = ramcloud.read(tableId, key);
            elapsed = System.nanoTime() - before;
            times[i] = elapsed / 1000.0;
            // System.out.printf("%d,%f\n", i, times[i]);
        }
        Arrays.sort(times);
        // System.out.printf("Median Java read time: %.3f\n", times[numTimes / 2]);
          
        ramcloud.remove(tableId, key);
          
        // Write tests
        before = System.nanoTime();
        for (int i = 0; i < numTimes; i++) {
            key[0] = (byte) (Math.random() * 255);
            before = System.nanoTime();
            ramcloud.write(tableId, key, value, null);
            elapsed = System.nanoTime() - before;
            times[i] = elapsed / 1000.0;
            ramcloud.remove(tableId, key);
        }
        Arrays.sort(times);
        // System.out.printf("Median Java write time: %.3f\n", times[numTimes / 2]);
    }

    private void basicTest() {
        // Do basic read/write/table/delete tests
        System.out.println("created table, id = " + tableId);
        long tableId2 = ramcloud.getTableId("hi");
        System.out.println("getTableId says tableId = " + tableId2);

        System.out.println("wrote obj version = "
                + ramcloud.write(tableId, "thisIsTheKey", "thisIsTheValue"));

        RAMCloudObject o = ramcloud.read(tableId, "thisIsTheKey");
        System.out.println("read object: key = [" + o.getKey() + "], value = ["
                + o.getValue() + "], version = " + o.getVersion());
    }

    private void enumerationTest() {
        int numTimes = 1000000;
        // Test Table Enumeration

        MultiWriteObject[] writes = new MultiWriteObject[numTimes];
        for (int i = 0; i < numTimes; i++) {
            writes[i] = new MultiWriteObject(tableId, "" + i, "" + i);
        }
        ramcloud.write(writes);

        // System.out.println("filled table");

        TableIterator it = ramcloud.getTableIterator(tableId);

        // *
        RAMCloudObject current = null;
        int count = 0;
        long start = System.nanoTime();
        while (it.hasNext()) {
            current = it.next();
            count++;
        }
        long time = System.nanoTime() - start;
        System.out.println("Average enumerate time per object: " + ((double) time / numTimes / 1000.0));
        System.out.println(count);
    }

    public static void printBytes(byte[] array) {
        System.out.print("[");
        for (int i = 0; i < array.length - 1; i++) {
            System.out.print(String.format("%02X", (array[i])) + ", ");
        }
        System.out.println(String.format("%02X", array[array.length - 1]) + "]");
    }

    public static native void test(long ramcloudClusterHandle, long arg);

}
