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

import java.util.*;

/**
 * A Java RAMCloud client used for testing. Will contain sample code eventually.
 */
public class TestClient {

    static {
        // Load C++ shared library for JNI
        System.loadLibrary("edu_stanford_ramcloud_TestClient");
    }

    /**
     * A simple end-to-end test of the java bindings.
     */
    public static void main(String argv[]) {
        // Include a pause to add gdb if need to debug c++ code
        boolean debug = false;
        if (debug) {
            Scanner scn = new Scanner(System.in);
            scn.nextLine();
        }

        int numTimes = 1000000;
        // Empty call test
        // *
        // Do basic read/write/table/delete tests
        RAMCloud ramcloud = new RAMCloud(argv[0]);
        // System.out.println("Created RamCloud object");
        long tableId = ramcloud.createTable("hi");
        /*
         * for (int i = 0; i < numTimes; i++) { test(Integer.toString(i)); }
         * //test("485361", "85034"); if (!debug) { return; }//
         */

        // *
        System.out.println("created table, id = " + tableId);
        long tableId2 = ramcloud.getTableId("hi");
        System.out.println("getTableId says tableId = " + tableId2);

        System.out.println("wrote obj version = "
                + ramcloud.write(tableId, "thisIsTheKey", "thisIsTheValue"));

        RAMCloudObject o = ramcloud.read(tableId, "thisIsTheKey");
        System.out.println("read object: key = [" + o.getKey() + "], value = ["
                + o.getValue() + "], version = " + o.getVersion());
        // */
        HashSet<String> keys = new HashSet<String>();
        // Test Table Enumeration
        // *
        for (int i = 0; i < numTimes; i++) {
            ramcloud.write(tableId, "" + i, "" + i);
            keys.add("" + i);
        }

        System.out.println("filled table");

        TableIterator it = new TableIterator(ramcloud.ramcloudObjectPointer,
                tableId);
        // *
        RAMCloudObject current = null;
        long start = System.nanoTime();
        int i = 0;
        while ((current = it.next()) != null) {
            if (!keys.remove(current.getKey())) {
                System.out.println("Duplicate key: " + current);
            }
            i++;
        }
        for (String s : keys) {
            System.out.println("Missing key: " + s);
        }
        long time = System.nanoTime() - start;
        System.out.println(i);
        System.out.println((double) time / numTimes / 1000.0);

        ramcloud.dropTable("hi");

        //

        /*
         * // Do rejectRules test RejectRules rejectRules = new RejectRules();
         * rejectRules.setGivenVersion(o.getVersion() + 1);
         * rejectRules.rejectIfVersionNeGiven(true); // try {
         * ramcloud.read(tableId, "thisIsTheKey", rejectRules); System.out
         * .println("Error: RejectRules Read should not have read properly"); }
         * catch (Exception e) { // OK } //
         * 
         * ramcloud.remove(tableId, "thisIsTheKey");
         * 
         * try { ramcloud.read(tableId, "thisIsTheKey");
         * System.out.println("Error: shouldn't have read successfully!"); }
         * catch (Exception e) { // OK } ///
         * 
         * /* long before, elapsed;
         * 
         * // Read tests byte[] key = new byte[30]; byte[] value = new
         * byte[100]; ramcloud.write(tableId, key, value, null); for (int i = 0;
         * i < 1000; i++) { ramcloud.read(tableId, key); } double[] times = new
         * double[numTimes]; for (int i = 0; i < numTimes; i++) { before =
         * System.nanoTime(); RAMCloudObject unused = ramcloud.read(tableId,
         * key); elapsed = System.nanoTime() - before; times[i] = elapsed /
         * 1000.0; } Arrays.sort(times);
         * System.out.printf("Median Java read time: %.3f\n", times[numTimes /
         * 2]);
         * 
         * ramcloud.remove(tableId, key);
         * 
         * // Write tests before = System.nanoTime(); for (int i = 0; i <
         * numTimes; i++) { key[0] = (byte) (Math.random() * 255); before =
         * System.nanoTime(); ramcloud.write(tableId, key, value, null); elapsed
         * = System.nanoTime() - before; times[i] = elapsed / 1000.0;
         * ramcloud.remove(tableId, key); } Arrays.sort(times);
         * System.out.printf("Median Java write time: %.3f\n", times[numTimes /
         * 2]); //
         */

        ramcloud.disconnect();
    }

    public static void printBytes(byte[] array) {
        System.out.print("[");
        for (int i = 0; i < array.length - 1; i++) {
            System.out.print(array[i] + ", ");
        }
        System.out.println(array[array.length - 1] + "]");
    }

    public static native void test(String key1);

}
