/* Copyright (c) 2013 Stanford University
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

import edu.stanford.ramcloud.exception.ConnectionTimeoutException;
import edu.stanford.ramcloud.test.*;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * This class provides Java bindings for RAMCloud. Right now it is a rather
 * simple subset of what RamCloud.h defines.
 *
 * Running ``javah'' on this file will generate a C header file with the
 * appropriate JNI function definitions. The glue interfacing to the C++
 * RAMCloud library can be found in JRamCloud.cc.
 *
 * For JNI information, the IBM tutorials and Android developer docs are much
 * better than Sun's at giving an overall intro:
 *      http://www.ibm.com/developerworks/java/tutorials/j-jni/section4.html
 *      http://developer.android.com/training/articles/perf-jni.html
 *
 */
public class JRamCloud {
    static {
        // Load C++ shared library for JNI
        System.loadLibrary("edu_stanford_ramcloud_JRamCloud");
    }

    /**
     * Returns a byte array representing the given RejectRules value.
     *
     * @param rules
     *      RejectRules object to convert to a byte array.
     * @return A byte array representation of the given RejectRules, or null if
     *      the given RejectRules was null.
     */
    public static byte[] getRejectRulesBytes(RejectRules rules) {
        if (rules == null) {
            return null;
        }
        // 8 bytes for verison number, 1 byte for each condition
        byte[] out = new byte[12];
        long version = rules.getGivenVersion();
        for (int i = 0; i < 8; i++) {
            out[i] = (byte)(version >>> (i * 8));
        }
        out[8] = (byte) (rules.doesntExist() ? 1 : 0);
        out[9] = (byte) (rules.exists() ? 1 : 0);
        out[10] = (byte) (rules.versionLeGiven() ? 1 : 0);
        out[11] = (byte) (rules.versionNeGiven() ? 1 : 0);
        return out;
    }

    /**
     * Pointer to the underlying C++ RAMCloud object associated with this object.
     */
    private long ramcloudObjectPointer = 0;

    /**
     * Connect to the RAMCloud cluster specified by the given coordinator's
     * service locator string. This causes the JNI code to instantiate the
     * underlying RamCloud C++ object.
     * 
     * @param coordinatorLocator
     *     String that identifies the coordinator server
     */
    public JRamCloud(String coordinatorLocator) {
        ramcloudObjectPointer = connect(coordinatorLocator);
    }

    /**
     * Constructor for testing.
     */
    public JRamCloud(long ramcloudObjectPointer) {
        this.ramcloudObjectPointer = ramcloudObjectPointer;
    }

    /**
     * Disconnect from the RAMCloud cluster. This causes the JNI code to destroy
     * the underlying RamCloud C++ object.
     */
    public void disconnect() {
        if (ramcloudObjectPointer != 0) {
            disconnect(ramcloudObjectPointer);
            ramcloudObjectPointer = 0;
        }
    }

    /**
     * This method is called by the garbage collector before destroying the
     * object. The user really should have called disconnect, but in case they
     * did not, be sure to clean up after them.
     */
    public void finalize() {
        System.err.println("warning: JRamCloud::disconnect() was not called "
                + "prior to the finalizer. You should disconnect "
                + "your JRamCloud object when you're done with it.");
        disconnect();
    }

    /**
     * Read the current contents of an object.
     *
     * @param tableId
     *      The table containing the desired object (return value from
     *      a previous call to getTableId).
     * @param key
     *      Variable length key that uniquely identifies the object within tableId.
     *      It does not necessarily have to be null terminated.  The caller must
     *      ensure that the storage for this key is unchanged through the life of
     *      the RPC.
     * @return A CloudObject holding the key, value, and version of
     *      the read object.
     */
    public CloudObject read(long tableId, String key) {
        return read(tableId, key.getBytes(), null);
    }

    /**
     * Read the current contents of an object.
     *
     * @param tableId
     *      The table containing the desired object (return value from
     *      a previous call to getTableId).
     * @param key
     *      Variable length key that uniquely identifies the object within tableId.
     *      It does not necessarily have to be null terminated.  The caller must
     *      ensure that the storage for this key is unchanged through the life of
     *      the RPC.
     * @return A CloudObject holding the key, value, and version of
     *      the read object.
     */
    public CloudObject read(long tableId, byte[] key) {
        return read(tableId, key, null);
    }

    /**
     * Read the current contents of an object.
     *
     * @param tableId
     *      The table containing the desired object (return value from
     *      a previous call to getTableId).
     * @param key
     *      Variable length key that uniquely identifies the object within tableId.
     *      It does not necessarily have to be null terminated.  The caller must
     *      ensure that the storage for this key is unchanged through the life of
     *      the RPC.
     * @param rejectRules
     *      If non-NULL, specifies conditions under which the read
     *      should be aborted with an error.
     * @return A CloudObject holding the key, value, and version of
     *      the read object.
     */
    public CloudObject read(long tableId, String key, RejectRules rules) {
        return read(tableId, key.getBytes(), rules);
    }

    /**
     * Read the current contents of an object.
     *
     * @param tableId
     *      The table containing the desired object (return value from a previous
     *      call to getTableId).
     * @param key
     *      Variable length key that uniquely identifies the object within
     *      tableId. It does not necessarily have to be null terminated. The
     *      caller must ensure that the storage for this key is unchanged through
     *      the life of the RPC.
     * @param rejectRules
     *      If non-NULL, specifies conditions under which the read should be
     *      aborted with an error.
     * @return A CloudObject holding the key, value, and version of the read object.
     */
    public CloudObject read(long tableId, byte[] key, RejectRules rules) {
        byte[] ruleBytes = JRamCloud.getRejectRulesBytes(rules);
        // The version number is stored in a long array because Java can't pass
        // primitives by reference.
        long[] version = new long[1];
        byte[] value = JRamCloud._read(ramcloudObjectPointer, tableId, key, ruleBytes, version);
        CloudObject obj = new CloudObject(key, value, version[0]);
        return obj;
    }
    
    /**
     * Delete an object from a table. If the object does not currently exist
     * then the operation succeeds without doing anything (unless rejectRules
     * causes the operation to be aborted).
     *
     * @param tableId
     *      The table containing the object to be deleted (return value from
     *      a previous call to getTableId).
     * @param key
     *      Variable length key that uniquely identifies the object within tableId.
     *      It does not necessarily have to be null terminated.  The caller must
     *      ensure that the storage for this key is unchanged through the life of
     *      the RPC.
     * @return The version number of the object (just before
     *      deletion).
     */
    public long remove(long tableId, byte[] key) {
        return remove(tableId, key, null);
    }
    
    /**
     * Delete an object from a table. If the object does not currently exist
     * then the operation succeeds without doing anything (unless rejectRules
     * causes the operation to be aborted).
     *
     * @param tableId
     *      The table containing the object to be deleted (return value from
     *      a previous call to getTableId).
     * @param key
     *      Variable length key that uniquely identifies the object within tableId.
     *      It does not necessarily have to be null terminated.  The caller must
     *      ensure that the storage for this key is unchanged through the life of
     *      the RPC.
     * @return The version number of the object (just before
     *      deletion).
     */
    public long remove(long tableId, String key) {
        return remove(tableId, key.getBytes(), null);
    }
    
    /**
     * Delete an object from a table. If the object does not currently exist
     * then the operation succeeds without doing anything (unless rejectRules
     * causes the operation to be aborted).
     *
     * @param tableId
     *      The table containing the object to be deleted (return value from
     *      a previous call to getTableId).
     * @param key
     *      Variable length key that uniquely identifies the object within tableId.
     *      It does not necessarily have to be null terminated.  The caller must
     *      ensure that the storage for this key is unchanged through the life of
     *      the RPC.
     * @param rejectRules
     *      If non-NULL, specifies conditions under which the delete
     *      should be aborted with an error.
     * @return The version number of the object (just before
     *      deletion).
     */
    public long remove(long tableId, String key, RejectRules rules) {
        return remove(tableId, key.getBytes(), rules);
    }
    
    /**
     * Delete an object from a table. If the object does not currently exist
     * then the operation succeeds without doing anything (unless rejectRules
     * causes the operation to be aborted).
     *
     * @param tableId
     *      The table containing the object to be deleted (return value from
     *      a previous call to getTableId).
     * @param key
     *      Variable length key that uniquely identifies the object within tableId.
     *      It does not necessarily have to be null terminated.  The caller must
     *      ensure that the storage for this key is unchanged through the life of
     *      the RPC.
     * @param rejectRules
     *      If non-NULL, specifies conditions under which the delete
     *      should be aborted with an error.
     * @return The version number of the object (just before
     *      deletion).
     */
    public long remove(long tableId, byte[] key, RejectRules rules) {
        byte[] ruleBytes = JRamCloud.getRejectRulesBytes(rules);
        return JRamCloud._remove(ramcloudObjectPointer, tableId, key, ruleBytes);
    }

    /**
     * Replace the value of a given object, or create a new object if none
     * previously existed.
     *
     * @param tableId
     *      The table containing the desired object (return value from
     *      a previous call to getTableId).
     * @param key
     *      Variable length key that uniquely identifies the object within tableId.
     *      It does not necessarily have to be null terminated.  The caller must
     *      ensure that the storage for this key is unchanged through the life of
     *      the RPC.
     * @param value
     *      NULL-terminated string providing the new value for the object (the
     *      terminating NULL character will not be part of the object).
     * @return The version number of the object is returned.
     *      If the operation was successful this will be the new version for
     *      the object. If the operation failed then the version number returned
     *      is the current version of the object, or 0 if the object does not
     *      exist.
     */
    public long write(long tableId, String key, String value) {
        return write(tableId, key.getBytes(), value.getBytes(), null);
    }

    /**
     * Replace the value of a given object, or create a new object if none
     * previously existed.
     *
     * @param tableId
     *      The table containing the desired object (return value from
     *      a previous call to getTableId).
     * @param key
     *      Variable length key that uniquely identifies the object within tableId.
     *      It does not necessarily have to be null terminated.  The caller must
     *      ensure that the storage for this key is unchanged through the life of
     *      the RPC.
     * @param value
     *      NULL-terminated string providing the new value for the object (the
     *      terminating NULL character will not be part of the object).
     * @return The version number of the object is returned.
     *      If the operation was successful this will be the new version for
     *      the object. If the operation failed then the version number returned
     *      is the current version of the object, or 0 if the object does not
     *      exist.
     */
    public long write(long tableId, String key, String value, RejectRules rules) {
        return write(tableId, key.getBytes(), value.getBytes(), rules);
    }

    /**
     * Replace the value of a given object, or create a new object if none
     * previously existed.
     *
     * @param tableId
     *      The table containing the desired object (return value from
     *      a previous call to getTableId).
     * @param key
     *      Variable length key that uniquely identifies the object within tableId.
     *      It does not necessarily have to be null terminated.  The caller must
     *      ensure that the storage for this key is unchanged through the life of
     *      the RPC.
     * @param value
     *      NULL-terminated string providing the new value for the object (the
     *      terminating NULL character will not be part of the object).
     * @return The version number of the object is returned.
     *      If the operation was successful this will be the new version for
     *      the object. If the operation failed then the version number returned
     *      is the current version of the object, or 0 if the object does not
     *      exist.
     */
    public long write(long tableId, String key, byte[] value) {
        return write(tableId, key.getBytes(), value, null);
    }

    /**
     * Replace the value of a given object, or create a new object if none
     * previously existed.
     *
     * @param tableId
     *      The table containing the desired object (return value from
     *      a previous call to getTableId).
     * @param key
     *      Variable length key that uniquely identifies the object within tableId.
     *      It does not necessarily have to be null terminated.  The caller must
     *      ensure that the storage for this key is unchanged through the life of
     *      the RPC.
     * @param value
     *      NULL-terminated string providing the new value for the object (the
     *      terminating NULL character will not be part of the object).
     * @param rejectRules
     *      If non-NULL, specifies conditions under which the write
     *      should be aborted with an error.
     * @return The version number of the object is returned.
     *      If the operation was successful this will be the new version for
     *      the object. If the operation failed then the version number returned
     *      is the current version of the object, or 0 if the object does not
     *      exist.
     */
    public long write(long tableId, String key, byte[] value, RejectRules rules) {
        return write(tableId, key.getBytes(), value, rules);
    }

    /**
     * Replace the value of a given object, or create a new object if none
     * previously existed.
     *
     * @param tableId
     *      The table containing the desired object (return value from
     *      a previous call to getTableId).
     * @param key
     *      Variable length key that uniquely identifies the object within tableId.
     *      It does not necessarily have to be null terminated.  The caller must
     *      ensure that the storage for this key is unchanged through the life of
     *      the RPC.
     * @param value
     *      NULL-terminated string providing the new value for the object (the
     *      terminating NULL character will not be part of the object).
     * @param rejectRules
     *      If non-NULL, specifies conditions under which the write
     *      should be aborted with an error.
     * @return The version number of the object is returned.
     *      If the operation was successful this will be the new version for
     *      the object. If the operation failed then the version number returned
     *      is the current version of the object, or 0 if the object does not
     *      exist.
     */
    public long write(long tableId, byte[] key, byte[] value, RejectRules rules) {
        byte[] ruleBytes = JRamCloud.getRejectRulesBytes(rules);
        return JRamCloud._write(ramcloudObjectPointer, tableId, key, value, ruleBytes);
    }
    
    /**
     * Create a new table.
     *
     * @param name
     *      Name for the new table.
     * @param serverSpan
     *      The number of servers across which this table will be divided
     *      (defaults to 1). Keys within the table will be evenly distributed
     *      to this number of servers according to their hash. This is a temporary
     *      work-around until tablet migration is complete; until then, we must
     *      place tablets on servers statically.
     * @return
     *      The return value is an identifier for the created table; this is
     *      used instead of the table's name for most RAMCloud operations
     *      involving the table.
     */
    public long createTable(String name, int serverSpan) {
        return JRamCloud._createTable(ramcloudObjectPointer, name, serverSpan);
    }
    
    /**
     * Create a new table.
     *
     * @param name
     *      Name for the new table.
     * @return
     *      The return value is an identifier for the created table; this is
     *      used instead of the table's name for most RAMCloud operations
     *      involving the table.
     */
    public long createTable(String name) {
        return createTable(name, 1);
    }
    
    /**
     * Delete a table.
     *
     * All objects in the table are implicitly deleted, along with any
     * other information associated with the table.  If the table does
     * not currently exist then the operation returns successfully without
     * actually doing anything.
     *
     * @param name
     *      Name of the table to delete.
     */
    public void dropTable(String name) {
        JRamCloud._dropTable(ramcloudObjectPointer, name);
    }
    
    /**
     * Given the name of a table, return the table's unique identifier, which
     * is used to access the table.
     *
     * @param name
     *      Name of the desired table.
     * @return
     *      The return value is an identifier for the table; this is used
     *      instead of the table's name for most RAMCloud operations
     *      involving the table.
     */
    public long getTableId(String name) {
        return JRamCloud._getTableId(ramcloudObjectPointer, name);
    }

    /**
     * #WIP: - Do not use yet
     */
    public TableIterator getTableIterator(long tableId) {
        return new TableIterator(ramcloudObjectPointer, tableId);
    }

    /**
     * Connect to the RAMCloud cluster specified by the given coordinator's
     * service locator string. This causes the JNI code to instantiate the
     * underlying RamCloud C++ object.
     * 
     * @param coordinatorLocator
     *     String that identifies the coordinator server
     */
    private static native long connect(String coordinatorLocator);

    /**
     * Disconnect from the RAMCloud cluster. This causes the JNI code to destroy
     * the underlying RamCloud C++ object.
     * 
     * @param ramcloudObjectPointer
     *      A pointer to the C++ RamCloud object.
     */
    private static native void disconnect(long ramcloudObjectPointer);

    /**
     * Create a new table.
     * 
     * @param ramcloudObjectPointer
     *      A pointer to the C++ RamCloud object.
     * @param name
     *      Name for the new table.
     * @param serverSpan
     *      The number of servers across which this table will be divided
     *      (defaults to 1). Keys within the table will be evenly distributed
     *      to this number of servers according to their hash. This is a temporary
     *      work-around until tablet migration is complete; until then, we must
     *      place tablets on servers statically.
     * @return
     *      The return value is an identifier for the created table; this is
     *      used instead of the table's name for most RAMCloud operations
     *      involving the table.
     */
    private static native long _createTable(long ramcloudObjectPointer,
                                            String name, int serverSpan);

    /**
     * Delete a table.
     *
     * All objects in the table are implicitly deleted, along with any
     * other information associated with the table.  If the table does
     * not currently exist then the operation returns successfully without
     * actually doing anything.
     *
     * @param ramcloudObjectPointer
     *      A pointer to the C++ RamCloud object.
     * @param name
     *      Name of the table to delete.
     */
    private static native void _dropTable(long ramcloudObjectPointer,
                                          String name);

    /**
     * Given the name of a table, return the table's unique identifier, which
     * is used to access the table.
     *
     * @param ramcloudObjectPointer
     *      A pointer to the C++ RamCloud object.
     * @param name
     *      Name of the desired table.
     * @return
     *      The return value is an identifier for the table; this is used
     *      instead of the table's name for most RAMCloud operations
     *      involving the table.
     */
    private static native long _getTableId(long ramcloudObjectPointer,
                                           String name);

    /**
     * Read the current contents of an object.
     *
     * @param ramcloudObjectPointer
     *      A pointer to the C++ RamCloud object
     * @param tableId
     *      The table containing the desired object (return value from a
     *      previous call to getTableId).
     * @param key
     *      Variable length key that uniquely identifies the object within
     *      tableId. It does not necessarily have to be null terminated. The
     *      caller must ensure that the storage for this key is unchanged
     *      through the life of the RPC.
     * @param rejectRules
     *      If non-NULL, specifies conditions under which the read should be
     *      aborted with an error.
     * @param version
     *      A long array with a single value that will hold the version of the
     *      read object.
     * @return A byte array holding the value of the read object
     */
    private static native byte[] _read(long ramcloudObjectPointer,
                                       long tableId, byte[] key,
                                       byte[] rules,
                                       long[] version);
    
    /**
     * Delete an object from a table. If the object does not currently exist
     * then the operation succeeds without doing anything (unless rejectRules
     * causes the operation to be aborted).
     *
     * @param ramcloudObjectPointer
     *      A pointer to the C++ RamCloud object
     * @param tableId
     *      The table containing the object to be deleted (return value from
     *      a previous call to getTableId).
     * @param key
     *      Variable length key that uniquely identifies the object within tableId.
     *      It does not necessarily have to be null terminated.  The caller must
     *      ensure that the storage for this key is unchanged through the life of
     *      the RPC.
     * @param rejectRules
     *      If non-NULL, specifies conditions under which the remove should be
     *      aborted with an error.
     * @return The version number of the object (just before
     *      deletion).
     */
    private static native long _remove(long ramcloudObjectPointer, long tableId,
                                       byte[] key, byte[] rules);

    /**
     * Replace the value of a given object, or create a new object if none
     * previously existed.
     *
     * @param ramcloudObjectPointer
     *      A pointer to the C++ RamCloud object
     * @param tableId
     *      The ID of the table to write to (return value from a previous call
     *      to getTableId).
     * @param key
     *      Variable length key that uniquely identifies the object within tableId.
     *      It does not necessarily have to be null terminated.  The caller must
     *      ensure that the storage for this key is unchanged through the life of
     *      the RPC.
     * @param value
     *      NULL-terminated string providing the new value for the object (the
     *      terminating NULL character will not be part of the object).
     * @param rejectRules
     *      If non-NULL, specifies conditions under which the write
     *      should be aborted with an error.
     * @return The version number of the object is returned.
     *      If the operation was successful this will be the new version for
     *      the object. If the operation failed then the version number returned
     *      is the current version of the object, or 0 if the object does not
     *      exist.
     */
    private static native long _write(long ramcloudObjectPointer, long tableId,
                                      byte[] key, byte[] value,
                                      byte[] rules);

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

        int numTimes = 100000;
        // Empty call test
        /*
        long testBefore = System.nanoTime();
        for (int i = 0; i < numTimes; i++) {
            test(100L, new byte[] {0, 2, 3}, null, 0.254);
        }
        long testAfter = System.nanoTime();
        System.out.println("Test time: " +
                           ((double) (testAfter - testBefore) / numTimes /
                            1000) + " usec");
        if (!debug) {
            return;
        }//*/
        //*
        // Do basic read/write/table/delete tests
        JRamCloud ramcloud = new JRamCloud(argv[0]);
        System.out.println("Created RamCloud object");
        long tableId = ramcloud.createTable("hi");

        //*
        System.out.println("created table, id = " + tableId);
        long tableId2 = ramcloud.getTableId("hi");
        System.out.println("getTableId says tableId = " + tableId2);

        System.out.println("wrote obj version = "
                + ramcloud.write(tableId, "thisIsTheKey", "thisIsTheValue"));

        CloudObject o = ramcloud.read(tableId, "thisIsTheKey");
        System.out.println("read object: key = [" + o.getKey() + "], value = ["
                + o.getValue() + "], version = " + o.getVersion());

        // Do rejectRules test
        RejectRules rejectRules = new RejectRules();
        rejectRules.setGivenVersion(o.getVersion() + 1);
        rejectRules.setVersionNeGiven(true);
        ///

        //*
        try {
            ramcloud.read(tableId, "thisIsTheKey", rejectRules);
            System.out
                    .println("Error: RejectRules Read should not have read properly");
        } catch (Exception e) {
            // OK
        } //

        ramcloud.remove(tableId, "thisIsTheKey");

        try {
            ramcloud.read(tableId, "thisIsTheKey");
            System.out.println("Error: shouldn't have read successfully!");
        } catch (Exception e) {
            // OK
        } //*/

        //*

        long before, elapsed;
        
        // Read tests
        byte[] key = new byte[30];
        byte[] value = new byte[100];
        ramcloud.write(tableId, key, value, null);
        for (int i = 0; i < 1000; i++) {
            ramcloud.read(tableId, key);
        }
        double[] times = new double[numTimes];
        for (int i = 0; i < numTimes; i++) {
            before = System.nanoTime();
            CloudObject unused = ramcloud.read(tableId, key);
            elapsed = System.nanoTime() - before;
            times[i] = elapsed / 1000.0;
        }
        Arrays.sort(times);
        System.out.printf("Median Java read time: %.3f\n", times[numTimes/2]);
        
        ramcloud.remove(tableId, key);

        // Write tests
        before = System.nanoTime();
        for (int i = 0; i < numTimes; i++) {
            key[0] = (byte)(Math.random() * 255);
            before = System.nanoTime();
            ramcloud.write(tableId, key, value, null);
            elapsed = System.nanoTime() - before;
            times[i] = elapsed / 1000.0;
            ramcloud.remove(tableId, key);
        }
        Arrays.sort(times);
        System.out.printf("Median Java write time: %.3f\n", times[numTimes/2]);
        //*/

        ramcloud.disconnect();

        TestCluster cluster = new TestCluster();
        cluster.destroy();
    }
}
