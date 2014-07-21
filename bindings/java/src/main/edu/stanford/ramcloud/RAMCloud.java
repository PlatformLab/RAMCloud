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

/**
 * This class provides Java bindings for RAMCloud. Right now it is a rather
 * simple subset of what RamCloud.h defines.
 *
 * Running ``javah'' on this file will generate a C header file with the
 * appropriate JNI function definitions. The glue interfacing to the C++
 * RAMCloud library can be found in RAMCloud.cc.
 *
 * For JNI information, the IBM tutorials and Android developer docs are much
 * better than Sun's at giving an overall intro:
 * http://www.ibm.com/developerworks/java/tutorials/j-jni/section4.html
 * http://developer.android.com/training/articles/perf-jni.html
 *
 */
public class RAMCloud {

    static {
        // Load C++ shared library for JNI
        System.loadLibrary("edu_stanford_ramcloud_RAMCloud");
    }

    /**
     * Returns a byte array representing the given RejectRules value.
     *
     * @param rules
     *            RejectRules object to convert to a byte array.
     * @return A byte array representation of the given RejectRules, or null if
     *         the given RejectRules was null.
     */
    public static byte[] getRejectRulesBytes(RejectRules rules) {
        if (rules == null) {
            return null;
        }
        // 8 bytes for verison number, 1 byte for each condition
        byte[] out = new byte[12];
        long version = rules.getGivenVersion();
        for (int i = 0; i < 8; i++) {
            out[i] = (byte) (version >>> (i * 8));
        }
        out[8] = (byte) (rules.rejectIfDoesntExist() ? 1 : 0);
        out[9] = (byte) (rules.rejectIfExists() ? 1 : 0);
        out[10] = (byte) (rules.rejectIfVersionLeGiven() ? 1 : 0);
        out[11] = (byte) (rules.rejectIfVersionNeGiven() ? 1 : 0);
        return out;
    }

    // TODO: Make this private before commit
    /**
     * Pointer to the underlying C++ RAMCloud object associated with this
     * object.
     */
    public long ramcloudObjectPointer = 0;

    /**
     * Construct a RAMCloud for a particular cluster.
     *
     * @param locator
     *            Describes how to locate the coordinator. It can have either of
     *            two forms. The preferred form is a locator for external
     *            storage that contains the cluster configuration information
     *            (such as a string starting with "zk:", which will be passed to
     *            the ZooStorage constructor). With this form, sessions can
     *            automatically be redirected to a new coordinator if the
     *            current one crashes. Typically the value for this argument
     *            will be the same as the value of the "-x" command-line option
     *            given to the coordinator when it started. The second form is
     *            deprecated, but is retained for testing. In this form, the
     *            location is specified as a RAMCloud service locator for a
     *            specific coordinator. With this form it is not possible to
     *            roll over to a different coordinator if a given one fails; we
     *            will have to wait for the specified coordinator to restart.
     * @param clusterName
     *            Name of the current cluster. Used to allow independent
     *            operation of several clusters sharing many of the same
     *            resources. This is typically the same as the value of the
     *            "--clusterName" command-line option given to the coordinator
     *            when it started.
     */
    public RAMCloud(String locator, String clusterName) {
        int[] status = new int[1];
        ramcloudObjectPointer = cppConnect(locator, clusterName, status);
        checkStatus(status[0]);
    }

    /**
     * Construct a RAMCloud for a particular cluster, with the default cluster
     * name "main".
     *
     * @see #RAMCloud(String, String)
     */
    public RAMCloud(String locator) {
        this(locator, "main");
    }

    /**
     * Constructor for testing.
     */
    public RAMCloud(long ramcloudObjectPointer) {
        this.ramcloudObjectPointer = ramcloudObjectPointer;
    }

    /**
     * Disconnect from the RAMCloud cluster. This causes the JNI code to destroy
     * the underlying RAMCloud C++ object.
     */
    public void disconnect() {
        if (ramcloudObjectPointer != 0) {
            cppDisconnect(ramcloudObjectPointer);
            ramcloudObjectPointer = 0;
        }
    }

    /**
     * This method is called by the garbage collector before destroying the
     * object. The user really should have called disconnect, but in case they
     * did not, be sure to clean up after them.
     */
    @Override
    public void finalize() {
        disconnect();
    }

    /**
     * Read the current contents of an object.
     *
     * @see #read(long, byte[], edu.stanford.ramcloud.RejectRules)
     */
    public RAMCloudObject read(long tableId, String key) {
        return read(tableId, key.getBytes(), null);
    }

    /**
     * Read the current contents of an object.
     *
     * @see #read(long, byte[], edu.stanford.ramcloud.RejectRules)
     */
    public RAMCloudObject read(long tableId, byte[] key) {
        return read(tableId, key, null);
    }

    /**
     * Read the current contents of an object.
     *
     * @see #read(long, byte[], edu.stanford.ramcloud.RejectRules)
     */
    public RAMCloudObject read(long tableId, String key, RejectRules rules) {
        return read(tableId, key.getBytes(), rules);
    }

    /**
     * Read the current contents of an object.
     *
     * @param tableId
     *            The table containing the desired object (return value from a
     *            previous call to getTableId).
     * @param key
     *            Variable length key that uniquely identifies the object within
     *            tableId. It does not necessarily have to be null terminated.
     *            The caller must ensure that the storage for this key is
     *            unchanged through the life of the RPC.
     * @param rules
     *            If non-NULL, specifies conditions under which the read should
     *            be aborted with an error.
     * @return A RAMCloudObject holding the key, value, and version of the read
     *         object.
     */
    public RAMCloudObject read(long tableId, byte[] key, RejectRules rules) {
        byte[] ruleBytes = RAMCloud.getRejectRulesBytes(rules);
        // The version number is stored in a long array because Java can't pass
        // primitives by reference.
        long[] version = new long[1];
        int[] status = new int[1];
        byte[] value = RAMCloud.cppRead(ramcloudObjectPointer, tableId, key,
                ruleBytes, version, status);
        RAMCloudObject obj = new RAMCloudObject(key, value, version[0]);
        checkStatus(status[0]);
        return obj;
    }

    /**
     * Delete an object from a table.
     *
     * @see #remove(long, byte[], edu.stanford.ramcloud.RejectRules)
     */
    public long remove(long tableId, byte[] key) {
        return remove(tableId, key, null);
    }

    /**
     * Delete an object from a table.
     *
     * @see #remove(long, byte[], edu.stanford.ramcloud.RejectRules)
     */
    public long remove(long tableId, String key) {
        return remove(tableId, key.getBytes(), null);
    }

    /**
     * Delete an object from a table.
     *
     * @see #remove(long, byte[], edu.stanford.ramcloud.RejectRules)
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
     *            The table containing the object to be deleted (return value
     *            from a previous call to getTableId).
     * @param key
     *            Variable length key that uniquely identifies the object within
     *            tableId.
     * @param rules
     *            If non-NULL, specifies conditions under which the delete
     *            should be aborted with an error.
     * @return The version number of the object (just before deletion).
     */
    public long remove(long tableId, byte[] key, RejectRules rules) {
        byte[] ruleBytes = RAMCloud.getRejectRulesBytes(rules);
        int[] status = new int[1];
        long out = RAMCloud.cppRemove(ramcloudObjectPointer, tableId, key,
                ruleBytes, status);
        checkStatus(status[0]);
        return out;
    }

    /**
     * Replace the value of a given object, or create a new object if none
     * previously existed.
     *
     * @see #write(long, byte[], byte[], edu.stanford.ramcloud.RejectRules)
     */
    public long write(long tableId, String key, String value) {
        return write(tableId, key.getBytes(), value.getBytes(), null);
    }

    /**
     * Replace the value of a given object, or create a new object if none
     * previously existed.
     *
     * @see #write(long, byte[], byte[], edu.stanford.ramcloud.RejectRules)
     */
    public long write(long tableId, String key, String value, RejectRules rules) {
        return write(tableId, key.getBytes(), value.getBytes(), rules);
    }

    /**
     * Replace the value of a given object, or create a new object if none
     * previously existed.
     *
     * @see #write(long, byte[], byte[], edu.stanford.ramcloud.RejectRules)
     */
    public long write(long tableId, String key, byte[] value) {
        return write(tableId, key.getBytes(), value, null);
    }

    /**
     * Replace the value of a given object, or create a new object if none
     * previously existed.
     *
     * @see #write(long, byte[], byte[], edu.stanford.ramcloud.RejectRules)
     */
    public long write(long tableId, String key, byte[] value, RejectRules rules) {
        return write(tableId, key.getBytes(), value, rules);
    }

    /**
     * Replace the value of a given object, or create a new object if none
     * previously existed.
     *
     * @param tableId
     *            The table containing the desired object (return value from a
     *            previous call to getTableId).
     * @param key
     *            Variable length key that uniquely identifies the object within
     *            tableId.
     * @param value
     *            String providing the new value for the object.
     * @param rules
     *            If non-NULL, specifies conditions under which the write should
     *            be aborted with an error.
     * @return The version number of the object is returned. If the operation
     *         was successful this will be the new version for the object. If
     *         the operation failed then the version number returned is the
     *         current version of the object, or 0 if the object does not exist.
     */
    public long write(long tableId, byte[] key, byte[] value, RejectRules rules) {
        byte[] ruleBytes = RAMCloud.getRejectRulesBytes(rules);
        int[] status = new int[1];
        long out = RAMCloud.cppWrite(ramcloudObjectPointer, tableId, key,
                value, ruleBytes, status);
        checkStatus(status[0]);
        return out;
    }

    /**
     * Create a new table, if it doesn't already exist.
     *
     * @param name
     *            Name for the new table.
     * @param serverSpan
     *            The number of servers across which this table will be divided
     *            (defaults to 1). Keys within the table will be evenly
     *            distributed to this number of servers according to their hash.
     *            This is a temporary work-around until tablet migration is
     *            complete; until then, we must place tablets on servers
     *            statically.
     * @return The return value is an identifier for the created table; this is
     *         used instead of the table's name for most RAMCloud operations
     *         involving the table.
     */
    public long createTable(String name, int serverSpan) {
        int[] status = new int[1];
        long out = RAMCloud.cppCreateTable(ramcloudObjectPointer, name,
                serverSpan, status);
        checkStatus(status[0]);
        return out;
    }

    /**
     * Create a new table, if it doesn't already exist.
     *
     * @param name
     *            Name for the new table.
     * @return The return value is an identifier for the created table; this is
     *         used instead of the table's name for most RAMCloud operations
     *         involving the table.
     */
    public long createTable(String name) {
        return createTable(name, 1);
    }

    /**
     * Delete a table.
     *
     * All objects in the table are implicitly deleted, along with any other
     * information associated with the table. If the table does not currently
     * exist then the operation returns successfully without actually doing
     * anything.
     *
     * @param name
     *            Name of the table to delete.
     */
    public void dropTable(String name) {
        int[] status = new int[1];
        RAMCloud.cppDropTable(ramcloudObjectPointer, name, status);
        checkStatus(status[0]);
    }

    /**
     * Given the name of a table, return the table's unique identifier, which is
     * used to access the table.
     *
     * @param name
     *            Name of the desired table.
     * @return The return value is an identifier for the table; this is used
     *         instead of the table's name for most RAMCloud operations
     *         involving the table.
     */
    public long getTableId(String name) {
        int[] status = new int[1];
        long out = RAMCloud.cppGetTableId(ramcloudObjectPointer, name, status);
        checkStatus(status[0]);
        return out;
    }

    /**
     * Returns a new TableIterator for the specified table.
     *
     * @param tableId
     *            The ID of the table to enumerate.
     * @return An Iterator that will enumerate the specified table's objects.
     */
    public TableIterator getTableIterator(long tableId) {
        return new TableIterator(this, tableId);
    }

    // Documentation for native methods in c++ file
    private static native long cppConnect(String locator, String clusterName,
            int[] status);

    private static native void cppDisconnect(long ramcloudObjectPointer);

    private static native long cppCreateTable(long ramcloudObjectPointer,
            String name, int serverSpan, int[] status);

    private static native void cppDropTable(long ramcloudObjectPointer,
            String name, int[] status);

    private static native long cppGetTableId(long ramcloudObjectPointer,
            String name, int[] status);

    private static native byte[] cppRead(long ramcloudObjectPointer,
            long tableId, byte[] key, byte[] rules, long[] version, int[] status);

    private static native long cppRemove(long ramcloudObjectPointer,
            long tableId, byte[] key, byte[] rules, int[] status);

    private static native long cppWrite(long ramcloudObjectPointer,
            long tableId, byte[] key, byte[] value, byte[] rules, int[] status);
}
