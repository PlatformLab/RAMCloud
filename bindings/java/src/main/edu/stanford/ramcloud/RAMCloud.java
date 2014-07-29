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

import java.nio.*;

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

    private static final int bufferCapacity = 1024 * 1024 * 2;
    private static final byte[] defaultRejectRules = new byte[12];

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
            return defaultRejectRules;
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
    
    /**
     * Pointer to the underlying C++ RAMCloud object associated with this
     * object.
     */
    private long ramcloudClusterHandle;
    
    /**
     * A native ByteBuffer that acts as a shared memory region between Java and
     * C++. This enables fast passing of arguments and return values for native
     * calls.
     */
    private ByteBuffer byteBuffer;
    
    /**
     * Pointer to the memory location that byteBuffer wraps.
     */
    private long byteBufferPointer;

    /**
     * Reuse existing MultiOpHandler objects to slightly increase performance.
     */
    private MultiReadHandler multiReadHandler;
    private MultiWriteHandler multiWriteHandler;
    private MultiRemoveHandler multiRemoveHandler;

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
        byteBuffer = ByteBuffer.allocateDirect(bufferCapacity);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteBufferPointer = cppGetByteBufferPointer(byteBuffer);
        byteBuffer.putInt(locator.length())
                .put(locator.getBytes())
                .put((byte) 0)
                .put(clusterName.getBytes())
                .put((byte) 0);
        cppConnect(byteBufferPointer);
        byteBuffer.rewind();
        checkStatus(byteBuffer.getInt());
        ramcloudClusterHandle = byteBuffer.getLong();
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
     * Constructor for the unit tests.
     */
    public RAMCloud(long ramcloudClusterHandle) {
        byteBuffer = ByteBuffer.allocateDirect(bufferCapacity);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteBufferPointer = cppGetByteBufferPointer(byteBuffer);
        this.ramcloudClusterHandle = ramcloudClusterHandle;
    }

    /**
     * Disconnect from the RAMCloud cluster. This causes the JNI code to destroy
     * the underlying RAMCloud C++ object.
     */
    public void disconnect() {
        if (ramcloudClusterHandle != 0) {
            cppDisconnect(ramcloudClusterHandle);
            ramcloudClusterHandle = 0;
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
        byteBuffer.rewind();
        // long time = System.nanoTime();
        byteBuffer.putLong(ramcloudClusterHandle)
                .putLong(tableId)
                .putInt(key.length)
                .put(key)
                .put(getRejectRulesBytes(rules));
        // long end = System.nanoTime() - time;
        // System.out.printf("%f\n", ((double) (end) / 1000.0));
        cppRead(byteBufferPointer);
        byteBuffer.rewind();
        ClientException.checkStatus(byteBuffer.getInt());
        long version = byteBuffer.getLong();
        int valueLength = byteBuffer.getInt();
        byte[] value = new byte[valueLength];
        byteBuffer.get(value);
        return new RAMCloudObject(key, value, version);
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
        byteBuffer.rewind();
        byteBuffer.putLong(ramcloudClusterHandle)
                .putLong(tableId)
                .putInt(key.length)
                .put(key)
                .put(getRejectRulesBytes(rules));
        RAMCloud.cppRemove(byteBufferPointer);
        byteBuffer.rewind();
        int status = byteBuffer.getInt();
        checkStatus(status);
        long version = byteBuffer.getLong();
        return version;
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
        byteBuffer.rewind();
        byteBuffer.putLong(ramcloudClusterHandle)
                .putLong(tableId)
                .putInt(key.length)
                .put(key)
                .putInt(value.length)
                .put(value)
                .put(getRejectRulesBytes(rules));
        cppWrite(byteBufferPointer);
        byteBuffer.rewind();
        checkStatus(byteBuffer.getInt());
        long version = byteBuffer.getLong();
        return version;
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
        byteBuffer.rewind();
        byteBuffer.putLong(ramcloudClusterHandle)
                .putInt(serverSpan)
                .put(name.getBytes())
                .put((byte) 0);
        RAMCloud.cppCreateTable(byteBufferPointer);
        byteBuffer.rewind();
        checkStatus(byteBuffer.getInt());
        long tableId = byteBuffer.getLong();
        return tableId;
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
        byteBuffer.rewind();
        byteBuffer.putLong(ramcloudClusterHandle)
                .put(name.getBytes())
                .put((byte) 0);
        RAMCloud.cppDropTable(byteBufferPointer);
        byteBuffer.rewind();
        checkStatus(byteBuffer.getInt());
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
        byteBuffer.rewind();
        byteBuffer.putLong(ramcloudClusterHandle)
                .put(name.getBytes())
                .put((byte) 0);
        RAMCloud.cppGetTableId(byteBufferPointer);
        byteBuffer.rewind();
        checkStatus(byteBuffer.getInt());
        long tableId = byteBuffer.getLong();
        return tableId;
    }

    /**
     * Returns a new TableIterator for the specified table.
     *
     * @param tableId
     *            The ID of the table to enumerate.
     * @return An Iterator that will enumerate the specified table's objects.
     */
    public TableIterator getTableIterator(long tableId) {
        return new TableIterator(this, ramcloudClusterHandle, tableId);
    }

    // Multi-ops
    
    /**
     * Reads a large number of objects at once. Will result in worse performance
     * than a single read if used with very large objects (1 MB).
     *
     * @param request
     *      The array of MultiReadObjects to read. The resulting values will be
     *      stored in the MultiReadObjects, along with the status of each read.
     */
    public void read(MultiReadObject[] request) {
        if (multiReadHandler == null) {
            multiReadHandler = new MultiReadHandler(byteBuffer,
                                                    byteBufferPointer,
                                                    ramcloudClusterHandle);
        }
        multiReadHandler.handle(request);
    }
    
    /**
     * Writes a large number of objects at once.
     *
     * @param data
     *      The array of MultiWriteObjects to write. The resulting versions will
     *      be stored in the MultiWriteObjects, along with the status of each
     *      write.
     */
    public void write(MultiWriteObject[] data) {
        if (multiWriteHandler == null) {
            multiWriteHandler = new MultiWriteHandler(byteBuffer,
                                                      byteBufferPointer,
                                                      ramcloudClusterHandle);
        }
        multiWriteHandler.handle(data);
    }
    
    /**
     * Deletes a large number of objects at once.
     *
     * @param data
     *      The array of MultiRemoveObjects to write. The versions just before
     *      removal will be stored in the MultiRemoveObjects, along with the
     *      status of each remove.
     */
    public void remove(MultiRemoveObject[] data) {
        if (multiRemoveHandler == null) {
            multiRemoveHandler = new MultiRemoveHandler(byteBuffer,
                                                      byteBufferPointer,
                                                      ramcloudClusterHandle);
        }
        multiRemoveHandler.handle(data);
    }

    // Declarations for native methods in c++ file
    private static native long cppGetByteBufferPointer(ByteBuffer byteBuffer);
    
    private static native void cppConnect(long byteBufferPointer);

    private static native void cppDisconnect(long byteBufferPointer);

    private static native void cppCreateTable(long byteBufferPointer);

    private static native void cppDropTable(long byteBufferPointer);

    private static native void cppGetTableId(long byteBufferPointer);

    private static native void cppRead(long byteBufferPointer);

    private static native void cppRemove(long byteBufferPointer);

    private static native void cppWrite(long byteBufferPointer);

    private static native void cppMultiRemove(long ramcloudClusterHandle,
                                              long[] tableIds,
                                              byte[][] objects,
                                              long[] versions,
                                              int[] statuses);
}
