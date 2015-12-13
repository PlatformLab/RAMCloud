package edu.stanford.ramcloud;

import java.util.Iterator;
import java.nio.*;

/**
 * This class provides the client-side interface for table enumeration;
 * each instance of this class can be used to enumerate the objects in
 * a single table. A new instance must be created each time the client
 * wants to restart the enumeration.
 */
public class TableIterator implements Iterator<RAMCloudObject> {
    static {
        // Load JNI library
        Util.loadLibrary("ramcloud_java");
    }

    /**
     * Keep a pointer to the C++ TableEnumerator object.
     */
    private long tableEnumeratorPointer;

    /**
     * Table ID that this object is enumerating.
     */
    private long tableId;

    /**
     * RAMCloud object that this object was created from.
     */
    private RAMCloud ramcloud;

    /**
     * A blob of bytes returned from the C++ enumerate RPC. Each blob should
     * contain a set of objects read from a RAMCloud table.
     */
    private ByteBuffer objectBlob;

    /**
     * The last object that was returned from a call to next().
     */
    private RAMCloudObject last;

    /**
     * Whether or not the enumerator is done with the table.
     */
    private boolean done;

    /**
     * Creates a TableIterator for the given table. Meant only to be called
     * internally in the Java bindings.
     */
    TableIterator(RAMCloud ramcloud, long ramcloudClusterHandle, long tableId) {
        this.tableId = tableId;
        this.ramcloud = ramcloud;
        tableEnumeratorPointer = TableIterator.createTableEnumerator(
                ramcloudClusterHandle,
                tableId);
    }

    /**
     * Retrieve the next blob of objects from the C++ TableEnumerator object.
     *
     * @return True if there are objects still to be enumerated, false
     *         otherwise.
     */
    private boolean retrieveBatch() {
        if (done) {
            return false;
        }
        if (objectBlob != null && objectBlob.remaining() > 0) {
            return true;
        }
        int[] status = new int[1];
        objectBlob = TableIterator.getNextBatch(tableEnumeratorPointer, status);
        ClientException.checkStatus(status[0]);
        if (objectBlob == null) {
            done = true;
            // Since the C++ enumerator will never be used again, delete it now.
            delete(tableEnumeratorPointer);
            tableEnumeratorPointer = -1;
            return false;
        }
        // Make sure this is in the right byte order.
        objectBlob.order(ByteOrder.LITTLE_ENDIAN);
        return true;
    }

    /**
     * Get the ID of the table that this Iterator is enumerating.
     *
     * @return The ID of the table being enumerated.
     */
    public long getTableId() {
        return tableId;
    }

    /**
     * Test if any objects remain to be enumerated from the table.
     *
     * @return
     *      True if any objects remain, or false otherwise.
     */
    @Override
    public boolean hasNext() {
        return retrieveBatch();
    }

    /**
     * Return the next object in the table.  Note: each object that existed
     * throughout the entire lifetime of the enumeration is guaranteed to
     * be returned exactly once.  Objects that are created after the enumeration
     * starts, or that are deleted before the enumeration completes, will be
     * returned either 0 or 1 time.
     *
     * @return The next object in the table. Will be null if enumeration is
     *         complete.
     */
    @Override
    public RAMCloudObject next() {
        if (!retrieveBatch()) {
            return null;
        }
        // This code depends on the format of the C++ Object class, defined in
        // Object.h. If that changes, this will need to change as well.
        int objectSize = objectBlob.getInt();
        // Skip checksum and timestamp
        objectBlob.position(objectBlob.position() + 8);
        long version = objectBlob.getLong();
        // Skip table ID and number of indeces
        objectBlob.position(objectBlob.position() + 9);
        short keySize = objectBlob.getShort();
        byte[] key = new byte[keySize];
        objectBlob.get(key);
        // Remaining bytes are value
        byte[] value = new byte[objectSize - (27 + keySize)];
        objectBlob.get(value);

        last = new RAMCloudObject(key, value, version);
        return last;
    }

    /**
     * Removes the object last returned from a call to getNext().
     */
    @Override
    public void remove() {
        if (last == null) {
            throw new IllegalStateException();
        }
        ramcloud.remove(tableId, last.getKeyBytes());
        last = null;
    }

    /**
     * This method is called when this object is being garbage collected. If the
     * iterator never iterated through the entire table, then clean up the C++
     * resources now.
     */
    @Override
    public void finalize() {
        if (tableEnumeratorPointer != -1) {
            delete(tableEnumeratorPointer);
        }
    }

    // Documentation in C++ files
    private static native long createTableEnumerator(long ramcloudClusterHandle,
                                                     long tableId);
    private static native ByteBuffer getNextBatch(long tableEnumeratorPointer,
                                                  int[] status);
    private static native void delete(long tableEnumeratorPointer);
}

