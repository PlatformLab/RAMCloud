package edu.stanford.ramcloud;

import java.util.Iterator;
import java.nio.*;

public class TableIterator implements Iterator<RAMCloudObject> {
    static {
        // Load JNI library
        System.loadLibrary("edu_stanford_ramcloud_TableIterator");
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
     * A blob of bytes returned from the C++ enumerate RPC.
     */
    private ByteBuffer objectBlob;
    /**
     * The last object that was enumerated.
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
    TableIterator(RAMCloud ramcloud, long tableId) {
        this.tableId = tableId;
        this.ramcloud = ramcloud;
        tableEnumeratorPointer = TableIterator.createTableEnumerator(
            ramcloud.ramcloudObjectPointer,
            tableId);
    }

    // Documentation in C++ files
    private static native long createTableEnumerator(long ramcloudObjectPointer,
                                                     long tableId);
    private static native ByteBuffer getNextBatch(long tableEnumeratorPointer,
                                                  int[] status);

    /**
     * Retrieve the next blob of objects from the C++ TableEnumerator object.
     */
    private boolean retrieveBatch() {
        int[] status = new int[1];
        objectBlob = TableIterator.getNextBatch(tableEnumeratorPointer, status);
        ClientException.checkStatus(status);
        if (objectBlob == null) {
            done = true;
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
        if (objectBlob == null) {
            return retrieveBatch();
        }
        return objectBlob.remaining() > 0 || retrieveBatch();
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
        if (done) {
            return null;
        }
        if (objectBlob == null || objectBlob.remaining() <= 0) {
            if (!retrieveBatch()) {
                return null;
            }
        }
        int objectSize = objectBlob.getInt();
        objectBlob.position(objectBlob.position() + 8);
        long version = objectBlob.getLong();
        objectBlob.position(objectBlob.position() + 9);
        short keySize = objectBlob.getShort();
        byte[] key = new byte[keySize];
        objectBlob.get(key);
        byte[] value = new byte[objectSize - (27 + keySize)];
        objectBlob.get(value);

        last = new RAMCloudObject(key, value, version);
        return last;
    }

    // Documentation inherited from java.util.Iterator

    @Override
    public void remove() {
        if (last == null) {
            throw new IllegalStateException();
        }
        ramcloud.remove(tableId, last.getKeyBytes());
        last = null;
    }
}

