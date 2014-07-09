package edu.stanford.ramcloud;

import java.util.Iterator;

public class TableIterator implements Iterator<RAMCloudObject>, Iterable<RAMCloudObject> {
    static {
        System.loadLibrary("edu_stanford_ramcloud_TableIterator");
    }
    
    private long tableEnumeratorPointer, tableId;
    
    TableIterator(long ramcloudObjectPointer, long tableId) {
        this.tableId = tableId;
        tableEnumeratorPointer = TableIterator.createTableEnumerator(
            ramcloudObjectPointer,
            tableId);
    }

    private static native long createTableEnumerator(long ramcloudObjectPointer,
                                                     long tableId);
    private static native long getNextObject(long tableEnumeratorPointer,
                                             byte[][] object);
    private static native boolean hasNext(long tableEnumeratorPointer);
    
    @Override
    public boolean hasNext() {
        return TableIterator.hasNext(tableEnumeratorPointer);
    }

    @Override
    public RAMCloudObject next() {
        byte[][] data = new byte[2][];
        long version = TableIterator.getNextObject(tableEnumeratorPointer, data);
        if (version >= 0) {
            RAMCloudObject object = new RAMCloudObject(data[0], data[1], version);
            return object;
        }
        return null;
    }

    @Override
    public void remove() {

    }

    @Override
    public Iterator<RAMCloudObject> iterator() {
        return this;
    }
}
