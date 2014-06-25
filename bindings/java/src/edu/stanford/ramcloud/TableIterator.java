package edu.stanford.ramcloud;

import java.util.Iterator;

public class TableIterator implements Iterator<CloudObject> {
    private long tableEnumeratorPointer, tableId;
    
    TableIterator(long ramcloudObjectPointer, long tableId) {
        this.tableId = tableId;
        tableEnumeratorPointer = TableIterator.createTableEnumerator(
            ramcloudObjectPointer,
            tableId);
    }

    private static native long createTableEnumerator(long ramcloudObjectPointer,
                                                     long tableId);

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public CloudObject next() {
        return null;
    }

    @Override
    public void remove() {

    }
}
