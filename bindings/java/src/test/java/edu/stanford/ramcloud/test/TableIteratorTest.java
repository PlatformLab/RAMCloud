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

package edu.stanford.ramcloud.test;

import java.lang.reflect.Method;
import java.util.*;

import static edu.stanford.ramcloud.ClientException.*;
import edu.stanford.ramcloud.*;
import static edu.stanford.ramcloud.test.ClientTestClusterSetup.*;

import org.testng.annotations.*;
import static org.testng.AssertJUnit.*;
import static org.testng.Reporter.*;

/**
 * Unit tests for TableIterator class.
 */
public class TableIteratorTest {
    private long tableId;

    @BeforeClass
    public void TableIteratorTestSetup() {
        tableId = ramcloud.createTable("tableIteratorTest");
    }

    @Test
    public void getTableId() {
        TableIterator iterator = ramcloud.getTableIterator(tableId);
        assertEquals(tableId, iterator.getTableId());
    }

    @Test
    public void hasNext() {
        int total = 100000;
        byte[] value = new byte[100];
        for (int i = 0; i < total; i++) {
            ramcloud.write(tableId, i + "", value);
        }

        TableIterator iterator = ramcloud.getTableIterator(tableId);
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertEquals(total, count);

        for (int i = 0; i < total; i++) {
            ramcloud.remove(tableId, i + "");
        }
    }

    @Test
    public void hasNext_singleObject() {
        ramcloud.write(tableId, "1", "1");
        TableIterator iterator = ramcloud.getTableIterator(tableId);
        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
        ramcloud.remove(tableId, "1");
    }

    @Test
    public void hasNext_empty() {
        TableIterator iterator = ramcloud.getTableIterator(tableId);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void next() {
        int total = 100000;
        HashSet<String> keys = new HashSet<String>();
        for (int i = 0; i < total; i++) {
            String key = "key:" + i;
            ramcloud.write(tableId, key, "value:" + i);
            keys.add(key);
        }

        TableIterator iterator = ramcloud.getTableIterator(tableId);
        RAMCloudObject current = null;
        while ((current = iterator.next()) != null) {
            String expectedValue = "value:" + current.getKey().substring(4);
            assertEquals(expectedValue, current.getValue());
            assertTrue(keys.remove(current.getKey()));
        }
        assertTrue(keys.isEmpty());

        for (int i = 0; i < total; i++) {
            ramcloud.remove(tableId, "value:" + i);
        }
    }

    @Test
    public void remove() {
        int total = 1000;
        byte[] value = new byte[100];
        for (int i = 0; i < total; i++) {
            ramcloud.write(tableId, i + "", value);
        }
        TableIterator iterator = ramcloud.getTableIterator(tableId);
        RAMCloudObject current = null;
        while ((current = iterator.next()) != null) {
            iterator.remove();
        }
        iterator = ramcloud.getTableIterator(tableId);
        assertFalse(iterator.hasNext());
    }
}
