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

import static edu.stanford.ramcloud.ClientException.*;
import edu.stanford.ramcloud.*;
import edu.stanford.ramcloud.multiop.*;
import static edu.stanford.ramcloud.test.ClientTestClusterSetup.*;

import org.testng.annotations.*;
import static org.testng.AssertJUnit.*;
import static org.testng.Reporter.*;

/**
 * Unit tests for RAMCloud class.
 */
public class RAMCloudTest {
    private long tableId;
    private String key;

    /**
     * The key that each method will use to test with will be the
     * method name.
     */
    @BeforeMethod
    public void beforeMethod(Method method) {
        key = method.getName();
    }

    @BeforeClass
    public void RAMCloudTestSetup() {
        tableId = ramcloud.createTable("testTable");
    }

    @AfterClass
    public void RAMCloudTestCleanUp() {
        ramcloud.dropTable("testTable");
    }

    @Test
    public void getRejectRulesBytes() {
        for (int i = 0; i <= 2; i++) {
            RejectRules rules = new RejectRules();
            rules.setGivenVersion(i);
            byte[] expected = {(byte)i,0,0,0,0,0,0,0,0,0,0,0};
            assertArrayEquals(expected, RAMCloud.getRejectRulesBytes(rules));
            rules.rejectIfDoesntExist(true);
            expected[8] = 1;
            assertArrayEquals(expected, RAMCloud.getRejectRulesBytes(rules));
            rules.rejectIfExists(true);
            expected[9] = 1;
            assertArrayEquals(expected, RAMCloud.getRejectRulesBytes(rules));
            rules.rejectIfVersionLeGiven(true);
            expected[10] = 1;
            assertArrayEquals(expected, RAMCloud.getRejectRulesBytes(rules));
            rules.rejectIfVersionNeGiven(true);
            expected[11] = 1;
            assertArrayEquals(expected, RAMCloud.getRejectRulesBytes(rules));
        }
    }

    @Test
    public void read_stringKey() {
        long version = ramcloud.write(tableId, key, "testValue");
        RAMCloudObject obj = ramcloud.read(tableId, key);
        assertEquals(key, obj.getKey());
        assertEquals("testValue", obj.getValue());
        assertEquals(version, obj.getVersion());
    }

    @Test
    public void read_byteKey() {
        long version = ramcloud.write(tableId, key, "testValue");
        RAMCloudObject obj = ramcloud.read(tableId, key.getBytes());
        assertEquals(key, obj.getKey());
        assertEquals("testValue", obj.getValue());
        assertEquals(version, obj.getVersion());
    }

    @Test
    public void read_stringKeyWithRejectRules() {
        long version = ramcloud.write(tableId, key, "testValue");
        RejectRules rules = new RejectRules();
        rules.setGivenVersion(version);
        rules.rejectIfVersionNeGiven(true);
        
        RAMCloudObject obj = ramcloud.read(tableId, key, rules);
        assertEquals(key, obj.getKey());
        assertEquals("testValue", obj.getValue());
        assertEquals(version, obj.getVersion());

        rules.setGivenVersion(version + 1);
        try {
            ramcloud.read(tableId, key, rules);
            fail();
        } catch (WrongVersionException ex) {
            // Good
        }
    }

    @Test
    public void read_byteKeyWithRejectRules() {
        long version = ramcloud.write(tableId, key, "testValue");
        RejectRules rules = new RejectRules();
        rules.setGivenVersion(version);
        rules.rejectIfVersionNeGiven(true);
        
        RAMCloudObject obj = ramcloud.read(tableId, key.getBytes(), rules);
        assertEquals(key, obj.getKey());
        assertEquals("testValue", obj.getValue());
        assertEquals(version, obj.getVersion());

        rules.setGivenVersion(version + 1);
        try {
            ramcloud.read(tableId, key.getBytes(), rules);
            fail();
        } catch (WrongVersionException ex) {
            // Good
        }
    }

    @Test
    (
        expectedExceptions = ObjectDoesntExistException.class
    )
    public void read_exception() {
        ramcloud.read(tableId, key);
    }

    @Test
    public void remove_byteKey() {
        long version = ramcloud.write(tableId, key, "testValue");
        assertEquals(version, ramcloud.remove(tableId, key.getBytes()));
    }

    @Test
    public void remove_stringKey() {
        long version = ramcloud.write(tableId, key, "testValue");
        assertEquals(version, ramcloud.remove(tableId, key));
    }

    @Test
    public void remove_stringKeyWithRejectRules() {
        long version = ramcloud.write(tableId, key, "testValue");
        RejectRules rules = new RejectRules();
        rules.setGivenVersion(version + 1);
        rules.rejectIfVersionNeGiven(true);
        
        try {
            ramcloud.remove(tableId, key, rules);
            fail();
        } catch (WrongVersionException ex) {
            // Good
        }
        rules.setGivenVersion(version);
        assertEquals(version, ramcloud.remove(tableId, key, rules));
    }

    @Test
    public void remove_byteKeyWithRejectRules() {
        long version = ramcloud.write(tableId, key, "testValue");
        RejectRules rules = new RejectRules();
        rules.setGivenVersion(version + 1);
        rules.rejectIfVersionNeGiven(true);
        
        try {
            ramcloud.remove(tableId, key.getBytes(), rules);
            fail();
        } catch (WrongVersionException ex) {
            // Good
        }
        rules.setGivenVersion(version);
        assertEquals(version, ramcloud.remove(tableId, key.getBytes(), rules));
    }

    @Test
    (
        expectedExceptions = TableDoesntExistException.class
    )
    public void remove_exception() {
        ramcloud.remove(tableId + 1, key);
    }

    @Test
    public void write_stringKeyStringValue() {
        long version = ramcloud.write(tableId, key, "testValue");
        RAMCloudObject obj = ramcloud.read(tableId, key);
        assertEquals(key, obj.getKey());
        assertEquals("testValue", obj.getValue());
        assertEquals(version, obj.getVersion());
    }
    
    @Test
    public void write_stringKeyStringValueWithRejectRules() {
        RejectRules rules = new RejectRules();
        long version = ramcloud.write(tableId, key, "testValue", rules);
        
        RAMCloudObject obj = ramcloud.read(tableId, key);
        assertEquals(key, obj.getKey());
        assertEquals("testValue", obj.getValue());
        assertEquals(version, obj.getVersion());
        
        rules.setGivenVersion(version + 1);
        rules.rejectIfVersionNeGiven(true);
        try {
            ramcloud.write(tableId, key, "testValue", rules);
            fail();
        } catch (WrongVersionException ex) {
            // Good
        }
    }

    @Test
    public void write_stringKeyByteValue() {
        long version = ramcloud.write(tableId, key, "testValue".getBytes());
        RAMCloudObject obj = ramcloud.read(tableId, key);
        assertEquals(key, obj.getKey());
        assertEquals("testValue", obj.getValue());
        assertEquals(version, obj.getVersion());
    }
    
    @Test
    public void write_stringKeyByteValueWithRejectRules() {
        RejectRules rules = new RejectRules();
        long version = ramcloud.write(tableId, key, "testValue".getBytes(), rules);
        
        RAMCloudObject obj = ramcloud.read(tableId, key);
        assertEquals(key, obj.getKey());
        assertEquals("testValue", obj.getValue());
        assertEquals(version, obj.getVersion());
        
        rules.setGivenVersion(version + 1);
        rules.rejectIfVersionNeGiven(true);
        try {
            ramcloud.write(tableId, key, "testValue".getBytes(), rules);
            fail();
        } catch (WrongVersionException ex) {
            // Good
        }
    }
    
    @Test
    public void write_byteKeyByteValueWithRejectRules() {
        RejectRules rules = new RejectRules();
        long version = ramcloud.write(tableId, key.getBytes(), "testValue".getBytes(), rules);
        
        RAMCloudObject obj = ramcloud.read(tableId, key);
        assertEquals(key, obj.getKey());
        assertEquals("testValue", obj.getValue());
        assertEquals(version, obj.getVersion());
        
        rules.setGivenVersion(version + 1);
        rules.rejectIfVersionNeGiven(true);
        try {
            ramcloud.write(tableId, key.getBytes(), "testValue".getBytes(), rules);
            fail();
        } catch (WrongVersionException ex) {
            // Good
        }
    }

    @Test
    (
        expectedExceptions = TableDoesntExistException.class
    )
    public void write_exception() {
        ramcloud.write(tableId + 100, key, key + "Value");
    }

    @Test
    public void createTable() {
        long tableId = ramcloud.createTable(key, 1);
        long readId = ramcloud.getTableId(key);
        assertEquals(tableId, readId);
        ramcloud.dropTable(key);
    }
    
    @Test
    public void createTable_defaultServerSpan() {
        long tableId = ramcloud.createTable(key);
        long readId = ramcloud.getTableId(key);
        assertEquals(tableId, readId);
        ramcloud.dropTable(key);
    }

    @Test
    (
        expectedExceptions = TableDoesntExistException.class
    )
    public void dropTable() {
        ramcloud.createTable(key);
        ramcloud.dropTable(key);
        ramcloud.getTableId(key);
    }

    @Test
    public void getTableId() {
        long tableId = ramcloud.createTable(key);
        assertEquals(tableId, ramcloud.getTableId(key));
        ramcloud.dropTable(key);
    }

    @Test
    public void getTableIterator() {
        TableIterator it = ramcloud.getTableIterator(tableId);
        assertNotNull(it);
    }

    @Test
    public void read_multi() {
        int count = 200;
        MultiReadObject[] reads = new MultiReadObject[count];
        for (int i = 0; i < count; i++) {
            String key = this.key + i;
            ramcloud.write(tableId, key, "value" + i);
            reads[i] = new MultiReadObject(tableId, key.getBytes());
        }
        ramcloud.read(reads);
        for (int i = 0; i < count; i++) {
            assertEquals("value" + i, reads[i].getValue());
            ramcloud.remove(tableId, this.key + i);
        }
    }

    @Test
    public void read_multiLargeObjects() {
        int count = 100;
        MultiReadObject[] reads = new MultiReadObject[count];
        String value = "a";
        for (int j = 0; j < 15; j++) {
            value += value;
        }
        for (int i = 0; i < count; i++) {
            String key = this.key + i;
            ramcloud.write(tableId, key, value);
            reads[i] = new MultiReadObject(tableId, key.getBytes());
        }
        ramcloud.read(reads);
        for (int i = 0; i < count; i++) {
            assertEquals(value, reads[i].getValue());
            ramcloud.remove(tableId, this.key + i);
        }
    }

    @Test
    public void read_multiError() {
        int count = 10;
        MultiReadObject[] reads = new MultiReadObject[count];
        for (int i = 0; i < count; i++) {
            String key = this.key + i;
            reads[i] = new MultiReadObject(tableId, key.getBytes());
        }
        ramcloud.read(reads);
        for (int i = 0; i < count; i++) {
            assertEquals(Status.STATUS_OBJECT_DOESNT_EXIST, reads[i].getStatus());
        }
    }

    @Test
    public void write_multi() {
        int count = 200;
        MultiWriteObject[] writes = new MultiWriteObject[count];
        for (int i = 0; i < count; i++) {
            writes[i] = new MultiWriteObject(tableId, key + i,
                                            "value" + i);
        }
        ramcloud.write(writes);
        for (int i = 0; i < count; i++) {
            assertEquals(Status.STATUS_OK, writes[i].getStatus());
            RAMCloudObject object = ramcloud.read(tableId, this.key + i);
            assertEquals("value" + i, object.getValue());
            ramcloud.remove(tableId, this.key + i);
        }
    }

    @Test
    public void write_multiRejectRules() {
        int count = 10;
        RejectRules rules = new RejectRules();
        rules.rejectIfDoesntExist(true);
        MultiWriteObject[] writes = new MultiWriteObject[count];
        for (int i = 0; i < count; i++) {
            writes[i] = new MultiWriteObject(tableId, key + i,
                                             "value" + i,
                                             rules);
        }
        ramcloud.write(writes);
        for (int i = 0; i < count; i++) {
            assertEquals(Status.STATUS_OBJECT_DOESNT_EXIST, writes[i].getStatus());
        }
    }

    @Test
    public void write_multiError() {
        int count = 10;
        MultiWriteObject[] writes = new MultiWriteObject[count];
        for (int i = 0; i < count; i++) {
            writes[i] = new MultiWriteObject(tableId + 1, key + i,
                                             "value" + i);
        }
        ramcloud.write(writes);
        for (int i = 0; i < count; i++) {
            assertEquals(Status.STATUS_TABLE_DOESNT_EXIST, writes[i].getStatus());
        }
    }

    @Test
    public void remove_multi() {
        int count = 200;
        MultiRemoveObject[] removes = new MultiRemoveObject[count];
        long[] versions = new long[count];
        for (int i = 0; i < count; i++) {
            versions[i] = ramcloud.write(tableId, key + i, "value" + i);
            removes[i] = new MultiRemoveObject(tableId, key + i);
        }
        ramcloud.remove(removes);
        for (int i = 0; i < count; i++) {
            assertEquals(Status.STATUS_OK, removes[i].getStatus());
            assertEquals(versions[i], removes[i].getVersion());
            try {
                ramcloud.read(tableId, key + i);
            } catch (ObjectDoesntExistException ex) {
                // Good
            }
        }
    }

    @Test
    public void remove_multiRejectRules() {
        int count = 10;
        MultiRemoveObject[] removes = new MultiRemoveObject[count];
        long[] versions = new long[count];
        for (int i = 0; i < count; i++) {
            versions[i] = ramcloud.write(tableId, key + i, "value" + i);
            RejectRules rules = new RejectRules();
            rules.rejectIfVersionNeGiven(true);
            rules.setGivenVersion(versions[i] + 1);
            removes[i] = new MultiRemoveObject(tableId, key + i, rules);
        }
        ramcloud.remove(removes);
        for (int i = 0; i < count; i++) {
            assertEquals(Status.STATUS_WRONG_VERSION, removes[i].getStatus());
            ramcloud.read(tableId, key + i);
        }
    }

    @Test
    public void remove_multiError() {
        int count = 10;
        MultiRemoveObject[] removes = new MultiRemoveObject[count];
        for (int i = 0; i < count; i++) {
            removes[i] = new MultiRemoveObject(tableId + 1, key + i);
        }
        ramcloud.remove(removes);
        for (int i = 0; i < count; i++) {
            assertEquals(Status.STATUS_TABLE_DOESNT_EXIST, removes[i].getStatus());
        }
    }
}
