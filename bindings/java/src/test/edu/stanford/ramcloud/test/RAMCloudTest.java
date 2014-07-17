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
    public void createTable() {
        long tableId = ramcloud.createTable(key, 1);
        long readId = ramcloud.getTableId(key);
        assertEquals(tableId, readId);
    }
    
    @Test
    public void createTable_defaultServerSpan() {
        long tableId = ramcloud.createTable(key);
        long readId = ramcloud.getTableId(key);
        assertEquals(tableId, readId);
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
    }

    @Test
    public void getTableIterator() {
        TableIterator it = ramcloud.getTableIterator(tableId);
        assertNotNull(it);
    }
}
