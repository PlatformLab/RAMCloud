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

    @Test
    (
        groups = "RAMCloudTest",
        dependsOnGroups = "RAMCloudTestInit"
    )
    public void getRejectRulesBytesTest() {
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
    (
        groups = "RAMCloudTest",
        dependsOnGroups = "RAMCloudTestInit"
    )
    public void readTest_stringKey() {
        long version = ramcloud.write(tableId, key, "testValue");
        RAMCloudObject obj = ramcloud.read(tableId, key);
        assertEquals(key, obj.getKey());
        assertEquals("testValue", obj.getValue());
        assertEquals(version, obj.getVersion());
    }

    @Test
    (
        groups = "RAMCloudTest",
        dependsOnGroups = "RAMCloudTestInit"
    )
    public void readTest_byteKey() {
        long version = ramcloud.write(tableId, key, "testValue");
        RAMCloudObject obj = ramcloud.read(tableId, key.getBytes());
        assertEquals(key, obj.getKey());
        assertEquals("testValue", obj.getValue());
        assertEquals(version, obj.getVersion());
    }

    @Test
    (
        groups = "RAMCloudTest",
        dependsOnGroups = "RAMCloudTestInit"
    )
    public void readTest_stringKeyWithRejectRules() {
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
    (
        groups = "RAMCloudTest",
        dependsOnGroups = "RAMCloudTestInit"
    )
    public void readTest_byteKeyWithRejectRules() {
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
        groups = "RAMCloudTest",
        dependsOnGroups = "RAMCloudTestInit"
    )
    public void removeTest_byteKey() {
        long version = ramcloud.write(tableId, key, "testValue");
        assertEquals(version, ramcloud.remove(tableId, key.getBytes()));
    }

    @Test
    (
        groups = "RAMCloudTest",
        dependsOnGroups = "RAMCloudTestInit"
    )
    public void removeTest_stringKey() {
        long version = ramcloud.write(tableId, key, "testValue");
        assertEquals(version, ramcloud.remove(tableId, key));
    }

    @Test
    (
        groups = "RAMCloudTest",
        dependsOnGroups = "RAMCloudTestInit"
    )
    public void removeTest_stringKeyWithRejectRules() {
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
    (
        groups = "RAMCloudTest",
        dependsOnGroups = "RAMCloudTestInit"
    )
    public void removeTest_byteKeyWithRejectRules() {
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
        groups = "RAMCloudTest",
        dependsOnGroups = "RAMCloudTestInit"
    )
    public void writeTest_stringKeyStringValue() {
        long version = ramcloud.write(tableId, key, "testValue");
        RAMCloudObject obj = ramcloud.read(tableId, key);
        assertEquals(key, obj.getKey());
        assertEquals("testValue", obj.getValue());
        assertEquals(version, obj.getVersion());
    }
    
    @Test
    (
        groups = "RAMCloudTest",
        dependsOnGroups = "RAMCloudTestInit"
    )
    public void writeTest_stringKeyStringValueWithRejectRules() {
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
    (
        groups = "RAMCloudTest",
        dependsOnGroups = "RAMCloudTestInit"
    )
    public void writeTest_stringKeyByteValue() {
        long version = ramcloud.write(tableId, key, "testValue".getBytes());
        RAMCloudObject obj = ramcloud.read(tableId, key);
        assertEquals(key, obj.getKey());
        assertEquals("testValue", obj.getValue());
        assertEquals(version, obj.getVersion());
    }
    
    @Test
    (
        groups = "RAMCloudTest",
        dependsOnGroups = "RAMCloudTestInit"
    )
    public void writeTest_stringKeyByteValueWithRejectRules() {
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
    (
        groups = "RAMCloudTest",
        dependsOnGroups = "RAMCloudTestInit"
    )
    public void writeTest_byteKeyByteValueWithRejectRules() {
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
        groups = "RAMCloudTestInit",
        dependsOnGroups = "UnitTestInit"
    )
    public void createTableTest() {
        tableId = ramcloud.createTable("testTable", 1);
        long readId = ramcloud.getTableId("testTable");
        assertEquals(tableId, readId);
    }
    
    @Test
    (
        groups = "RAMCloudTest",
        dependsOnGroups = "RAMCloudTestInit"
    )
    public void createTableTest_defaultServerSpan() {
        long tableId = ramcloud.createTable("testTable2");
        long readId = ramcloud.getTableId("testTable2");
        assertEquals(tableId, readId);
    }

    @Test
    (
        groups = "RAMCloudTestFinish",
        dependsOnGroups = "RAMCloudTest",
        expectedExceptions = TableDoesntExistException.class
    )
    public void dropTableTest() {
        ramcloud.dropTable("testTable");
        ramcloud.getTableId("testTable");
    }
}
