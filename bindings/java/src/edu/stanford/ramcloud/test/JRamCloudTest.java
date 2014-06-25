// TODO: comment

package edu.stanford.ramcloud.test;

import edu.stanford.ramcloud.exception.*;
import edu.stanford.ramcloud.*;

import org.testng.annotations.*;
import static org.testng.AssertJUnit.*;
import static org.testng.Reporter.*;

public class JRamCloudTest {
    private long tableId;
    private long version;

    @Test
    public void createTableTest() {
        tableId = ClientTests.ramcloud.createTable("testTable");
        long readId = ClientTests.ramcloud.getTableId("testTable");
        assertEquals(tableId, readId);
    }

    @Test(dependsOnMethods = {"createTableTest"})
    public void readWriteTest() {
        version = ClientTests.ramcloud.write(tableId, "key", "value");
        CloudObject obj = ClientTests.ramcloud.read(tableId, "key");
        assertEquals("key", obj.getKey());
        assertEquals("value", obj.getValue());
        assertEquals(version, obj.getVersion());
    }

    @Test(
        dependsOnMethods = {"readWriteTest"},
        expectedExceptions = ObjectDoesntExistException.class
    )
    public void removeTest() {
        ClientTests.ramcloud.remove(tableId, "key");
        ClientTests.ramcloud.read(tableId, "key");
    }

    @Test(dependsOnMethods = {"readWriteTest"})
    public void rejectRulesReadTest() {
        RejectRules rules = new RejectRules();
        rules.setGivenVersion(version);
        rules.setVersionNeGiven(true);
        ClientTests.ramcloud.read(tableId, "key", rules);
        rules.setGivenVersion(version + 1);
        try {
            ClientTests.ramcloud.read(tableId, "key", rules);
            fail("Read - Reject Rules: VersionNeGiven failed");
        } catch(WrongVersionException ex) {
            // OK 
        }
        
        rules.setVersionNeGiven(false);
        rules.setVersionLeGiven(true);
        rules.setGivenVersion(version - 1);
        ClientTests.ramcloud.read(tableId, "key", rules);
        rules.setGivenVersion(version + 1);
        try {
            ClientTests.ramcloud.read(tableId, "key", rules);
            fail("Read - Reject Rules: VerionLeGiven failed");
        } catch(WrongVersionException ex) {
            // OK
        }
    }

    @Test(dependsOnMethods = {"rejectRulesReadTest"})
    public void rejectRulesWriteTest() {
        RejectRules rules = new RejectRules();
        rules.setGivenVersion(version);
        rules.setVersionNeGiven(true);
        version = ClientTests.ramcloud.write(tableId, "key", "value", rules);
        rules.setGivenVersion(version + 1);
        try {
            version = ClientTests.ramcloud.write(tableId, "key", "value", rules);
            fail("Write - Reject Rules: VersionNeGiven failed");
        } catch(WrongVersionException ex) {
            // OK
        }
        
        rules.setVersionNeGiven(false);
        rules.setVersionLeGiven(true);
        rules.setGivenVersion(version - 1);
        version = ClientTests.ramcloud.write(tableId, "key", "value", rules);
        rules.setGivenVersion(version + 1);
        try {
            version = ClientTests.ramcloud.write(tableId, "key", "value", rules);
            fail("Write - Reject Rules: VerionLeGiven failed");
        } catch(WrongVersionException ex) {
            // OK
        }

        rules.setVersionLeGiven(false);
        rules.setExists(true);
        ClientTests.ramcloud.write(tableId, "key2", "value2", rules);
        try {
            ClientTests.ramcloud.write(tableId, "key2", "value2", rules);
            fail("Write - Reject Rules: Exists failed");
        } catch(edu.stanford.ramcloud.exception.ObjectExistsException ex) {
            // OK
        }
        
        rules.setExists(false);
        rules.setDoesntExist(true);
        ClientTests.ramcloud.write(tableId, "key2", "value2", rules);
        try {
            ClientTests.ramcloud.write(tableId, "key3", "value2", rules);
            fail("Write - Reject Rules: DoesntExist failed");
        } catch(ObjectDoesntExistException ex) {
            // OK
        }
    }

    @Test(dependsOnMethods = {"rejectRulesWriteTest"})
    public void rejectRulesRemoveTest() {
        RejectRules rules = new RejectRules();
        rules.setVersionNeGiven(true);
        rules.setGivenVersion(version + 1);
        try {
            ClientTests.ramcloud.remove(tableId, "key", rules);
            fail("Remove - Reject Rules: VersionNeGiven failed");
        } catch(WrongVersionException ex) {
            // OK 
        }
        rules.setGivenVersion(version);
        ClientTests.ramcloud.remove(tableId, "key", rules);

        version = ClientTests.ramcloud.read(tableId, "key2").getVersion();
        rules.setVersionNeGiven(false);
        rules.setVersionLeGiven(true);
        rules.setGivenVersion(version + 1);
        try {
            ClientTests.ramcloud.remove(tableId, "key2", rules);
            fail("Remove - Reject Rules: VerionLeGiven failed");
        } catch(WrongVersionException ex) {
            // OK
        }
        rules.setGivenVersion(version - 1);
        ClientTests.ramcloud.remove(tableId, "key2", rules);
    }

    @Test(
        dependsOnMethods = {"rejectRulesRemoveTest"},
        expectedExceptions = TableDoesntExistException.class
    )
    public void dropTableTest() {
        ClientTests.ramcloud.dropTable("testTable");
        ClientTests.ramcloud.getTableId("testTable");
    }
}
