
// TODO: comment

package edu.stanford.ramcloud.test;

import edu.stanford.ramcloud.*;

import org.testng.annotations.*;

public class ClientTests {
    public static JRamCloud ramcloud;
    public static TestCluster cluster;
    
    @BeforeSuite
    public void setupClient() {
        cluster = new TestCluster();
        ClientTests.ramcloud = new JRamCloud(cluster.getRamcloudClientPointer());
    }

    @AfterSuite
    public void cleanUpClient() {
        cluster.destroy();
    }
}
