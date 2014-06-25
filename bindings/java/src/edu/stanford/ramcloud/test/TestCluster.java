package edu.stanford.ramcloud.test;

public class TestCluster {
    static {
        System.loadLibrary("edu_stanford_ramcloud_test_TestCluster");
    }
    
    private long[] pointers;
    
    public TestCluster() {
        pointers = new long[2];
        createMockCluster(pointers);
    }

    private static native void createMockCluster(long[] pointers);
    private static native void destroy(long[] pointers);

    public long getRamcloudClientPointer() {
        return pointers[1];
    }

    public void destroy() {
        destroy(pointers);
    }
}
