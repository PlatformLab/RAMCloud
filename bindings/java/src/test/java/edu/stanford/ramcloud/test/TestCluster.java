package edu.stanford.ramcloud.test;

public class TestCluster {
    static {
        // Load native library
        System.loadLibrary("edu_stanford_ramcloud_test_TestCluster");
    }

    // Keep pointers to the C++ RAMCloud and MockCluster objects.
    private long[] pointers;

    /**
     * Construct a new TestCluster with one master.
     */
    public TestCluster() {
        pointers = new long[2];
        createMockCluster(pointers);
    }

    // Native methods documented in corresponding C++ file.
    private static native void createMockCluster(long[] pointers);
    private static native void destroy(long[] pointers);

    /**
     * Get the pointer to the C++ RamCloud object.
     *
     * @return The memory address of the C++ RamCloud object, for use in
     *         contructing a Java RAMCloud object tied to the C++ RamCloud
     *         object.
     */
    public long getRamcloudClientPointer() {
        return pointers[1];
    }

    /**
     * When the test is done with the TestCluster and RAMCloud object, call this
     * method to delete the pointers to the corresponding C++ objects.
     */
    public void destroy() {
        destroy(pointers);
    }
}
