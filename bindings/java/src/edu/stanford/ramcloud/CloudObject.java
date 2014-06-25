package edu.stanford.ramcloud;

/**
 * This class is returned by Read operations. It encapsulates the entire
 * object, including the key, value, and version.
 *
 * It mostly exists because Java doesn't support primitive out parameters or
 * multiple return values, and we don't know the object's size ahead of
 * time, so passing in a fixed-length array would be problematic.
 */
public class CloudObject {
    final private byte[] key;
    final private byte[] value;
    final private long version;
        
    CloudObject(byte[] _key, byte[] _value, long _version) {
        key = _key;
        value = _value;
        version = _version;
    }

    public byte[] getKeyBytes() {
        return key;
    }

    public byte[] getValueBytes() {
        return value;
    }

    public String getKey() {
        return new String(key);
    }

    public String getValue() {
        return new String(value);
    }

    public long getVersion() {
        return version;
    }
}
