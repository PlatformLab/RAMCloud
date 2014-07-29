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
package edu.stanford.ramcloud;

/**
 * This class is returned by Read operations. It encapsulates the entire object,
 * including the key, value, and version.
 *
 * It mostly exists because Java doesn't support primitive out parameters or
 * multiple return values, and we don't know the object's size ahead of time, so
 * passing in a fixed-length array would be problematic.
 */
public class RAMCloudObject {
    /**
     * The key of this object as a byte array.
     */
    private byte[] key;
    /**
     * The value of this object as a byte array.
     */
    private byte[] value;
    /**
     * The version of this object, read from the server.
     */
    private long version;

    /**
     * Constructs a new RAMCloudObject.
     *
     * @param key
     *            The key of the object, a variable length byte array.
     * @param value
     *            The value of the object, a variable length byte array.
     * @param version
     *            The version of the object.
     */
    public RAMCloudObject(byte[] key, byte[] value, long version) {
        this.key = key;
        this.value = value;
        this.version = version;
    }

    /**
     * RAMCloudObject default contructor.
     */
    public RAMCloudObject() {
    }

    /**
     * Get the key of the object.
     *
     * @return The key of the object as a byte array.
     */
    public byte[] getKeyBytes() {
        return key;
    }

    /**
     * Set the key of the object.
     *
     * @param key
     *      The key of the object as a byte array.
     */
    public void setKeyBytes(byte[] key) {
        this.key = key;
    }

    /**
     * Get the value of the object.
     *
     * @return The value of the object as a byte array.
     */
    public byte[] getValueBytes() {
        return value;
    }

    /**
     * Set the value of the object.
     *
     * @param value
     *      The value of the object as a byte array.
     */
    public void setValueBytes(byte[] value) {
        this.value = value;
    }

    /**
     * Get the key of the object.
     *
     * @return The key of the object as a String.
     */
    public String getKey() {
        return new String(key);
    }

    /**
     * Set the key of the object.
     *
     * @param key
     *      The key of the object as a String.
     */
    public void setKey(String key) {
        this.key = key.getBytes();
    }

    /**
     * Get the value of the object.
     *
     * @return The value of the object as a String.
     */
    public String getValue() {
        return new String(value);
    }

    /**
     * Set the value of the object.
     *
     * @param value
     *      The value of the object as a String.
     */
    public void setValue(String value) {
        this.value = value.getBytes();
    }

    /**
     * Get the version of the object.
     *
     * @return The version number of the object.
     */
    public long getVersion() {
        return version;
    }

    /**
     * Set the version of the object.
     *
     * @param version
     *      The version of the object.
     */
    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return String.format("CloudObject[key: %s, value: %s, version: %d]",
                getKey(), getValue(), getVersion());
    }
}
