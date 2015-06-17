/**
 * Copyright (c) 2013 Stanford University. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/*
 * This file implements a RAMCloud client DB layer for YCSB (Yahoo!'s Cloud
 * Storage Benchmark suite).
 *
 * See https://github.com/brianfrankcooper/YCSB/wiki/Adding-a-Database and the
 * com.yahoo.ycsb.DB abstract class if you're starting to hack on this.
 */

package com.yahoo.ycsb.db;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.ArrayList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.BufferUnderflowException;

import edu.stanford.ramcloud.RAMCloud;
import edu.stanford.ramcloud.RAMCloudObject;

public class RamCloudClient extends DB {
    private RAMCloud ramcloud;
    private HashMap<String, Long> tableIds;

    public static final String LOCATOR_PROPERTY = "ramcloud.coordinatorLocator";
    public static final String TABLE_SERVER_SPAN_PROPERTY = "ramcloud.tableServerSpan";
    public static final String DEBUG_PROPERTY = "ramcloud.debug";

    /// Success is always 0. 
    public static final int OK = 0;

    /// YCSB interprets anything non-0 as an error, but doesn't interpret the
    /// specific value.
    public static final int ERROR = 1;

    /// The number of servers each tablet should be split across (tables will
    /// be split tablets into this many tablets). This is set via the
    /// TABLE_SERVER_SPAN_PROPERTY, if given.
    private int tableServerSpan = 1;

    /// Value from the DEBUG_PROPERTY property. If true, print various
    /// messages to stderr that give some minor insight into what's going
    /// on.
    private static boolean debug = false;

    /**
     * This method returns the 64-bit table identifier for the given table,
     * creating it first if necessary.
     */
    private long
    getTableId(String tableName)
    {
        if (!tableIds.containsKey(tableName))
            tableIds.put(tableName, ramcloud.createTable(tableName, tableServerSpan));
        return tableIds.get(tableName);
    }

    /**
     * Serialize the fields and values for a particular key into a byte[]
     * array to be written to RAMCloud. This method uses Java's built-in
     * Object serialization functionality.
     */
    private static byte[]
    serializeUNUSED(HashMap<String, ByteIterator> values)
    {
        Object object = StringByteIterator.getStringMap(values);
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try {
            ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
            objectStream.writeObject(object);
        } catch (Exception e) {
            if (debug)
                System.err.println("RamCloudClient serialization failed: " + e);
        }
        return byteStream.toByteArray();
    }

    /**
     * Deserialize the fields and values stored in a RAMCloud object blob
     * into the given HashMap. This method uses Java's built-in Object
     * deserialization functionality.
     */
    private static void
    deserializeUNUSED(byte[] bytes, HashMap<String, ByteIterator> into) throws DBException
    {
        Object object = null;
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        try {
            ObjectInputStream objectStream = new ObjectInputStream(byteStream);
            object = objectStream.readObject();
        } catch (Exception e) {
            if (debug)
                System.err.println("RamCloudClient deserialization failed: " + e);
        }
        HashMap<String, String> stringMap = (HashMap<String, String>)object;
        StringByteIterator.putAllAsByteIterators(into, stringMap);
    }

    /**
     * Serialize the fields and values for a particular key into a byte[]
     * array to be written to RAMCloud. This method uses a hand-coded
     * serializer. It's about 1.2-3x as fast as when using Java's object
     * serializer.
     */
    private static byte[]
    serialize(HashMap<String, ByteIterator> values)
    {
        byte[][] kvArray = new byte[values.size() * 2][];

        int kvIndex = 0;
        int totalLength = 0;
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            totalLength += 8;   // fields denoting key length and value length

            byte[] keyBytes = entry.getKey().getBytes();
            kvArray[kvIndex++] = keyBytes;
            totalLength += keyBytes.length;

            byte[] valueBytes = entry.getValue().toString().getBytes();
            kvArray[kvIndex++] = valueBytes;
            totalLength += valueBytes.length;
        }
        ByteBuffer buf = ByteBuffer.allocate(totalLength);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        kvIndex = 0;
        for (int i = 0; i < kvArray.length / 2; i++) {
            byte[] keyBytes = kvArray[kvIndex++];
            buf.putInt(keyBytes.length);
            buf.put(keyBytes);
            byte[] valueBytes = kvArray[kvIndex++];
            buf.putInt(valueBytes.length);
            buf.put(valueBytes);
        }

        return buf.array();
    }

    /**
     * Deserialize the fields and values stored in a RAMCloud object blob
     * into the given HashMap. This method uses a hand-coded deserializer.
     * It's about 3-5x as fast as when using Java's object deserializer.
     */
    private static void
    deserialize(byte[] bytes, HashMap<String, ByteIterator> into) throws DBException
    {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        boolean workingOnKey = false;
        try {
            while (true) {
                int keyByteLength = buf.getInt();
                workingOnKey = true;
                String key = new String(bytes, buf.position(), keyByteLength);
                buf.position(buf.position() + keyByteLength);

                int valueByteLength = buf.getInt();
                String value = new String(bytes, buf.position(), valueByteLength);
                buf.position(buf.position() + valueByteLength);

                into.put(key, new StringByteIterator(value));
                workingOnKey = false;
            }
        } catch (BufferUnderflowException e) {
            // Done, hopefully.
            if (buf.remaining() != 0 || workingOnKey) {
                throw new DBException("deserialize: ByteBuffer not parsed " +
                    "properly! Had " + buf.remaining() + " bytes left over!");
            }
        }
    }

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void
    init() throws DBException
    {
        assert ramcloud == null;
        assert tableIds == null;

        Properties props = getProperties();
        String locator = props.getProperty(LOCATOR_PROPERTY);
        if (locator == null)
            throw new DBException("Missing property " + LOCATOR_PROPERTY);

        String tableServerSpanString = props.getProperty(TABLE_SERVER_SPAN_PROPERTY);
        if (tableServerSpanString != null)
            tableServerSpan = new Integer(tableServerSpanString).intValue();

        if (props.getProperty(DEBUG_PROPERTY) != null)
            debug = true;

        if (debug)
            System.err.println("RamCloudClient connecting to " + locator + " ...");
        ramcloud = new RAMCloud(locator);
        tableIds = new HashMap<String, Long>();
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void
    cleanup() throws DBException
    {
        if (debug)
            System.err.println("RamCloudClient disconnecting ...");
        ramcloud.disconnect();
        ramcloud = null; 
        tableIds = null;
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error.
     */
    @Override
    public int
    delete(String table, String key) {
        try {
            ramcloud.remove(getTableId(table), key);
        } catch (Exception e) {
            if (debug)
                System.err.println("RamCloudClient delete threw: " + e);
            return ERROR;
        }
        return OK;
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error.
     */
    @Override
    public int
    insert(String table, String key, HashMap<String, ByteIterator> values)
    {
        byte[] value = serialize(values);
        try {
            ramcloud.write(getTableId(table), key, value);
        } catch (Exception e) {
            if (debug)
                System.err.println("RamCloudClient insert threw: " + e);
            return ERROR;
        }
        return OK;
    }

    /**
     * Read a record from the database. Each field/value pair from the result
     * will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    public int
    read(String table,
         String key,
         Set<String> fields,
         HashMap<String, ByteIterator> result)
    {
        RAMCloudObject object = null;
        HashMap<String, ByteIterator> map = new HashMap<String, ByteIterator>();
        try {
            object = ramcloud.read(getTableId(table), key);
        } catch (Exception e) {
            if (debug)
                System.err.println("RamCloudClient read threw: " + e);
            return ERROR;
        }

        try {
            deserialize(object.getValueBytes(), map);
        } catch (DBException e) {
            if (debug)
                System.err.println("RamCloudClient deserializer threw: " + e);
            return ERROR;
        }

        if (fields == null) {
            result.putAll(map);
        } else {
            for (String field : fields) {
                if (!map.containsKey(field))
                    return ERROR;
                result.put(field, map.get(field));
            }
        }

        return OK;
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value
     * pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value
     *               pairs for one record
     * @return Zero on success, a non-zero error code on error.
     */
    @Override
    public int
    scan(String table,
         String startkey,
         int recordcount,
         Set<String> fields,
         Vector<HashMap<String, ByteIterator>> result)
    {
        if (debug)
            System.err.println("Warning: RAMCloud doesn't support range scans yet.");
        return ERROR;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     *
     * The YCSB documentation makes no explicit mention of this, but as far as
     * I can gather an update here is akin to a SQL update. That is, we are
     * expected to preserve the values that aren't being updated. For RAMCloud,
     * this means having to do a read-modify-write.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error.
     */
    @Override
    public int
    update(String table, String key, HashMap<String, ByteIterator> values)
    {
        // XXX- Should we use conditional ops to ensure the RMW is atomic?
        HashMap<String, ByteIterator> oldValues = new HashMap<String, ByteIterator>();
        read(table, key, null, oldValues);
        oldValues.putAll(values);
        return insert(table, key, oldValues);
    }

    /***************************************************************************
     * The following methods are for debugging / benchmarking this class as a
     * standalone application.
     **************************************************************************/

    private static HashMap<String, ByteIterator>
    generateValues(int numFields, int fieldWidth)
    {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        String fieldValuePrototype = "";
        for (int i = 0; i < fieldWidth; i++)
            fieldValuePrototype += "!";
        for (int i = 0; i < numFields; i++) {
            String fieldValue = fieldValuePrototype.substring(0, fieldWidth - ("" + i).length()) + i;
            values.put("field" + i, new StringByteIterator(fieldValue));
        }
        return values;
    }

    private static double
    measureReadLatency(RamCloudClient client, int numFields, int fieldWidth)
    {
        HashMap<String, ByteIterator> values = generateValues(numFields, fieldWidth);
        client.insert("theTable", "theKey", values);
        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        long before = System.nanoTime();
        for (int i = 0; i < 100000; i++) {
            client.read("theTable", "theKey", null, result);
            result.clear();
        }
        long after = System.nanoTime();
        client.delete("theTable", "theKey");
        return ((double)(after - before) / 100000 / 1000);
    }

    private static double
    measureWriteLatency(RamCloudClient client, int numFields, int fieldWidth)
    {
        HashMap<String, ByteIterator> values = generateValues(numFields, fieldWidth);
        long before = System.nanoTime();
        for (int i = 0; i < 100000; i++)
            client.insert("theTable", "theKey", values);
        long after = System.nanoTime();
        client.delete("theTable", "theKey");
        return ((double)(after - before) / 100000 / 1000);
    }

    private static double
    measureSerializerLatency(int numFields, int fieldWidth)
    {
        HashMap<String, ByteIterator> values = generateValues(numFields, fieldWidth);
        long before = System.nanoTime();
        for (int i = 0; i < 100000; i++) {
            byte[] serialized = serialize(values);
        }
        long after = System.nanoTime();
        return ((double)(after - before) / 100000 / 1000);
    }

    private static double
    measureDeserializerLatency(int numFields, int fieldWidth)
    {
        HashMap<String, ByteIterator> values = generateValues(numFields, fieldWidth);
        byte[] serialized = serialize(values);
        HashMap<String, ByteIterator> results = new HashMap<String, ByteIterator>();
        long before = System.nanoTime();
        for (int i = 0; i < 100000; i++) {
            try {
                deserialize(serialized, results);
            } catch (DBException e) {
            }
            results.clear();
        }
        long after = System.nanoTime();
        return ((double)(after - before) / 100000 / 1000);
    }

    public static void
    main(String argv[])
    {
        if (argv.length != 1) {
            System.err.println("Error: first argument must be the " +
                "coordinator ServiceLocator string");
            return;
        }

        // argv[0] is the coordinatorLocator
        RamCloudClient client = new RamCloudClient();
        Properties props = client.getProperties();
        props.setProperty(LOCATOR_PROPERTY, argv[0]);
        try {
            client.init();
        } catch (DBException e) {
            System.err.println("Failed to initialize RamCloudClient: " + e);
            return;
        }

        // Warm up first.
        measureReadLatency(client, 5, 10);

        int[] fieldCounts = { 1, 2, 3, 5, 10, 20, 50, 100 };

        // Measure read performance. The RAMCloud bindings are good for about
        // 7.5us reads on Infiniband, so we need to be careful that our
        // field serialisation is fast.
        for (int fields : fieldCounts) {
            System.out.println("Avg read latency (" + fields +
                " field(s)):  " + measureReadLatency(client, fields, 100) + " us");
        }

        // And now write performance...
        for (int fields : fieldCounts) {
            System.out.println("Avg write latency (" + fields +
                " field(s)):  " + measureWriteLatency(client, fields, 100) + " us");
        }

        // And let's see how fast our serializer and deserializer are...
        for (int fields : fieldCounts) {
            System.out.println("Avg serialization / deserialization latency (" + fields +
                " field(s)):  " + measureSerializerLatency(fields, 100) + " / " +
                measureDeserializerLatency(fields, 100) + " us");
        }
    }
}
