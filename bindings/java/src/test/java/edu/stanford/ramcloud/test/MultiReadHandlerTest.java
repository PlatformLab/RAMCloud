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

import edu.stanford.ramcloud.*;
import edu.stanford.ramcloud.multiop.*;
import static edu.stanford.ramcloud.test.ClientTestClusterSetup.*;

import org.testng.annotations.*;
import static org.testng.AssertJUnit.*;

import java.nio.ByteBuffer;

/**
 * Unit tests for the MultiReadHandler class
 */
public class MultiReadHandlerTest {
    @Test
    public void writeRequest() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(50);
        byte[] key = "This is the key".getBytes();
        MultiReadObject obj = new MultiReadObject(1, key);
        MultiReadHandler handler = new MultiReadHandler(buffer, 0, 0);
        Boolean success = (Boolean) invoke(
                handler, "writeRequest",
                new Class[] {ByteBuffer.class, MultiReadObject.class},
                buffer, obj);
        assertTrue(success);
        assertEquals(key.length + 10, buffer.position());
        buffer.rewind();
        assertEquals(1, buffer.getLong());
        assertEquals(key.length, buffer.getShort());
        byte[] keyCheck = new byte[key.length];
        buffer.get(keyCheck);
        assertArrayEquals(key, keyCheck);
    }

    @Test
    public void writeRequest_overflow() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(12);
        byte[] key = "This is the key".getBytes();
        MultiReadObject obj = new MultiReadObject(1, key);
        MultiReadHandler handler = new MultiReadHandler(buffer, 0, 0);
        Boolean success = (Boolean) invoke(
                handler, "writeRequest",
                new Class[] {ByteBuffer.class, MultiReadObject.class},
                buffer, obj);
        assertFalse(success);
        assertEquals(0, buffer.position());
    }

    @Test
    public void readResponse() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(50);
        long version = 3;
        byte[] value = "This is the value".getBytes();
        buffer.putLong(version)
                .putInt(value.length)
                .put(value)
                .rewind();
        MultiReadObject obj = new MultiReadObject(0, (byte[]) null);
        MultiReadHandler handler = new MultiReadHandler(buffer, 0, 0);
        invoke(handler, "readResponse",
               new Class[] {ByteBuffer.class, MultiReadObject.class},
               buffer, obj);
        assertEquals(version, obj.getVersion());
        assertArrayEquals(value, obj.getValueBytes());
    }
}
