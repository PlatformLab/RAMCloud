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
 * Unit tests for the MultiRemoveHandler class
 */
public class MultiRemoveHandlerTest {
    @Test
    public void writeRequest() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(100);
        byte[] key = "This is the key".getBytes();
        MultiRemoveObject obj = new MultiRemoveObject(1, key);
        MultiRemoveHandler handler = new MultiRemoveHandler(buffer, 0, 0);
        Boolean success = (Boolean) invoke(
                handler, "writeRequest",
                new Class[] {ByteBuffer.class, MultiRemoveObject.class},
                buffer, obj);
        assertTrue(success);
        assertEquals(key.length + 22, buffer.position());
        buffer.rewind();
        assertEquals(1, buffer.getLong());
        
        assertEquals(key.length, buffer.getShort());
        byte[] keyCheck = new byte[key.length];
        buffer.get(keyCheck);
        assertArrayEquals(key, keyCheck);
    }

    @Test
    public void writeRequest_overflow() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(25);
        byte[] key = "This is the key".getBytes();
        MultiRemoveObject obj = new MultiRemoveObject(1, key);
        MultiRemoveHandler handler = new MultiRemoveHandler(buffer, 0, 0);
        Boolean success = (Boolean) invoke(
                handler, "writeRequest",
                new Class[] {ByteBuffer.class, MultiRemoveObject.class},
                buffer, obj);
        assertFalse(success);
        assertEquals(0, buffer.position());
    }

    @Test
    public void readResponse() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(10);
        long version = 10;
        buffer.putLong(version)
                .rewind();
        MultiRemoveObject obj = new MultiRemoveObject(0,
                                                    (byte[]) null);
        MultiRemoveHandler handler = new MultiRemoveHandler(buffer, 0, 0);
        invoke(handler, "readResponse",
               new Class[] {ByteBuffer.class, MultiRemoveObject.class},
               buffer, obj);
        assertEquals(version, obj.getVersion());
    }
}
