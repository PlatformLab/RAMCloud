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

package edu.stanford.ramcloud.multiop;

import edu.stanford.ramcloud.*;
import java.nio.ByteBuffer;

/**
 * A class that implements the Java bindings to the multi-read operation.
 */
public class MultiReadHandler extends MultiOpHandler<MultiReadObject> {
    /**
     * Constructs a MultiReadHandler object
     */
    public MultiReadHandler(ByteBuffer byteBuffer,
                            long byteBufferPointer,
                            long ramcloudClusterHandle) {
        super(byteBuffer, byteBufferPointer, ramcloudClusterHandle);
        setBatchLimit(200);
    }
    
    @Override
    protected boolean writeRequest(ByteBuffer buffer, MultiReadObject request) {
        byte[] key = request.getKeyBytes();
        if (buffer.position() + 10 + key.length >= buffer.capacity()) {
            return false;
        }
        buffer.putLong(request.getTableId())
                .putShort((short) key.length)
                .put(key);
        return true;
    }

    @Override
    protected void readResponse(ByteBuffer buffer, MultiReadObject response) {
        long version = buffer.getLong();
        response.setVersion(version);
        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value);
        response.setValueBytes(value);
    }

    @Override
    protected void callCppHandle(long byteBufferPointer) {
        cppMultiRead(byteBufferPointer);
    }
}
