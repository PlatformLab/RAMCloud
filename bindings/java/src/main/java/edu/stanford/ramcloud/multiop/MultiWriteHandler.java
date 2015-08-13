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
 * A class that implements the Java bindings to the multi-write operation.
 */
public class MultiWriteHandler extends MultiOpHandler<MultiWriteObject> {
    /**
     * Constructs a MultiWriteHandler object
     */
    public MultiWriteHandler(ByteBuffer byteBuffer,
                             long byteBufferPointer,
                             long ramcloudClusterHandle) {
        super(byteBuffer, byteBufferPointer, ramcloudClusterHandle);
        setBatchLimit(200);
    }
    
    @Override
    protected boolean writeRequest(ByteBuffer buffer, MultiWriteObject request) {
        byte[] key = request.getKeyBytes();
        byte[] value = request.getValueBytes();
        if (buffer.position() + 26 + key.length + value.length
                >= buffer.capacity()) {
            return false;
        }
        buffer.putLong(request.getTableId())
                .putShort((short) key.length)
                .put(key)
                .putInt(value.length)
                .put(value)
                .put(RAMCloud.getRejectRulesBytes(request.getRejectRules()));
        return true;
    }

    @Override
    protected void readResponse(ByteBuffer buffer, MultiWriteObject response) {
        response.setVersion(buffer.getLong());
    }

    @Override
    protected void callCppHandle(long byteBufferPointer) {
        cppMultiWrite(byteBufferPointer);
    }
}
