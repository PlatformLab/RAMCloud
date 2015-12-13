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
 * Superclass for all MultiOperations utilizing a shared ByteBuffer between Java
 * and C++.
 */
public abstract class MultiOpHandler<T extends MultiOpObject> {
    static {
        Util.loadLibrary("ramcloud_java");
    }

    /**
     * The array of MultiOpObjects currently being processed
     */
    private T[] objects;

    /**
     * The shared ByteBuffer between Java and C++
     */
    private ByteBuffer byteBuffer;

    /**
     * The pointer to the memory region the shared ByteBuffer wraps for C++
     */
    private long byteBufferPointer;

    /**
     * A pointer to the C++ RAMCloud object
     */
    private long ramcloudClusterHandle;

    /**
     * The limit to how many operations to put into one multiop batch
     */
    private int batchLimit = 200;

    /**
     * Sets the class fields for this MultiOpHandler
     */
    public MultiOpHandler(ByteBuffer byteBuffer,
                          long byteBufferPointer,
                          long ramcloudClusterHandle) {
        this.byteBuffer = byteBuffer;
        this.byteBufferPointer = byteBufferPointer;
        this.ramcloudClusterHandle = ramcloudClusterHandle;
    }

    /**
     * Set the limit at which this MultiOpHandler will send a batch of requests
     * to C++.
     *
     * @param batchLimit
     *      When handling a request, when this many requests have been
     *      written to the ByteBuffer, a C++ call will be made to handle those
     *      requests.
     */
    public void setBatchLimit(int batchLimit) {
        this.batchLimit = batchLimit;
    }

    /**
     * Handles the MultiOp request.
     *
     * @param request
     *      An array of MultiOpObjects constituting this MultiOp request.
     */
    public void handle(T[] request) {
        this.objects = request;
        int totalLength = request.length;

        for (int start = 0; start < totalLength; start += batchLimit) {
            int length = Math.min(totalLength - start, batchLimit);
            int sent = 0;
            while (sent < length) {
                byteBuffer.rewind();
                byteBuffer.putLong(ramcloudClusterHandle);
                byteBuffer.putInt(start + sent);
                byteBuffer.putInt(0); // Placeholder for numObjects
                int i;
                for (i = 0; i < length - sent; i++) {
                    if (!writeRequest(byteBuffer, request[i + sent + start])) {
                        break;
                    }
                }
                byteBuffer.putInt(12, i);
                callCppHandle(byteBufferPointer);
                sent += i;
            }
        }
    }

    /**
     * Reads the data currently in the ByteBuffer, and stores it in the
     * MultiOpObject objects array. This method is called only by C++, when one
     * of two conditions are met: The C++ code can no longer fit more responses
     * in the ByteBuffer, or when the C++ code has finished putting responses
     * in the ByteBuffer.
     */
    private void unloadBuffer() {
        byteBuffer.rewind();
        int startIndex = byteBuffer.getInt();
        int numResults = byteBuffer.getInt();

        for (int i = 0; i < numResults; i++) {
            int status = byteBuffer.getInt();
            objects[startIndex + i].setStatus(Status.statuses[status]);
            if (status == 0) {
                readResponse(byteBuffer, objects[startIndex + i]);
            }
        }
    }

    /**
     * Try to write the single specified multiop request to the ByteBuffer.
     *
     * @param buffer
     *      The ByteBuffer to append to.
     * @param request
     *      The request to append.
     * @return Whether or not the request was actually written. The only
     *      circumstance under which this should return false is when there was
     *      no more room left in the ByteBuffer to fit the request.
     */
    protected abstract boolean writeRequest(ByteBuffer buffer, T request);

    /**
     * Reads a single multiop response from the specified ByteBuffer into the
     * specified MultiOpObject.
     *
     * @param buffer
     *      The ByteBuffer to read from.
     * @param response
     *      The MultiOpObject to store the response in.
     */
    protected abstract void readResponse(ByteBuffer buffer, T response);

    /**
     * Call the C++ implementation for handling the multiop request.
     *
     * @param byteBufferPointer
     *      A pointer to the shared memory location between Java and C++.
     */
    protected abstract void callCppHandle(long byteBufferPointer);

    // Documentation for native methods located in C++ files
    protected native void cppMultiRead(long byteBufferPointer);
    protected native void cppMultiWrite(long byteBufferPointer);
    protected native void cppMultiRemove(long byteBufferPointer);
}
