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

#ifndef RAMCLOUD_BYTEBUFFER_H
#define RAMCLOUD_BYTEBUFFER_H

namespace RAMCloud {
 
/**
 * A class to parallel the Java ByteBuffer class. It keeps track of the current
 * mark in the buffer, in order to avoid having to increment a mark manually for
 * every read.
 */
class ByteBuffer {
  PUBLIC:
    /**
     * An index in the buffer signifying where in the buffer the next read or
     * write will go to.
     */
    uint32_t mark;
    
    /**
     * A pointer to the region of memory wrapped by the ByteBuffer. It is a char
     * pointer because arithmetic operations cannot be done on void pointers.
     */
    char* pointer;
    
    /**
     * Constructs a ByteBuffer pointing to the specified memory location,
     * initializing the mark to 0, or the beginning of the memory region.
     */
    ByteBuffer(uint64_t byteBufferPointer)
            : pointer(reinterpret_cast<char*>(byteBufferPointer))
            , mark(0) { }

    /**
     * Read some value from the ByteBuffer. This also increments the mark by the
     * size of the read type.
     */
    template<typename T> T read() {
        T out = *reinterpret_cast<T*>(pointer + mark);
        mark += sizeof(T);
        return out;
    }

    /**
     * Writes some value to the ByteBuffer. This also increments the mark by the
     * size of the type written.
     */
    template<typename T> void write(T value) {
        *reinterpret_cast<T*>(pointer + mark) = value;
        mark += sizeof(T);
    }

    /**
     * Reads a 8 byte long from the ByteBuffer, and interprets that as a pointer
     * of the type given. This also increments the mark by 8.
     */
    template<typename T> T* readPointer() {
        T* out = reinterpret_cast<T*>(
                *reinterpret_cast<uint64_t*>(
                        pointer + mark));
        mark += 8;
        return out;
    }

    /**
     * Gets a pointer to the current mark in the ByteBuffer as a void pointer,
     * and increments mark by the specified size.
     */
    void* getVoidPointer(uint32_t size = 0) {
        void* out = static_cast<void*>(pointer + mark);
        mark += size;
        return out;
    }

    /**
     * This returns a pointer to the region in the ByteBuffer offset by the mark.
     * Modifying the value at the pointer will also modify the contents of the
     * ByteBuffer.
     */
    template<typename T> T* getPointer() {
        T* out = reinterpret_cast<T*>(pointer + mark);
        mark += sizeof(T);
        return out;
    }

    /**
     * Resets the mark of this ByteBuffer to 0, or the beginning of the
     * ByteBuffer.
     */
    void rewind() {
        mark = 0;
    }
};

}

#endif // RAMCLOUD_BYTEBUFFER_H
