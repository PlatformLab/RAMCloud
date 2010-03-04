/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file Header file for the Buffer class.
 */

#ifndef RAMCLOUD_BUFFER_H
#define RAMCLOUD_BUFFER_H

#include <Common.h>

namespace RAMCloud {

/**
 * \class
 *
 * This class manages a logically linear array of bytes, which is implemented as
 * discontiguous chunks in memory. This class exists so that we can avoid copies
 * between the multiple layers of the RAMCloud system, by passing the Buffer
 * associated with memory regions instead of copying the regions themselves.
 */     
class Buffer {
  public:
    void prepend (void* src, uint32_t size);

    void append (void* src, uint32_t size);
    
    uint32_t peek (uint32_t offset, uint32_t length, void** returnPtr);

    uint32_t read(uint32_t offset, uint32_t length, void **returnPtr);
    
    uint32_t copy (uint32_t offset, uint32_t length, void* dest);

    /**
     * Returns the entire length of this Buffer.
     *
     * \return Returns the total length of the logical Buffer represented by
     *         this object.
     */
    uint32_t totalLength() const { return totalLen; }

    Buffer();

    ~Buffer();

  private:
    void allocateMoreChunks();

    uint32_t findChunk(uint32_t offset);

    uint32_t offsetOfChunk(uint32_t chunkIndex);
    
    /**
     * A Buffer is an ordered collection of Chunks. Each induvidual chunk
     * represents a physically contiguous region of memory. When taking
     * together, an array of Chunks represent a logically contiguous memory
     * region, i.e., this Buffer.
     */
    struct Chunk {
        void *ptr;      // Pointer to the data represented by this Chunk.
        uint32_t size;  // The size of this Chunk in bytes.
    };

    /**
     * The initial size of the chunk array (see below). 10 should cover the vast
     * majority of Buffers. If not, we can increase this later.
     */
    static const int INITIAL_CHUNK_ARR_SIZE = 10;

    uint32_t chunksUsed;  // The index of the last chunk thats currently being
                          // used.
    uint32_t chunksAvail;  // The total number of chunks that have been
                           // allocated so far.
    Chunk* chunks;  // The pointers and lengths of various chunks represented
                    // by this Buffer. Initially, we allocate
                    // INITIAL_CHUNK_ARR_SIZE of the above chunks, since this
                    // would be faster than using a vector<chunk>.
    uint32_t totalLen;  // The total length of all the memory blocks represented
                        // by this Buffer.
    void *bufRead;  // Space to be used when a call to read spans a Chunk
                    // boundary. In this case, we allocate space here, and
                    // return a pointer to this.
    uint32_t bufReadSize;  // The size of the bufRead memory region.

    friend class BufferTest;  // For CppUnit testing purposes.

    DISALLOW_COPY_AND_ASSIGN(Buffer);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_BUFFERPTR_H
