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
    /**
     * Prepends a new memory region to the Buffer. This new region is added to
     * the beginning of the chunk list instead of at the end (like in the
     * appeand function).
     * 
     * \param[in] src The memory region to be added.
     * \param[in] size The size in bytes of the memory region represented by
                       buf.
     * \return Returns true or false depending on teh success of the prpepend
     * op.
     */
    void prepend (void* src, uint32_t size);
    
    /**
     * Appends a new memory region to the Buffer.
     *
     * \param[in] src The memory region to be added.
     * \param[in] size The size of the memory region represented by buf.
     * \return Returns true or false depending on the success of the append op.
     */
    void append (void* src, uint32_t size);
    
    /**
     * Returns the pointer to the buffer at the given offset. It does not
     * copy. The combination of the offset and length parameters represent a
     * logical block of memory that we return. If the block spans multiple
     * chunks, we only return the number of bytes upto the end of the first
     * chunk.
     *
     * \param[in] offset The offset into the logical buffer represented by this
     *             Buffer
     * \param[in] length The length of the memory block to return.
     * \param[out] return_ptr The pointer to the memory block requested.
     * \return The number of bytes that region of memory pointed to by
     *             return_ptr has.
     */
    uint32_t peek (uint32_t offset, uint32_t length, void** returnPtr);

    /**
     * If the logical memory region represented by the <offset, length> tuple
     * does not span any chunk boundaries, this function performs the same
     * functionality as peek.
     *
     * However, if the logical memory region represented by the <offset, length>
     * tuple spans one or more chunk boundaries, then we allocate buffer space
     * which can hold 'length' bytes, and 'copy' the required bytes into this
     * buffer space before returning a pointer to it.
     *
     * \param[in] offset The offset into the logical Buffer.
     * \param[in] length The length in bytes of the region we want to obtain.
     * \param[out] returnPtr The pointer to the memory region that we return.
     * \return The size in bytes of the memory region pointed to by returnPtr.
     */
    uint32_t read(uint32_t offset, uint32_t length, void **returnPtr);
    
    /**
     * Copies the memory block identified by the <offset, length> tuple into the
     * region pointed to by dest.
     *
     * If the <offset, length> memory block extends beyond the end of the
     * logical Buffer, we copy all the bytes from offset upto the end of the
     * Buffer, and returns the number of bytes copied.
     *
     * \param[in] offset The offset at which the memory block we should copy
                         starts.
     * \param[in] length The length of the memory block we should copy.
     * \param[in] dest The pointer to which we should copy the logical memory
                        block.
     * \return The actual number of bytes copied.
     */
    uint32_t copy (uint32_t offset, uint32_t length, void* dest);

    /**
     * Returns the entire length of this Buffer.
     *
     * \return Returns the total length of the logical Buffer represented by
     *         this object.
     */
    uint32_t totalLength() const { return totalLen; }

    /**
     * This constructor initializes all of the variables to their default
     * values. It also allocates memory for some initial chunks, so that we do
     * not have to allocates chunks on each append or prepend operation.
     */
    Buffer();

    /**
     * Deallocate some or all of the memory used during the life of this Buffer.
     */
    ~Buffer();

  private:
    /**
     * Allocates more chunks by growing the chunks array by a factor of 2.
     */
    void allocateMoreChunks();

    /**
     * Find the chunk which corresposnds to the supplied offset.
     * 
     * \param[in] offset The offset in bytes of the point to identify.
     * \return The chunk index of the chunk in which this offset lies.
     */
    uint32_t findChunk(uint32_t offset);

    /**
     * Returns the chunk's offset from the beginning of the Buffer.
     *
     * \param[in] chunkIndex The index of the chunk whose offset we calculate.
     * \return The offset, in bytes, of the beginning of the chunk.
     */
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
     * majority of Buffers. If not, we can increate this later.
     */
    static const int INITIAL_CHUNK_ARR_SIZE = 10;

    uint32_t chunksUsed;   // The index of the last chunk thats currently being
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
