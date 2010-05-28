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
 * \file
 * Header file for the RAMCloud::Buffer class.
 */

#ifndef RAMCLOUD_BUFFER_H
#define RAMCLOUD_BUFFER_H

#include <Common.h>

namespace RAMCloud {

/**
 * This class manages a logically linear array of bytes, which is implemented as
 * discontiguous chunks in memory. This class exists so that we can avoid copies
 * between the multiple layers of the RAMCloud system, by passing the Buffer
 * associated with memory regions instead of copying the regions themselves.
 *
 * TODO(ongaro): Get allocation of Chunks out of the critical path.
 */
class Buffer {
    class Chunk;

  public:

    /**
     * This class provides a way to iterate over the chunks of a Buffer.
     * This should only be used by the low-level networking code, as Buffer
     * provides more convenient methods to access the Buffer for higher-level
     * code.
     *
     * \warning
     * The Buffer must not be modified during the lifetime of the iterator.
     *
     * Historical note: This is largely useless now that Buffer is a linked
     * list. On the other hand, it proved to be a nice abstraction during that
     * change.
     */
    class Iterator {
      public:
        explicit Iterator(const Buffer& buffer);
        explicit Iterator(const Iterator& other);
        ~Iterator();
        Iterator& operator=(const Iterator& other);
        bool isDone() const;
        void next();
        void* getData() const;
        uint32_t getLength() const;

      private:
        /**
         * The current chunk over which we're iterating.
         * This starts out as #chunks and ends up at \c NULL.
         */
        Chunk* current;

      friend class BufferIteratorTest;
    };

    void prepend(void* src, uint32_t length);
    void append(void* src, uint32_t length);
    uint32_t peek(uint32_t offset, void** returnPtr);
    void* getRange(uint32_t offset, uint32_t length);
    uint32_t copy(uint32_t offset, uint32_t length, void* dest); // NOLINT

    /**
     * Returns the sum of the individual sizes of all the chunks composing this
     * Buffer.
     * \return See above.
     */
    uint32_t getTotalLength() const { return totalLength; }

    /**
     * Return the number of chunks composing this Buffer.
     * Along with #Iterator, this is useful for networking code that is trying
     * to export the Buffer into a different format.
     * \return See above.
     */
    uint32_t getNumberChunks() const { return numberChunks; }

    Buffer();
    Buffer(void* firstChunk, uint32_t firstChunkLen);
    ~Buffer();

  private:

    void copy(const Chunk* current, uint32_t offset,  // NOLINT
              uint32_t length, void* dest);
    void* allocateScratchRange(uint32_t length);

    /**
     * A Buffer is an ordered collection of Chunks. Each individual chunk
     * represents a physically contiguous region of memory. When taken
     * together, a list of Chunks represents a logically contiguous memory
     * region, i.e., this Buffer.
     */
    struct Chunk {
        Chunk* next;       /// The next Chunk in the list.
        void *data;        /// The data represented by this Chunk.
        uint32_t length;   /// The length of this Chunk in bytes.
    };

    /**
     * A list of allocated memory areas. See #scratchRanges.
     */
    struct ScratchRange {
        ScratchRange* next; /// The next range in the list.
        char data[0];       /// The actual memory starts here.
    };

    /**
     * The sum of the individual sizes of all the chunks in the chunks list.
     */
    uint32_t totalLength;

    /**
     * The number of chunks in the chunks list.
     */
    uint32_t numberChunks;

    /**
     * The linked list of chunks that make up this Buffer.
     */
    Chunk* chunks;

    /**
     * A singly linked list of extra scratch memory areas that are freed when
     * the Buffer is deallocated. This is used in #getRange().
     */
    ScratchRange* scratchRanges;

    friend class Iterator;
    friend class BufferTest;  // For CppUnit testing purposes.
    friend class BufferIteratorTest;

    DISALLOW_COPY_AND_ASSIGN(Buffer);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_BUFFER_H
