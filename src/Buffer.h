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
 * A dummy type for distinguishing the different operator new calls the Buffer
 * provides.
 */
enum PREPEND_T { PREPEND };

/**
 * A dummy type for distinguishing the different operator new calls the Buffer
 * provides.
 */
enum APPEND_T { APPEND };

/**
 * A dummy type for distinguishing the different operator new calls the Buffer
 * provides.
 */
enum CHUNK_T { CHUNK };

/**
 * A dummy type for distinguishing the different operator new calls the Buffer
 * provides.
 */
enum MISC_T { MISC };

class Buffer;

}

void* operator new(size_t numBytes, RAMCloud::Buffer* buffer,
                   RAMCloud::PREPEND_T prepend);
void* operator new[](size_t numBytes, RAMCloud::Buffer* buffer,
                     RAMCloud::PREPEND_T prepend);
void* operator new(size_t numBytes, RAMCloud::Buffer* buffer,
                   RAMCloud::APPEND_T append);
void* operator new[](size_t numBytes, RAMCloud::Buffer* buffer,
                     RAMCloud::APPEND_T append);
void* operator new(size_t numBytes, RAMCloud::Buffer* buffer,
                   RAMCloud::CHUNK_T chunk);
void* operator new[](size_t numBytes, RAMCloud::Buffer* buffer,
                     RAMCloud::CHUNK_T chunk);
void* operator new(size_t numBytes, RAMCloud::Buffer* buffer,
                   RAMCloud::MISC_T misc);
void* operator new[](size_t numBytes, RAMCloud::Buffer* buffer,
                     RAMCloud::MISC_T misc);

namespace RAMCloud {


/**
 * This class manages a logically linear array of bytes, which is implemented as
 * discontiguous chunks in memory. This class exists so that we can avoid copies
 * between the multiple layers of the RAMCloud system, by passing the Buffer
 * associated with memory regions instead of copying the regions themselves. It
 * also lets us build up a logical byte array piecewise so that multiple layers
 * can contribute chunks without copies.
 *
 * Buffers also provide a special-purpose memory allocator for memory that is
 * freed at the time of the Buffer's destruction. This is useful because
 * functions that add chunks to Buffers often return before the Buffer is
 * destroyed, so they need to allocate the memory for these chunks somewhere
 * other than their stacks, and they need to ensure that memory is later freed.
 * The Buffer's memory allocator provides a convenient way to handle this.
 *
 * There are two ways to add data to a Buffer:
 *  - To store the data inside the Buffer, use
 *    <tt>MyStruct* m = new(buffer, PREPEND) MyStruct;</tt> or
 *    <tt>MyStruct* m = new(buffer, APPEND) MyStruct;</tt>.
 *    This memory will be deallocated at the time the buffer is destroyed.
 *  - To reference data that is external to the Buffer, use
 *    #Buffer::Chunk::prependToBuffer(), #Buffer::Chunk::appendToBuffer(), or
 *    similar methods of a derivative class.
 */
// TODO(ongaro): Describe remaining interface to the memory allocator.
class Buffer {

    /**
     * A block of memory used by the Buffer's special-purpose memory allocator.
     * This class hides the complex layout that minimizes the number of Chunks
     * required to make up a Buffer.
     *
     * A particular instance of this class uses a fixed amount of space. To
     * manage a variable amount of space, see the wrappers
     * #RAMCloud::Buffer::allocateChunk(),
     * #RAMCloud::Buffer::allocatePrepend(), and
     * #RAMCloud::Buffer::allocateAppend().
     *
     * See also #allocations.
     *
     * Three kinds of data can be stored in an Allocation:
     *  - Chunk instances.
     *  - Prepend data is data that is added to the beginning of the Buffer.
     *  - Append data is data that is added to the end of the Buffer.
     *
     * The Allocation object's memory is laid out as follows, where the arrows
     * represent stacks growing:
     * <pre>
     *  +---------------+-------------------------------+
     *  |    <--prepend | append-->            <--chunk |
     *  +---------------+-------------------------------+
     *  0          prependSize                      totalSize
     * </pre>
     */
    class Allocation {
        friend class BufferAllocationTest;
        friend class BufferTest;

        /**
         * An integer that indexes into #data.
         * This type is used for the tops of the prepend, append, and chunk
         * stacks.
         */
        typedef uint32_t DataIndex;

      public:

        /**
         * A pointer to the next Allocation in the Buffer's #allocations list.
         */
        Allocation* next;

      private:

        /**
         * The byte with the smallest index of #data in use by the prepend
         * stack.
         * The prepend stack grows down from prependSize to 0.
         */
        DataIndex prependTop;

        /**
         * The byte with the largest index of #data in use by the append stack.
         * The append stack grows up from prependSize to the top of chunk
         * stack.
         */
        DataIndex appendTop;

        /**
         * The byte with the smallest index of #data in use by the chunk stack.
         * The chunk stack grows down from totalSize to the top of the append
         * stack.
         */
        DataIndex chunkTop;

      public:
        static Allocation* newAllocation(uint32_t prependSize,
                                         uint32_t totalSize);
        Allocation(uint32_t prependSize, uint32_t totalSize);
        ~Allocation();

        void* allocatePrepend(uint32_t size);
        void* allocateAppend(uint32_t size);
        void* allocateChunk(uint32_t size);

      private:
        /**
         * The memory from which portions are returned by the allocate methods
         * starts here.
         */
        char data[0];

        DISALLOW_COPY_AND_ASSIGN(Allocation);
    };

  public:

    /**
     * A Chunk represents a contiguous subrange of bytes within a Buffer.
     *
     * The Chunk class refers to memory whose lifetime is at least as great as
     * that of the Buffer and does not release this memory. More intelligent
     * derivatives of the Chunk class know how to release the memory they point
     * to and do so as part of the Buffer's destruction.
     *
     * Derived classes must:
     *  - Not declare any public constructors to ensure that all Chunk
     *    instances are allocated with the Buffer's allocator.
     *  - Implement static prependToBuffer() and appendToBuffer() methods that
     *    allocate a new instance with <tt>new(buffer, CHUNK) MyChunkType</tt>
     *    and then call #::prependChunkToBuffer or #::appendChunkToBuffer.
     */
    class Chunk {
      friend class Buffer;
      friend class BufferTest;
      friend class BufferChunkTest;
      friend class BufferIteratorTest;
      friend void* ::operator new(size_t numBytes, Buffer* buffer,
                                  RAMCloud::PREPEND_T prepend);
      friend void* ::operator new(size_t numBytes, Buffer* buffer,
                                  RAMCloud::APPEND_T append);
      friend void* ::operator new(size_t numBytes, Buffer* buffer,
                                  RAMCloud::CHUNK_T chunk);
      friend void* ::operator new(size_t numBytes, Buffer* buffer,
                                  RAMCloud::MISC_T misc);

      protected:
        /**
         * Add a chunk to the front of a buffer.
         * This is the same as Buffer's #prependChunk but is accessible from all
         * Chunk derivatives.
         * \param[in] buffer
         *      The buffer to which to add \a chunk.
         * \param[in] chunk
         *      A pointer to a Chunk or a subclass of Chunk that will be added
         *      to the front of \a buffer.
         */
        static void prependChunkToBuffer(Buffer *buffer, Chunk *chunk) {
            buffer->prependChunk(chunk);
        }

        /**
         * Add a chunk to the end of a buffer.
         * This is the same as Buffer's #appendChunk but is accessible from all
         * Chunk derivatives.
         * \param[in] buffer
         *      The buffer to which to add \a chunk.
         * \param[in] chunk
         *      A pointer to a Chunk or a subclass of Chunk that will be added
         *      to the end of \a buffer.
         */
        static void appendChunkToBuffer(Buffer *buffer, Chunk *chunk) {
            buffer->appendChunk(chunk);
        }

      public:
        /**
         * Add a new, unmanaged memory chunk to front of a Buffer.
         * The Chunk itself will be deallocated automatically in the Buffer's
         * destructor.
         *
         * \param[in] buffer
         *      The buffer to which to prepend the new chunk.
         * \param[in] data
         *      The start of the memory region to which the new chunk refers.
         * \param[in] length
         *      The number of bytes \a data points to.
         * \return
         *      A pointer to the newly constructed Chunk.
         */
        static Chunk* prependToBuffer(Buffer* buffer,
                                      void* data, uint32_t length) {
            Chunk* chunk = new(buffer, CHUNK) Chunk(data, length);
            Chunk::prependChunkToBuffer(buffer, chunk);
            return chunk;
        }

        /**
         * Add a new, unmanaged memory chunk to end of a Buffer.
         * See #prependToBuffer(), which is analogous.
         */
        static Chunk* appendToBuffer(Buffer* buffer,
                                     void* data, uint32_t length) {
            Chunk* chunk = new(buffer, CHUNK) Chunk(data, length);
            Chunk::appendChunkToBuffer(buffer, chunk);
            return chunk;
        }

      protected:
        /**
         * Construct a chunk that does not release the memory it points to.
         * \param[in] data
         *      The start of the memory region to which the new chunk refers.
         * \param[in] length
         *      The number of bytes \a data points to.
         */
        Chunk(void* data, uint32_t length)
            : data(data), length(length), next(NULL) {}

      public:
        /**
         * Destructor that does not release any memory.
         */
        virtual ~Chunk() {
        }

        /**
         * Return whether this instance is of the Chunk type as opposed to some
         * derivative. This is useful in prepending and appending data onto
         * existing chunks, which is safe for instances of class Chunk that
         * have a trivial destructor.
         * \return See above.
         */
        bool isRawChunk() const __attribute__((noinline)) {
            static const Buffer::Chunk rawChunk(NULL, 0);
            // no hacks here, move along...
            return (this->_vptr == rawChunk._vptr);
        }

      protected:
        /**
         * The first byte of data referenced by this Chunk.
         * \warning
         *      This may change during the lifetime of the chunk. Derivative
         *      classes should store a local copy of the original pointer if
         *      they will need it (e.g., in the destructor).
         */
        void* data;

        /**
         * The number of bytes starting at #data for this Chunk.
         * \warning
         *      This may change during the lifetime of the chunk. Derivative
         *      classes should store a local copy of the original length if
         *      they will need it (e.g., in the destructor).
         */
        uint32_t length;

      private:
        /**
         * The next Chunk in the list. This is for Buffer's use.
         */
        Chunk* next;

        DISALLOW_COPY_AND_ASSIGN(Chunk);
    };

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

    Buffer();
    ~Buffer();

    uint32_t peek(uint32_t offset, void** returnPtr);
    void* getRange(uint32_t offset, uint32_t length);
    uint32_t copy(uint32_t offset, uint32_t length, void* dest); // NOLINT
    string toString();

    void truncateFront(uint32_t length);
    void truncateEnd(uint32_t length);

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

  private:
    Chunk* getLastChunk() const;

    /* For operator new's use only. */
    void* allocateChunk(uint32_t size);
    void* allocatePrepend(uint32_t size);
    void* allocateAppend(uint32_t size);
    friend void* ::operator new(size_t numBytes, Buffer* buffer,
                                RAMCloud::PREPEND_T prepend);
    friend void* ::operator new(size_t numBytes, Buffer* buffer,
                                RAMCloud::APPEND_T append);
    friend void* ::operator new(size_t numBytes, Buffer* buffer,
                                RAMCloud::CHUNK_T chunk);
    friend void* ::operator new(size_t numBytes, Buffer* buffer,
                                RAMCloud::MISC_T misc);

    /* For Chunk's use only. */
    void prependChunk(Chunk* newChunk);
    void appendChunk(Chunk* newChunk);

    Allocation* newAllocation(uint32_t minPrependSize, uint32_t minAppendSize);
    void copyChunks(const Chunk* current, uint32_t offset,  // NOLINT
                    uint32_t length, void* dest) const;

    /**
     * The total number of bytes in the logical array represented by this
     * Buffer.
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

    enum {
        /**
         * The minimum size in bytes of the first Allocation instance to be
         * allocated. Must be a power of two.
         */
        INITIAL_ALLOCATION_SIZE = 2048,
    };
    static_assert((INITIAL_ALLOCATION_SIZE &
                   (INITIAL_ALLOCATION_SIZE - 1)) == 0);
    static_assert((INITIAL_ALLOCATION_SIZE >> 3) != 0);

    /**
     * A container for an Allocation that is allocated along with the Buffer.
     * 
     * Since any Buffer is going to need some storage space (at least for its
     * Chunk instances), it makes sense for a Buffer to come with some of that
     * space already. This avoids a malloc call the first time data is
     * prepended or appended to the Buffer.
     *
     * This container is needed to provide the Allocation with some space to
     * manage directly following it.
     */
    struct InitialAllocationContainer {
        InitialAllocationContainer() : allocation(INITIAL_ALLOCATION_SIZE >> 3,
                                                  INITIAL_ALLOCATION_SIZE) {}
        Allocation allocation;
      private:
        // At least INITIAL_ALLOCATION_SIZE bytes of usable space must directly
        // follow the above Allocation.
        char allocationManagedSpace[INITIAL_ALLOCATION_SIZE];
    } initialAllocationContainer;
    static_assert(sizeof(InitialAllocationContainer) ==
                  sizeof(Allocation) + INITIAL_ALLOCATION_SIZE);

    /**
     * A singly-linked list of Allocation objects used by #allocateChunk(),
     * #allocatePrepend(), and #allocateAppend(). Allocation objects are each
     * of a fixed size, and more are created and added to this list as needed.
     */
    Allocation* allocations;

    /**
     * The minimum size in bytes of the next Allocation instance to be
     * allocated. Must be a power of two. This is doubled for each subsequent
     * allocation, per the algorithm in #newAllocation().
     */
    uint32_t nextAllocationSize;

    friend class Iterator;
    friend class BufferTest;  // For CppUnit testing purposes.
    friend class BufferAllocationTest;
    friend class BufferIteratorTest;

    DISALLOW_COPY_AND_ASSIGN(Buffer);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_BUFFER_H
