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
// TODO(ongaro): Add a way to get a Buffer with an adjacent Allocation.
class Buffer {

    /**
     * A block of memory used by the Buffer's special-purpose memory allocator.
     * This class hides the complex layout that minimizes the number of Chunks
     * required to make up a Buffer.
     *
     * An instance of this class uses a fixed amount of space. To manage a
     * variable amount of space, see the wrappers
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
     *  0         APPEND_START                     TOTAL_SIZE
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
        typedef uint16_t DataIndex;

        enum {
            /**
             * Where the prepend and append stacks start.
             * TODO(ongaro): Make this variable.
             */
            APPEND_START = 256,

            /**
             * The number of bytes of #data.
             * TODO(ongaro): Make this variable.
             */
            TOTAL_SIZE = 2048
        };

        // make sure the width of the stack indexes is big enough
        static_assert(TOTAL_SIZE < (1UL << (8 * sizeof(DataIndex))));

      public:

        /**
         * A pointer to the next Allocation in the Buffer's #allocations list.
         */
        Allocation* next;

      private:

        /**
         * The byte with the smallest index of #data in use by the prepend
         * stack.
         * The prepend stack grows down from #APPEND_START to 0.
         */
        DataIndex prependTop;

        /**
         * The byte with the largest index of #data in use by the append stack.
         * The append stack grows up from #APPEND_START to the top of chunk
         * stack.
         */
        DataIndex appendTop;

        /**
         * The byte with the smallest index of #data in use by the chunk stack.
         * The chunk stack grows down from #TOTAL_SIZE to the top of the append
         * stack.
         */
        DataIndex chunkTop;

        /**
         * The memory from which portions are returned by the allocate methods.
         */
        char data[TOTAL_SIZE];

      public:

        Allocation();
        ~Allocation();

        static bool canAllocatePrepend(uint32_t size);
        static bool canAllocateAppend(uint32_t size);
        static bool canAllocateChunk(uint32_t size);

        void* allocatePrepend(uint32_t size);
        void* allocateAppend(uint32_t size);
        void* allocateChunk(uint32_t size);

      private:
        DISALLOW_COPY_AND_ASSIGN(Allocation);
    };

    /**
     * A dynamic memory region that is freed when the Buffer is deallocated.
     * See #bigAllocations.
     */
    struct BigAllocation {
        /**
         * A pointer to the next BigAllocation in the Buffer's
         * #bigAllocations list.
         */
        BigAllocation* next;
        /**
         * The actual memory starts here.
         */
        char data[0];
      private:
        DISALLOW_COPY_AND_ASSIGN(BigAllocation);
    };


  public:

    /**
     * A Chunk represents a contiguous subrange of bytes within a Buffer.
     *
     * The Chunk class refers to memory whose lifetime is at least as great as
     * that of the Buffer and does not release this memory. More intelligent
     * derivatives of the Chunk class, such as #RAMCloud::Buffer::HeapChunk and
     * #RAMCloud::Buffer::NewChunk, know how to release the memory they point
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
         * The first byte of data referenced by this Chunk.
         */
        void* data;

        /**
         * The number of bytes starting at #data for this Chunk.
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
     * A derivative of #Chunk that will free() its memory region when it is
     * destroyed.
     */
    class HeapChunk : public Chunk {
      public:

        // TODO(ongaro): docs
        static HeapChunk* prependToBuffer(Buffer* buffer,
                                          void* data, uint32_t length) {
            HeapChunk* chunk = new(buffer, CHUNK) HeapChunk(data, length);
            Chunk::prependChunkToBuffer(buffer, chunk);
            return chunk;
        }

        // TODO(ongaro): docs
        static HeapChunk* appendToBuffer(Buffer* buffer,
                                         void* data, uint32_t length) {
            HeapChunk* chunk = new(buffer, CHUNK) HeapChunk(data, length);
            Chunk::appendChunkToBuffer(buffer, chunk);
            return chunk;
        }

      protected:
        /**
         * Construct a chunk that frees the memory it points to.
         * \param[in] data
         *      The start of the malloc'ed memory region to which this chunk
         *      refers.
         * \param[in] length
         *      The number of bytes \a data points to.
         */
        HeapChunk(void* data, uint32_t length)
            : Chunk(data, length) {}

      public:
        /**
         * Destructor that frees \c data.
         */
        ~HeapChunk() {
            free(data);
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(HeapChunk);
    };

    /**
     * A derivative of #Chunk that will delete its memory region when it is
     * destroyed.
     */
    template<typename T>
    class NewChunk : public Chunk {
      public:
        // TODO(ongaro): docs
        static NewChunk<T>* prependToBuffer(Buffer* buffer, T* data) {
            NewChunk<T>* chunk = new(buffer, CHUNK) NewChunk(data);
            Chunk::prependChunkToBuffer(buffer, chunk);
            return chunk;
        }

        // TODO(ongaro): docs
        static NewChunk<T>* appendToBuffer(Buffer* buffer, T* data) {
            NewChunk<T>* chunk = new(buffer, CHUNK) NewChunk(data);
            Chunk::appendChunkToBuffer(buffer, chunk);
            return chunk;
        }

      protected:
        /**
         * Construct a chunk that deletes the memory it points to.
         * \param[in] data
         *      The start of the memory region that was allocated with \c new
         *      to which this chunk refers.
         */
        explicit NewChunk(T* data)
            : Chunk(static_cast<void*>(data), sizeof(*data)) {}

      public:
        /**
         * Destructor that deletes \c data.
         */
        ~NewChunk() {
            delete static_cast<T*>(data);
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(NewChunk);
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

    Allocation* newAllocation();
    void copyChunks(const Chunk* current, uint32_t offset,  // NOLINT
                    uint32_t length, void* dest) const;
    void* allocateBigAllocation(uint32_t length);

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

    /**
     * A singly-linked list of Allocation objects used by #allocateChunk(),
     * #allocatePrepend(), and #allocateAppend(). Allocation objects are fixed
     * size, and more are created and added to this list as needed.
     */
    Allocation* allocations;

    /**
     * A singly-linked list of extra dynamic memory regions that are freed when
     * the Buffer is deallocated. This is used as a fall-back when the Buffer's
     * usual memory allocator can't allocate enough space.
     * TODO(ongaro): Make Allocation variable-sized and get rid of this.
     */
    BigAllocation* bigAllocations;

    friend class Iterator;
    friend class BufferTest;  // For CppUnit testing purposes.
    friend class BufferAllocationTest;
    friend class BufferIteratorTest;

    DISALLOW_COPY_AND_ASSIGN(Buffer);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_BUFFER_H
