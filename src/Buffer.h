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

#ifndef RAMCLOUD_BUFFER_H
#define RAMCLOUD_BUFFER_H

#include <cstdio>

#include "Common.h"
#include "Syscall.h"

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
  PRIVATE:

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

      PRIVATE:

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
        void reset(uint32_t prependSize, uint32_t totalSize);

      PRIVATE:

        /**
         * Structure padding so that \a data is 8-byte aligned within an
         * Allocation.
         */
        char padding[4];

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
     *    and then call #prependChunkToBuffer or #appendChunkToBuffer.
     */
    class Chunk {
      friend class Buffer;
      friend void* ::operator new(size_t numBytes, Buffer* buffer,
                                  RAMCloud::PREPEND_T prepend);
      friend void* ::operator new(size_t numBytes, Buffer* buffer,
                                  RAMCloud::APPEND_T append);
      friend void* ::operator new(size_t numBytes, Buffer* buffer,
                                  RAMCloud::CHUNK_T chunk);
      friend void* ::operator new(size_t numBytes, Buffer* buffer,
                                  RAMCloud::MISC_T misc);

      PROTECTED:
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
         * destructor, but the memory region is not deallocated.
         *
         * \param[in] buffer
         *      The buffer to which to prepend the new chunk.
         * \param[in] data
         *      The start of the memory region to which the new chunk refers.
         *      This memory must have a lifetime at least as great as
         *      \a buffer.
         * \param[in] length
         *      The number of bytes \a data points to.
         * \return
         *      A pointer to the newly constructed Chunk.
         */
        static Chunk* prependToBuffer(Buffer* buffer,
                                      const void* data, uint32_t length) {
            Chunk* chunk = new(buffer, CHUNK) Chunk(data, length);
            Chunk::prependChunkToBuffer(buffer, chunk);
            return chunk;
        }

        /**
         * Add a new, unmanaged memory chunk to end of a Buffer.
         * See #prependToBuffer(), which is analogous.
         */
        static Chunk* appendToBuffer(Buffer* buffer,
                                     const void* data, uint32_t length) {
            Chunk* chunk = new(buffer, CHUNK) Chunk(data, length);
            Chunk::appendChunkToBuffer(buffer, chunk);
            return chunk;
        }

        /**
         * Prepend unmanaged memory chunks from one Buffer to another.
         * This is effectively a means of copying data from one Buffer to
         * another, without actually moving the underlying bytes.
         *
         * This method should only be called on Buffers that contain
         * unmanaged memory (this class will not ensure that memory
         * prepended to the destination Buffer will remain valid if the
         * source Buffer owned it and was destroyed).
         *
         * \param[out] destination
         *      Buffer to prepend unmanaged memory to.
         * \param[in] source
         *      Buffer containing the memory we wish to prepend to the
         *      destination.
         * \param[in] offset
         *      Byte offset within the source Buffer to begin prepend
         *      memory from.
         * \param[in] length
         *      Number of bytes to prepend from the source Buffer.
         */
        static void prependToBuffer(Buffer* destination,
                                    Buffer* source,
                                    uint32_t offset,
                                    uint32_t length)
        {
            Buffer::Iterator it(*source, offset, length);
            uint32_t sourceChunks = it.getNumberChunks();

            // We need to prepend chunks in reverse order, otherwise if we
            // prepend <A, B, C>, we'll end up with <C, B, A>.
            struct {
                const void* data;
                uint32_t length;
            } sourceData[sourceChunks];

            for (uint32_t i = 0; i < sourceChunks; i++) {
                sourceData[i].data = it.getData();
                sourceData[i].length = it.getLength();
                it.next();
            }

            for (uint32_t i = 0; i < sourceChunks; i++) {
                destination->prepend(sourceData[sourceChunks - 1 - i].data,
                                     sourceData[sourceChunks - 1 - i].length);
            }
        }

        /**
         * Append unmanaged memory chunks from one Buffer to another.
         * This is effectively a means of copying data from one Buffer to
         * another, without actually moving the underlying bytes.
         *
         * This method should only be called on Buffers that contain
         * unmanaged memory (this class will not ensure that memory
         * appended to the destination Buffer will remain valid if the
         * source Buffer owned it and was destroyed).
         *
         * \param[out] destination
         *      Buffer to append unmanaged memory to.
         * \param[in] source
         *      Buffer containing the memory we wish to append to the
         *      destination.
         * \param[in] offset
         *      Byte offset within the source Buffer to begin append 
         *      memory from.
         * \param[in] length
         *      Number of bytes to append from the source Buffer.
         */
        static void appendToBuffer(Buffer* destination,
                                   Buffer* source,
                                   uint32_t offset,
                                   uint32_t length)
        {
            Buffer::Iterator it(*source, offset, length);
            while (!it.isDone()) {
                destination->append(it.getData(), it.getLength());
                it.next();
            }
        }

      PROTECTED:
        /**
         * Construct a chunk that does not release the memory it points to.
         * \param[in] data
         *      The start of the memory region to which the new chunk refers.
         * \param[in] length
         *      The number of bytes \a data points to.
         */
        Chunk(const void* data, uint32_t length)
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
            // works with g++
            return (this->_vptr == rawChunk._vptr);
        }

      PROTECTED:
        /**
         * The first byte of data referenced by this Chunk.
         * \warning
         *      This may change during the lifetime of the chunk. Derivative
         *      classes should store a local copy of the original pointer if
         *      they will need it (e.g., in the destructor).
         */
        const void* data;

        /**
         * The number of bytes starting at #data for this Chunk.
         * \warning
         *      This may change during the lifetime of the chunk. Derivative
         *      classes should store a local copy of the original length if
         *      they will need it (e.g., in the destructor).
         */
        uint32_t length;

      PRIVATE:
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
     * It also provides a constructor for easy iteration across the chunks
     * corresponding to a subrange of a Buffer.
     */
    class Iterator {
      public:
        explicit Iterator(const Buffer& buffer);
        Iterator(const Buffer& buffer, uint32_t offset, uint32_t length);
        Iterator(const Iterator& other);
        ~Iterator();
        Iterator& operator=(const Iterator& other);
        bool isDone() const;
        void next();
        const void* getData() const;
        uint32_t getLength() const;
        uint32_t getTotalLength() const;
        uint32_t getNumberChunks() const;

      PRIVATE:
        /**
         * The current chunk over which we're iterating.
         * This starts out as #chunks and ends up at \c NULL.
         */
        Chunk* current;

        /**
         * The offset into the buffer of the the first byte of current.
         */
        uint32_t currentOffset;

        /**
         * The number of bytes starting from offset that should be iterated
         * over before this isDone().  If this value is initialized to a
         * length that together with offset indicates a range that is out
         * bounds then the length will be trimmed so that offset + length
         * extends precisely to the end of the Buffer.
         */
        uint32_t length;

        /**
         * The index in bytes into the buffer that the first call
         * to getData() should return.  If this value is initialized
         * to an offset that is out of range in the constructor then
         * it will be trimmed to match the length of the Buffer.
         */
        uint32_t offset;

        /**
         * An upper bound on the number of chunks the iterator will
         * iterate over.  Calculated lazily in the case of an Iterator
         * that only covers a subrange, see getNumberChunks.
         */
        uint32_t numberChunks;

        /**
         * Used by getNumberChunks to determine if numberChunks needs to
         * be computed still or not.
         */
        uint32_t numberChunksIsValid;
    };

    Buffer();
    ~Buffer();

    /**
     * Convenience function that invokes Buffer::Chunk::appendToBuffer
     * on this Buffer object.
     */
    Chunk*
    append(const void* data, uint32_t length)
    {
        return Buffer::Chunk::appendToBuffer(this, data, length);
    }

    /**
     * Convenience method that invokes Buffer::Chunk::appendToBuffer.
     * on this Buffer object, appending the entire given buffer to it.
     *
     * \param[in] src
     *      The buffer whose data will be appended to this Buffer.
     */
    void
    append(Buffer* src)
    {
        Buffer::Chunk::appendToBuffer(this, src, 0, src->getTotalLength());
    }

    /**
     * Convenience method that invokes Buffer::Chunk::prependToBuffer
     * on this Buffer object.
     */
    Chunk*
    prepend(const void* data, uint32_t length)
    {
        return Buffer::Chunk::prependToBuffer(this, data, length);
    }

    /**
     * Convenience method that invokes Buffer::Chunk::prependToBuffer
     * on this Buffer object, prepending the entire given buffer to it.
     *
     * \param[in] src
     *      The buffer whose data will be prepended to this Buffer.
     */
    void
    prepend(Buffer* src)
    {
        Buffer::Chunk::prependToBuffer(this, src, 0, src->getTotalLength());
    }

    uint32_t peek(uint32_t offset, const void** returnPtr);
    const void* getRange(uint32_t offset, uint32_t length);

    /**
     * Convenience for getRange.
     * \return
     *      sizeof(\a T) bytes starting at \a offset.
     */
    template<typename T> const T* getOffset(uint32_t offset) {
        return static_cast<const T*>(getRange(offset, sizeof(T)));
    }

    /**
     * Convenience for getRange.
     * \return
     *      sizeof(\a T) bytes at the beginning of the Buffer.
     */
    template<typename T> const T* getStart() {
        return getOffset<T>(0);
    }

    uint32_t copy(uint32_t offset, uint32_t length, void* dest); // NOLINT
    uint32_t write(uint32_t offset, uint32_t length, FILE* f);
    void reset();
    void fillFromString(const char* s);

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

  PRIVATE:
    void convertChar(char c, string *out);

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

    /**
     * Pointer to the last chunk in the linked list starting at #chunks.
     * Used for fast appends. NULL if #chunks is empty.
     */
    Chunk* chunksTail;

    enum {
        /**
         * The minimum size in bytes of the first Allocation instance to be
         * allocated. Must be a power of two.
         */
        INITIAL_ALLOCATION_SIZE = 2048,
    };
    static_assert((INITIAL_ALLOCATION_SIZE &
                   (INITIAL_ALLOCATION_SIZE - 1)) == 0,
                  "INITIAL_ALLOCATION_SIZE must be a power of two");
    static_assert((INITIAL_ALLOCATION_SIZE >> 3) != 0,
                  "INITIAL_ALLOCATION_SIZE must be greater than 8");

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
        void reset()
        {
            allocation.reset(INITIAL_ALLOCATION_SIZE >> 3,
                              INITIAL_ALLOCATION_SIZE);
        }
        Allocation allocation;
      PRIVATE:
        // At least INITIAL_ALLOCATION_SIZE bytes of usable space must directly
        // follow the above Allocation.
        char allocationManagedSpace[INITIAL_ALLOCATION_SIZE];
    } initialAllocationContainer;
    static_assert(sizeof(InitialAllocationContainer) ==
                  sizeof(Allocation) + INITIAL_ALLOCATION_SIZE,
                  "InitialAllocationContainer size mistmatch");

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

    static Syscall* sys;

    friend class Iterator;

    DISALLOW_COPY_AND_ASSIGN(Buffer);
};

bool operator==(Buffer::Iterator left, Buffer::Iterator right);

/// See equals.
inline bool operator!=(Buffer::Iterator left, Buffer::Iterator right) {
    return !(left == right);
}

/**
 * Two Buffers are equal if they contain the same logical array of bytes,
 * regardless of their internal representation.
 */
inline bool operator==(const Buffer& left, const Buffer& right) {
    return Buffer::Iterator(left) == Buffer::Iterator(right);
}

/// See equals.
inline bool operator!=(const Buffer& left, const Buffer& right) {
    return !(left == right);
}

}  // namespace RAMCloud

#endif  // RAMCLOUD_BUFFER_H
