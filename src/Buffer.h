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
 * Unfortunately there are several ways to add data to a buffer. Here's a
 * guide:
 * \li Firstly, use prepend variants to add data to the beginning of a buffer and
 * append variants to add data to the end. If either way works for you, use
 * append variants when the data is potentially large (prepend is sized for
 * smaller headers).
 *
 * \li If you already have the data somewhere that will outlive the Buffer's
 * lifetime, use #prepend(void*, uint32_t) or #append(void*, uint32_t).
 *
 * \li If you already have the data somewhere but need some help to ensure that
 * it will extend through the Buffer's lifetime, use #BUFFER_PREPEND() or
 * #BUFFER_APPEND() with a derivative of the #RAMCloud::Buffer::Chunk class.
 *
 * \li If you'll need to copy the data somewhere and know how big it is in
 * advance, use #prepend(uint32_t) or #append(uint32_t).
 *
 * \li Finally, if you'll need to copy the data somewhere but only have an
 * upper bound on its size, use #allocatePrepend() or #allocateAppend() with
 * the upper bound now. Once you know the actual size,
 * use #prepend(void*, uint32_t) or #append(void*, uint32_t).
 */
// TODO(ongaro): Add a way to get a Buffer with an adjacent Allocation.
class Buffer {

    /**
     * A special-purpose memory allocator for Chunk objects and Buffer data.
     *
     * An instance of this class uses a fixed amount of space. To manage a
     * variable amount of space, see the wrappers
     * #RAMCloud::Buffer::allocateChunk(),
     * #RAMCloud::Buffer::allocatePrepend(), and
     * #RAMCloud::Buffer::allocateAppend().
     *
     * See also #allocations.
     */
    class Allocation {
        friend class BufferAllocationTest;
        friend class BufferTest;

        /**
         * An integer that indexes into #data.
         * This type is used for the stack indexes.
         */
        typedef uint16_t DataIndex;

        enum {
            /**
             * Where the prepend and append stacks start.
             * See the diagram for #data.
             */
            APPEND_START = 256,

            /**
             * The number of bytes of #data.
             * See the diagram for #data.
             */
            TOTAL_SIZE = 2048
        };

        // make sure the width of the stack indexes is big enough
        static_assert(TOTAL_SIZE < (1UL << (8 * sizeof(DataIndex))));

      public:

        /**
         * The next Allocation in the list.
         */
        Allocation* next;

      private:

        /**
         * The prepend stack grows down from #APPEND_START to 0.
         * See the diagram for #data.
         */
        DataIndex prependTop;

        /**
         * The append stack grows up from #APPEND_START to the top of chunk
         * stack.
         * See the diagram for #data.
         */
        DataIndex appendTop;

        /**
         * The chunk stack grows down from #TOTAL_SIZE to the top of append
         * stack.
         * See the diagram for #data.
         */
        DataIndex chunkTop;

        /**
         * The memory from which portions are returned by the allocate methods.
         * The layout of the data array, where the arrows represent stacks
         * growing:
         * <pre>
         *  +---------------+-------------------------------+
         *  |    <--prepend | append-->            <--chunk |
         *  +---------------+-------------------------------+
         *  0         APPEND_START                     TOTAL_SIZE
         * </pre>
         */
        char data[TOTAL_SIZE];

      public:

        Allocation();
        ~Allocation();

        static bool canAllocateChunk(uint32_t size);
        static bool canAllocatePrepend(uint32_t size);
        static bool canAllocateAppend(uint32_t size);

        void* allocateChunk(uint32_t size);
        void* allocatePrepend(uint32_t size);
        void* allocateAppend(uint32_t size);

      private:
        DISALLOW_COPY_AND_ASSIGN(Allocation);
    };

  public:

    /**
     * A Buffer is an ordered collection of Chunks. Each individual chunk
     * represents a physically contiguous region of memory. When taken
     * together, a list of Chunks represents a logically contiguous memory
     * region, i.e., this Buffer.
     *
     * The Chunk class refers to memory that is somehow guaranteed to extend
     * through the lifetime of the Buffer and does not release this memory.
     * More intelligent derivatives of the Chunk class, such as
     * #RAMCloud::Buffer::HeapChunk and #RAMCloud::Buffer::NewChunk, know how
     * to release the memory they point to and do so as part of the Buffer's
     * destruction.
     */
    class Chunk {
      friend class Buffer;
      friend class BufferTest;
      friend class BufferChunkTest;
      friend class BufferIteratorTest;
      public:
        /**
         * Construct a chunk that does not release the memory it points to.
         * \param[in] data    The start of the memory region to which this
         *                    chunk refers.
         * \param[in] length  The number of bytes \a data points to.
         */
        Chunk(void* data, uint32_t length)
            : data(data), length(length), next(NULL) {}

        /**
         * Destructor that does not release any memory.
         */
        virtual ~Chunk() { // omnipotent
            data = NULL;
            length = 0;
            next = NULL;
        }

        void* data;       /// The data represented by this Chunk.
        uint32_t length;  /// The length of this Chunk in bytes.
      private:
        Chunk* next;      /// The next Chunk in the list.
        DISALLOW_COPY_AND_ASSIGN(Chunk);
    };

    /**
     * A derivative of #Chunk that will free() its memory region when it is
     * destroyed.
     */
    class HeapChunk : public Chunk {
      public:
        /**
         * Construct a chunk that frees the memory it points to.
         * \param[in] data    The start of the malloc'ed memory region to which
         *                    this chunk refers.
         * \param[in] length  The number of bytes \a data points to.
         */
        HeapChunk(void* data, uint32_t length)
            : Chunk(data, length) {}

        /**
         * Destructor that frees \c data.
         */
        ~HeapChunk() { // omnipotent
            if (data != NULL)
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
        /**
         * Construct a chunk that deletes the memory it points to.
         * \param[in] data    The start of the memory region that was allocated
         *                    with \c new to which this chunk refers.
         */
        explicit NewChunk(T* data)
            : Chunk(static_cast<void*>(data), sizeof(*data)) {}

        /**
         * Destructor that deletes \c data.
         */
        ~NewChunk() { // omnipotent
            if (data != NULL)
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
    Buffer(void* data, uint32_t length);
    ~Buffer();

    void* allocateChunk(uint32_t size);
    void* allocatePrepend(uint32_t size);
    void* allocateAppend(uint32_t size);

    void prepend(Chunk* newChunk);
    void prepend(void* data, uint32_t length);
    void* prepend(uint32_t length);

    /**
     * Add a new memory chunk to front of a Buffer. The memory region
     * physically described by the chunk will be added to the logical beginning
     * of the Buffer.
     *
     * The Chunk itself will be deallocated automatically in the Buffer's
     * destructor.
     *
     * See #RAMCloud::Buffer for help deciding among the prepend variants.
     *
     * \param[in] buffer       A pointer to the #RAMCloud::Buffer.
     * \param[in] chunkType    The class (#RAMCloud::Buffer::Chunk or a
     *                         derivative) which will be instantiated to
     *                         describe a region of memory.
     * \param[in] ...          Arguments to be passed verbatim to
     *                         \a chunkType's constructor.
     * \return A pointer to the newly constructed instance of \a chunkType.
     */
#define BUFFER_PREPEND(buffer, chunkType, ...) ({ \
    RAMCloud::Buffer* b = (buffer); \
    void* d = b->allocateChunk(sizeof(chunkType)); \
    RAMCloud::Buffer::Chunk* chunk = new(d) chunkType(__VA_ARGS__); \
    b->prepend(chunk); \
    chunk; })

    void append(Chunk* newChunk);
    void append(void* data, uint32_t length);
    void* append(uint32_t length);

    /**
     * Add a new memory chunk to end of a Buffer. The memory region physically
     * described by the chunk will be added to the logical end of the Buffer.
     *
     * See #BUFFER_PREPEND(), which is analogous.
     *
     * \param[in] buffer       See #BUFFER_PREPEND().
     * \param[in] chunkType    See #BUFFER_PREPEND().
     * \param[in] ...          See #BUFFER_PREPEND().
     * \return See #BUFFER_PREPEND().
     */
#define BUFFER_APPEND(buffer, chunkType, ...) ({ \
    RAMCloud::Buffer* b = (buffer); \
    void* d = b->allocateChunk(sizeof(chunkType)); \
    RAMCloud::Buffer::Chunk* chunk = new(d) chunkType(__VA_ARGS__); \
    b->append(chunk); \
    chunk; })

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

    Allocation* newAllocation();
    void copy(const Chunk* current, uint32_t offset,  // NOLINT
              uint32_t length, void* dest);
    void* allocateScratchRange(uint32_t length);

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
     * The linked list of #RAMCloud::Buffer::Allocation objects used by
     * #allocateChunk(), #allocatePrepend(), and #allocateAppend().
     */
    Allocation* allocations;

    /**
     * A singly linked list of extra scratch memory areas that are freed when
     * the Buffer is deallocated. This is used in #getRange() and as a
     * fall-back for #allocateChunk(), #allocatePrepend(), and
     * #allocateAppend().
     */
    ScratchRange* scratchRanges;

    friend class Iterator;
    friend class BufferTest;  // For CppUnit testing purposes.
    friend class BufferAllocationTest;
    friend class BufferIteratorTest;

    DISALLOW_COPY_AND_ASSIGN(Buffer);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_BUFFER_H
