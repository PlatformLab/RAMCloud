/* Copyright (c) 2010-2014 Stanford University
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
#include "Tub.h"

namespace RAMCloud {

/**
 * A Buffer represents a logically linear array of bytes, but allows the
 * array to be stored as a collection of discontiguous chunks of memory.
 * Its purpose is to allow information to be assembled and passed around
 * without having to make copies to maintain contiguity. For example, an
 * incoming network message might be represented with a Buffer, where each
 * chunk holds one packet of the message. This class contains several
 * optimizations to minimize the number of chunks in the Buffer, since these
 * introduce overhead. In many simple cases a Buffer will only contain a
 * single contiguous chunk, which is most efficient.
 *
 * In some cases, the storage for chunks is managed internally by the
 * buffer (about 1KB of storage is available automatically, and additional
 * storage will be malloc-ed if needed). Methods such as alloc, allocAux,
 * emplaceAppend, emplacePrepend, and appendCopy use internal storage.
 *
 * In other cases, the storage for chunks is provided from outside the
 * Buffer (e.g., network packets). Methods such as append incorporate
 * external storage into the Buffer. The Buffer class will notify outside
 * agents when their storage is no longer needed for the Buffer.
 */
class Buffer {
  PUBLIC:
    class Chunk;                // Forward declaration.

    Buffer();
    virtual ~Buffer();

    void* alloc(size_t numBytes);
    void* allocAux(size_t numBytes);

    /**
     * Allocate memory for an object of type T from the storage managed by
     * the buffer, and construct an object in it. The object will *not* be
     * part of the logical buffer. This method is intended primarily for
     * internal use by the Buffer class (e.g. for allocating Chunk objects),
     * but it is available externally as well.
     *
     * \param args
     *      Arguments to pass to T's constructor.
     *
     * \return
     *      The return value is a pointer to an object of type T. The
     *      storage for this object will exist only until the next time the
     *      buffer is reset or destroyed. Note: the destructor for the
     *      object will not be invoked automatically by the Buffer class
     *      when the storage is recycled.
     */
    template<typename T, typename... Args>
    T* allocAux(Args&&... args) {
        void* allocation = allocAux(sizeof(T));
        new(allocation) T(static_cast<Args&&>(args)...);
        return static_cast<T*>(allocation);
    }

    void* allocPrepend(size_t numBytes);

    /**
     * Extend the buffer with a block of external data. The data is not
     * copied; the buffer will refer to the source block, so the caller must
     * ensure that the source data remains intact for the lifetime of the
     * buffer.
     *
     * \param data
     *      Address of first byte of data to append to the buffer.
     * \param numBytes
     *      Number of bytes of data to append.
     */
    inline void
    append(const void* data, uint32_t numBytes)
    {
        // At one point this method was modified to just copy in the data,
        // if it is small, rather than creating a new chunk that refers to
        // external data. In many situations this would improve performance,
        // but in some situations it is essential that the Buffer refer
        // to external data (such as data in the log).
        appendChunk(allocAux<Chunk>(data, numBytes));
    }

    void append(Buffer* src, uint32_t offset = 0, uint32_t length = ~0);
    void appendChunk(Chunk* chunk);

    /**
     * Makes a copy of a block of data in internal storage managed by the
     * buffer, and appends that copy to the end of the buffer.
     *
     * \param data
     *      Address of first byte of data to append to the buffer. Need
     *      not persist once this method returns.
     * \param numBytes
     *      Number of bytes of data to append.
     */
    inline void
    appendCopy(const void* data, uint32_t numBytes)
    {
        memcpy(alloc(numBytes), data, numBytes);
    }

    /**
     * Makes a copy of an object in buffer-managed storage and appends that
     * copy to the end of the buffer.
     *
     * \param  object
     *      An object to append to the buffer. Need not persist once this
     *      method returns.
     */
    template<typename T>
    inline void
    appendCopy(const T* object)
    {
        memcpy(alloc(sizeof(T)), object, sizeof(T));
    }

    uint32_t copy(uint32_t offset, uint32_t length, void* dest); // NOLINT

    /**
     * Extend the buffer with enough space for an object of type T, and
     * construct an object in that space. The object becomes part of the
     * logical buffer.
     *
     * \param args
     *      Arguments to pass to T's constructor.
     *
     * \return
     *      The return value is a new object of type T, which now occupies
     *      the last bytes in the buffer. The object is stored in memory
     *      managed by the buffer, which will exist until the buffer is
     *      reset or destroyed. Note: the destructor for the object will
     *      not be invoked automatically by the Buffer class when the
     *      storage is recycled.
     */
    template<typename T, typename... Args>
    inline T*
    emplaceAppend(Args&&... args) {
        void* allocation = alloc(sizeof(T));
        new(allocation) T(static_cast<Args&&>(args)...);
        return static_cast<T*>(allocation);
    }

    /**
     * Construct a new object of type T (using storage managed by the
     * buffer) and prepend the object at the beginning of the buffer.
     *
     * \param args
     *      Arguments to pass to T's constructor.
     *
     * \return
     *      The return value is a new object of type T, which now occupies
     *      the first bytes in the buffer. The storage for this object will
     *      exist only until the buffer is reset or destroyed. Note: the
     *      destructor for the object will not be invoked automatically by
     *      the Buffer class when the storage is recycled.
     */
    template<typename T, typename... Args>
    inline T*
    emplacePrepend(Args&&... args) {
        void* allocation = allocPrepend(sizeof(T));
        new(allocation) T(static_cast<Args&&>(args)...);
        return static_cast<T*>(allocation);
    }

    void fillFromString(const char* s);
    uint32_t getNumberChunks();

    /**
     * Returns a pointer to an object of a particular type, stored
     * at a particular location in the buffer. If the object is not
     * currently stored contiguously, is copied to make it contiguous.
     * 
     * \param offset
     *      Number of bytes in the buffer preceding the desired object.
     * \return
     *      sizeof(\a T) bytes starting at \a offset, or NULL if the
     *      buffer isn't long enough to hold the requested object.
     */
    template<typename T>
    inline T*
    getOffset(uint32_t offset) {
        return static_cast<T*>(getRange(offset, sizeof(T)));
    }

    void* getRange(uint32_t offset, uint32_t length);

    /**
     * Returns a pointer to an object of a particular type, stored
     * at the beginning of the buffer. If the object is not
     * currently stored contiguously, it is copied to make it contiguous.
     * \return
     *      sizeof(\a T) bytes at the beginning of the Buffer.  If the
     *      Buffer does not contain sizeof(\a T) bytes then NULL is
     *      returned.
     */
    template<typename T>
    inline T*
    getStart() {
        return getOffset<T>(0);
    }

    uint32_t peek(uint32_t offset, void** returnPtr);
    void prependChunk(Chunk* chunk);
    virtual void reset();

    /**
     * Returns the total amount of data stored in the Buffer,
     * in bytes.
     */
    inline uint32_t
    size() const {
        return totalLength;
    }

    void truncate(uint32_t newLength);
    void truncateFront(uint32_t bytesToDelete);

    uint32_t write(uint32_t offset, uint32_t length, FILE* f);

  PRIVATE:
    char* getNewAllocation(uint32_t bytesNeeded, uint32_t* bytesAllocated);

    /**
     * This method implements both the destructor and the reset method.
     * For highest performance, the destructor skips parts of this code.
     * Putting this method here as an in-line allows the compiler to
     * optimize out the parts not needed for the destructor.
     * 
     * \param isReset
     *      True means implement the full reset functionality; false means
     *      implement only the functionality need for the destructor.
     */
    inline void
    resetInternal(bool isReset)
    {
        // Free the chunks.
        Chunk* current = firstChunk;
        while (current != NULL) {
            Chunk* next = current->next;
            current->~Chunk();
            current = next;
        }

        // Free any malloc-ed memory.
        if (allocations) {
            for (uint32_t i = 0; i < allocations->size(); i++) {
                free((*allocations)[i]);
            }
            if (isReset) {
                allocations->clear();
            }
        }

        // Reset state.
        if (isReset) {
            totalLength = 0;
            firstChunk = lastChunk = cursorChunk = NULL;
            cursorOffset = ~0;
            extraAppendBytes = 0;
            availableLength = sizeof(internalAllocation) - PREPEND_SPACE;
            firstAvailable = reinterpret_cast<char*>(internalAllocation)
                    + PREPEND_SPACE;
            totalAllocatedBytes = 0;
        }
    }

    /// Number of bytes currently stored in the buffer (total across
    /// all chunks).
    uint32_t totalLength;

    /// First in list of Chunks for the buffer, or NULL if no Chunks yet.
    Chunk* firstChunk;

    /// Last in list of Chunks for the buffer, or NULL if no Chunks yet.
    Chunk* lastChunk;

    /// The following variables keep a "cursor" into the buffer (info about
    /// a recently-accessed chunk). This information improves performance
    /// by avoiding the need to scan all of the buffer chunks from the
    /// beginning for each request. It works best when the buffer is
    /// scanned sequentially.
    /// during c.
    Chunk* cursorChunk;              // Recently accessed chunk.
    uint32_t cursorOffset;           // Offset within the buffer of the first
                                     // byte in cursorChunk; all ones means
                                     // no cursorChunk.

    /// The last Chunk may have been overallocated, leaving extra space
    /// that allows us to satisfy more "new" requests that will be
    /// contiguous to the existing Chunk. If so, this variable
    /// indicates how many bytes are available; if not, this is 0.
    uint32_t extraAppendBytes;

    /// The variables below describe the storage managed internally as
    /// part of the buffer, e.g. to service alloc and allocAux requests.

    /// If we must dynamically allocate space, this variable keeps
    /// track of all the allocations so they can be freed by reset. This is
    /// a Tub so that we don't have to construct and destroy the vector
    /// unless it is actually used (which is fairly rare).
    Tub<std::vector<void*>> allocations;

    /// In some situations we have extra storage space available that
    /// isn't part of a Chunk. When this happens, the variables below
    /// keep track of that space. The space may be used for future
    /// Chunks.
    uint32_t availableLength;        // Total bytes available.
    char* firstAvailable;            // Address of first available byte.

    /// Keeps track of the total amount of extra memory that has been
    /// allocated for this buffer; used for testing and also for
    /// detecting problem situations where too much memory gets
    /// allocated.
    uint32_t totalAllocatedBytes;

    /// The following variable is used so we can log situations where
    /// excessive allocations occur. If totalAllocatedBytes reaches
    /// this value for a Buffer, a log message gets printed and this
    /// value gets doubled.
    static uint32_t allocationLogThreshold;

    /// This variable provides some initial storage for the Buffer's use;
    /// by including this directly as part of the Buffer, we can usually
    /// get by without having to call malloc, which is expensive. The first
    /// few bytes of this are reserved to allow a bit of additional data
    /// to be prepended to the buffer later (e.g., network packet headers);
    /// if all goes well, this data will be adjacent to the first chunk,
    /// so the prepend won't introduce any additional chunks.
    ///
    /// Note: the type char* is used here in order to force 8-byte alignment
    /// of the array.
    char* internalAllocation[1000/sizeof(char*)]; // NOLINT

    /// How much space to reserve for prepending. This should be
    /// at least large enough for Ethernet, IP, and UDP headers.
    static const int PREPEND_SPACE  = 100;

  PUBLIC:

    /**
     * A Chunk represents a contiguous subrange of bytes within a Buffer.
     * There are two different kinds of chunks:
     * 
     * - Some chunks are allocated by the Buffer to hold the results of
     *   the alloc and emplaceAppend methods.
     * - Some chunks are provided externally (e.g. network packets from a
     *   NIC may be appended to a Buffer using this kind of chunk). The
     *   caller must guarantee that these chunks are stable for the
     *   lifetime of the Buffer.  If external code wishes to be notified
     *   when the Chunk is no longer part of the buffer (e.g. so a network
     *   buffer can be returned to the NIC), it should subclass this class
     *   and define a destructor, which will be invoked when the Chunk is
     *   removed from the buffer.
     */
    class Chunk {
      public:
        Chunk(const void* data, uint32_t length)
            : data(const_cast<char*>(static_cast<const char*>(data)))
            , length(length)
            , next(NULL)
        { }

        virtual ~Chunk() {}

        /// First byte of valid data in this Chunk.
        char* data;

        /// The number of valid bytes currently stored in the Chunk.
        uint32_t length;

        /// Next Chunk in Buffer, or NULL if this is the last Chunk.
        Chunk* next;

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(Chunk);
    };

    /**
     * This class provides a way to iterate over the chunks of a Buffer.
     * This is intended for callers that need to know the precise chunk
     * structure of the buffer, such as a network drivers that feed the
     * Buffer chunks to a NIC in pieces.
     *
     * \warning
     * The Buffer must not be modified during the lifetime of the iterator.
     */
    class Iterator {
      public:
        explicit Iterator(const Buffer* buffer);
        Iterator(const Buffer* buffer, uint32_t offset, uint32_t length);
        Iterator(const Iterator& other);
        ~Iterator();
        Iterator& operator=(const Iterator& other);

        /**
         * Return a pointer to the first byte of the data that is available
         * contiguously at the current iterator position, or NULL if the
         * iteration is complete.
         */
        const void*
        getData() const
        {
            return currentData;
        }

        /**
         * Return the number of bytes of contiguous data available at the
         * current position (and part of the iterator's range), or zero if
         * the iteration is complete.
         */
        uint32_t
        getLength() const
        {
            return currentLength;
        }

        uint32_t getNumberChunks();

        /**
         * Indicate whether the entire range has been iterated.
         * \return
         *      True means there are no more bytes left in the range, and
         *      #next(), #getData(), and #getLength should not be called again.
         *      False means there are more bytes left.
         */
        inline bool
        isDone() const
        {
            return currentLength == 0;
        }

        void next();

        /**
         * Returns the total number bytes left to iterate, including those
         * in the current chunk.
         */
        uint32_t size() {return bytesLeft;}

      PRIVATE:
        /// The current chunk over which we're iterating.  May be NULL.
        Chunk* current;

        /// The first byte of data available at the current iterator
        /// position (NULL means no more bytes are available).
        char* currentData;

        /// The number of bytes of data available at the current
        /// iterator position. 0 means that iteration has finished.
        uint32_t currentLength;

        /// The number of bytes left to iterate, including those at the
        /// current iterator position.
        uint32_t bytesLeft;
    };

    static Syscall* sys;

    friend class Iterator;
    DISALLOW_COPY_AND_ASSIGN(Buffer);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_BUFFER_H
