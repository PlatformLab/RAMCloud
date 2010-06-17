/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file
 * Contains the implementation for the RAMCloud::Buffer class.
 */

#include <Buffer.h>

#include <string.h>

namespace RAMCloud {

/**
 * Constructor for Allocation.
 */
Buffer::Allocation::Allocation()
    : next(NULL),
      prependTop(APPEND_START),
      appendTop(APPEND_START),
      chunkTop(TOTAL_SIZE) {
}

/**
 * Destructor for Allocation.
 */
Buffer::Allocation::~Allocation() {
    next = NULL;
    prependTop = 0;
    appendTop = TOTAL_SIZE;
    chunkTop = APPEND_START;
}

/**
 * Returns whether an empty Allocation could allocate space for a chunk of a
 * given size.
 * \param[in] size  The size in bytes of the chunk that may or may not fit in
 *                  an empty Allocation.
 * \return See above.
 */
bool
Buffer::Allocation::canAllocateChunk(uint32_t size) {
    return (size <= TOTAL_SIZE - APPEND_START);
}

/**
 * Returns whether an empty Allocation could allocate space for prepend data of
 * a given size.
 * \param[in] size  The size in bytes of the prepend data that may or may not
 *                  fit in an empty Allocation.
 * \return See above.
 */
bool
Buffer::Allocation::canAllocatePrepend(uint32_t size) {
    return (size <= APPEND_START);
}

/**
 * Returns whether an empty Allocation could allocate space for append data of
 * a given size.
 * \param[in] size  The size in bytes of the append data that may or may not
 *                  fit in an empty Allocation.
 * \return See above.
 */
bool
Buffer::Allocation::canAllocateAppend(uint32_t size) {
    return (size <= TOTAL_SIZE - APPEND_START);
}

/**
 * Try to allocate space for a chunk of a given size.
 * \param[in] size  The size in bytes to allocate for a chunk.
 * \return  A pointer to the allocated space or \c NULL if there is not enough
 *          space in this Allocation.
 */
// TODO(ongaro): Alignment issue?
void*
Buffer::Allocation::allocateChunk(uint32_t size) {
    if (static_cast<DataIndex>(chunkTop - appendTop) < size)
        return NULL;
    chunkTop = static_cast<DataIndex>(chunkTop - size);
    return &data[chunkTop];
}

/**
 * Try to allocate space for prepend data of a given size.
 * \param[in] size  The size in bytes to allocate for prepend data.
 * \return  A pointer to the allocated space or \c NULL if there is not enough
 *          space in this Allocation.
 */
// TODO(ongaro): Alignment issue?
void*
Buffer::Allocation::allocatePrepend(uint32_t size) {
    if (prependTop < size)
        return NULL;
    prependTop = static_cast<DataIndex>(prependTop - size);
    return &data[prependTop];
}

/**
 * Try to allocate space for append data of a given size.
 * \param[in] size  The size in bytes to allocate for append data.
 * \return  A pointer to the allocated space or \c NULL if there is not enough
 *          space in this Allocation.
 */
// TODO(ongaro): Alignment issue?
void*
Buffer::Allocation::allocateAppend(uint32_t size) {
    if (static_cast<DataIndex>(chunkTop - appendTop) < size)
        return NULL;
    char *retVal = &data[appendTop];
    appendTop = static_cast<DataIndex>(appendTop + size);
    return retVal;
}

/**
 * This constructor initializes an empty Buffer.
 */
Buffer::Buffer()
    : totalLength(0), numberChunks(0), chunks(NULL),
      allocations(NULL), scratchRanges(NULL) {
}

/**
 * This constructor initializes a Buffer with a single Chunk.
 *
 * This is equivalent to calling the default constructor, followed by
 * #append(void*, uint32_t).
 *
 * \param[in]  data     The memory region to be added to the new Buffer. The
 *                      caller must arrange for this memory region to extend
 *                      through the life of this Buffer.
 * \param[in]  length   The size, in bytes, of the memory region represented by
 *                      \a data.
 */
Buffer::Buffer(void* data, uint32_t length)
    : totalLength(0), numberChunks(0), chunks(NULL),
      allocations(NULL), scratchRanges(NULL) {
    append(data, length);
}

/**
 * Deallocate the memory allocated by this Buffer.
 */
Buffer::~Buffer() {
    { // free the list of chunks
        Chunk* current = chunks;
        while (current != NULL) {
            Chunk* next;
            next = current->next;
            totalLength -= current->length;
            --numberChunks;
            current->~Chunk();
            current = next;
        }
        chunks = NULL;
        assert(numberChunks == 0);
        assert(totalLength == 0);
    }

    { // free the list of scratch ranges
        ScratchRange* current = scratchRanges;
        while (current != NULL) {
            ScratchRange* next;
            next = current->next;
            free(current);
            current = next;
        }
        scratchRanges = NULL;
    }

    { // free the list of allocations
        Allocation* current = allocations;
        while (current != NULL) {
            Allocation* next;
            next = current->next;
            delete current;
            current = next;
        }
        allocations = NULL;
    }
}

/**
 * Prepend a new Allocation object to the #allocations list.
 * This is used in #allocateChunk(), #allocatePrepend(), and #allocateAppend()
 * when the existing Allocation(s) have run out of space.
 * \return The newly allocated Allocation object.
 */
Buffer::Allocation*
Buffer::newAllocation() {
    Allocation* newAllocation = new Allocation();
    newAllocation->next = allocations;
    allocations = newAllocation;
    return newAllocation;
}

/**
 * Allocate space for a Chunk instance that will be deallocated in the Buffer's
 * destructor. The intent is that you will initialize a Chunk in this space and
 * add it to the Buffer.
 * \warning This probably shouldn't be called outside #BUFFER_PREPEND() and
 * #BUFFER_APPEND().
 * \param[in] size  The size in bytes to allocate for a chunk.
 * \return  A pointer to the allocated space (will never be \c NULL).
 */
// TODO(ongaro): Collect statistics on the distributions of sizes.
void*
Buffer::allocateChunk(uint32_t size) {
    // Common case: allocate size bytes in the latest Allocation.
    // It is perhaps wasteful of space to not try the other Allocations in
    // the list if this fails, but the others are probably close to full
    // anyway.
    if (allocations != NULL) {
        void* data = allocations->allocateChunk(size);
        if (data != NULL)
            return data;
    }

    // Create a new Allocation only if size will fit.
    if (Allocation::canAllocateChunk(size)) {
        return newAllocation()->allocateChunk(size);
    } else {
        // Hopefully this is extremely rare.
        return allocateScratchRange(size);
    }
}

/**
 * Allocate space for prepend data that will be deallocated in the Buffer's
 * destructor. The intent is that you will prepend the returned space to the
 * Buffer. Usually, you'll want #prepend(uint32_t) instead.
 * \param[in] size  The size in bytes to allocate for the prepend data.
 * \return  A pointer to the allocated space (will never be \c NULL).
 */
// TODO(ongaro): Collect statistics on the distributions of sizes.
void*
Buffer::allocatePrepend(uint32_t size) {
    if (allocations != NULL) {
        void* data = allocations->allocatePrepend(size);
        if (data != NULL)
            return data;
    }
    if (Allocation::canAllocatePrepend(size))
        return newAllocation()->allocatePrepend(size);
    else
        return allocateScratchRange(size);
}

/**
 * Allocate space for append data that will be deallocated in the Buffer's
 * destructor. The intent is that you will append the returned space to the
 * Buffer. Usually, you'll want #append(uint32_t) instead.
 * \param[in] size  The size in bytes to allocate for the append data.
 * \return  A pointer to the allocated space (will never be \c NULL).
 */
// TODO(ongaro): Collect statistics on the distributions of sizes.
void*
Buffer::allocateAppend(uint32_t size) {
    if (allocations != NULL) {
        void* data = allocations->allocateAppend(size);
        if (data != NULL)
            return data;
    }
    if (Allocation::canAllocateAppend(size))
        return newAllocation()->allocateAppend(size);
    else
        return allocateScratchRange(size);
}

/**
 * Add a new memory chunk to front of the Buffer. The memory region physically
 * described by the chunk will be added to the logical beginning of the Buffer.
 *
 * \warning #BUFFER_PREPEND() should probably be the only caller of this
 * method, since we want the Buffer to keep all of its chunks allocated
 * together for cache locality. See the other variants of prepend and
 * #BUFFER_PREPEND() instead.
 *
 * See #RAMCloud::Buffer for help deciding among the prepend variants.
 *
 * \param[in]  newChunk   The Chunk describing the memory region to be added.
 *                        The caller must arrange for the memory storing this
 *                        Chunk instance to extend through the life of this
 *                        Buffer.
 */
void Buffer::prepend(Chunk* newChunk) {
    ++numberChunks;
    totalLength += newChunk->length;

    newChunk->next = chunks;
    chunks = newChunk;
}

/**
 * Add a contiguous region of memory to the front of the Buffer. The memory
 * region physically described by the arguments will be added to the logical
 * beginning of the Buffer.
 *
 * This is a convenient wrapper for #BUFFER_PREPEND() with the Chunk class.
 * See #RAMCloud::Buffer for help deciding among the prepend variants.
 *
 *
 * \param[in]  data    The memory region to be added. The caller must arrange
 *                     for this memory to extend through the life of this
 *                     Buffer.
 * \param[in]  length  The size in bytes of the memory region pointed to by
 *                     \a data.
 */
void Buffer::prepend(void* data, uint32_t length) {
    BUFFER_PREPEND(this, Chunk, data, length);
}

/**
 * Allocate a contiguous region of memory and add it to the front of the
 * Buffer. The memory region returned will be added to the logical beginning of
 * the Buffer.
 *
 * See #RAMCloud::Buffer for help deciding among the prepend variants.
 *
 * \param[in]  length  The size in bytes of the memory region to allocate.
 * \return The newly allocated memory region of size \a length, which will be
 *         automatically deallocated in this Buffer's destructor.
 */
void* Buffer::prepend(uint32_t length) {
    void* data = allocatePrepend(length);
    prepend(data, length);
    return data;
}

/**
 * Adds a new memory chunk to end of the Buffer. The memory region physically
 * described by the chunk will be added to the logical end of the Buffer.
 *
 * See #prepend(Chunk*), which is analogous.
 * \param[in]  newChunk   See #prepend(Chunk*).
 */
void Buffer::append(Chunk* newChunk) {
    ++numberChunks;
    totalLength += newChunk->length;

    newChunk->next = NULL;
    if (chunks == NULL) {
        chunks = newChunk;
    } else {
        Chunk* lastChunk = chunks;
        // TODO(ongaro): Measure how long this loop takes under real workloads.
        // We could easily add a chunksTail pointer to optimize it out.
        while (lastChunk->next != NULL)
            lastChunk = lastChunk->next;
        lastChunk->next = newChunk;
    }
}

/**
 * Add a contiguous region of memory to the end of the Buffer. The memory
 * region physically described by the arguments will be added to the logical
 * end of the Buffer.
 *
 * See #prepend(void*, uint32_t), which is analogous.
 *
 * \param[in]  data    See #prepend(void*, uint32_t).
 * \param[in]  length  See #prepend(void*, uint32_t).
 */
void Buffer::append(void* data, uint32_t length) {
    BUFFER_APPEND(this, Chunk, data, length);
}

/**
 * Allocate a contiguous region of memory and add it to the end of the Buffer.
 * The memory region returned will be added to the logical end of the Buffer.
 *
 * See #prepend(uint32_t), which is analogous.
 *
 * \param[in]  length  See #prepend(uint32_t).
 * \return See #prepend(uint32_t).
 */
void* Buffer::append(uint32_t length) {
    void* data = allocateAppend(length);
    append(data, length);
    return data;
}

/**
 * Returns the number of contiguous bytes available at the requested memory
 * region. Also returns a pointer to this memory region. This is more efficient
 * that #getRange() or #copy(), since a pointer is returned to existing memory
 * (no copy is done).
 *
 * \param[in]   offset     The offset into the logical memory represented by
 *                         this Buffer.
 * \param[out]  returnPtr  The pointer to the memory block requested. This is
 *                         returned to the caller.
 * \return  The number of contiguous bytes available starting at \a returnPtr.
 * \retval  0, if the given \a offset is invalid.
 */
uint32_t Buffer::peek(uint32_t offset, void** returnPtr) {
    for (Chunk* current = chunks; current != NULL; current = current->next) {
        if (offset < current->length) {
            *returnPtr = static_cast<char*>(current->data) + offset;
            return (current->length - offset);
        }
        offset -= current->length;
    }
    *returnPtr = NULL;
    return 0;
}

/**
 * Copies logically contiguous data starting from an offset into a given
 * physically contiguous region.
 *
 * If the logical region extends beyond the end of the Buffer, data is copied
 * only up to the end of the buffer.
 *
 * \param[in]  start   The first chunk in the Buffer having data to be copied
 *                     out. This may not be \c NULL.
 * \param[in]  offset  The physical offset relative to \a start of the first
 *                     byte to be copied. The caller must ensure this offset is
 *                     within the range of the \a start chunk.
 * \param[in]  length  The number of bytes to copy from \a start and possibly
 *                     subsequent chunks. The caller must ensure that this does
 *                     not extend beyond the end of the Buffer.
 * \param[in]  dest    The pointer to which we should copy the logical memory
 *                     block. The caller must make sure that \a dest contains at
 *                     least \a length bytes.
 * \return  The actual number of bytes copied. This will be less than length if
 *          the requested range of bytes overshoots the end of the Buffer.
 */
void Buffer::copy(const Chunk* start, uint32_t offset, // NOLINT
                  uint32_t length, void* dest) {
    assert(start != NULL && offset < start->length);

    const Chunk* current = start;

    // offset is the physical offset from 'current' at which to start copying.
    // This may be non-zero for the first Chunk but will be 0 for every
    // subsequent Chunk.

    // length is the logical length left to copy,
    // starting at 'current' + 'offset'.

    while (length > 0) {
        uint32_t bytesToCopy;
        bytesToCopy = current->length - offset;
        if (bytesToCopy > length)
            bytesToCopy = length;
        char* data = static_cast<char*>(current->data);
        memcpy(dest, data + offset, bytesToCopy);
        dest = static_cast<char*>(dest) + bytesToCopy;
        length -= bytesToCopy;
        offset = 0;

        current = current->next;
        // The caller made sure length was within bounds,
        // but I don't trust the caller.
        assert(current != NULL || length == 0);
    }
}

/**
 * Allocates a scratch memory area to be freed when the Buffer is destroyed.
 * \param[in] length  The length of the memory area to allocate.
 * \return A pointer to the newly allocated memory area.
 *         This will never be \c NULL.
 */
void* Buffer::allocateScratchRange(uint32_t length) {
    void* m = xmalloc(sizeof(ScratchRange) + length);
    ScratchRange* newRange = static_cast<ScratchRange*>(m);

    newRange->next = scratchRanges;
    scratchRanges = newRange;

    return newRange->data;
}

/**
 * Make a range of logical memory available as contiguous bytes. If this range
 * is not already contiguous, we copy it into a newly allocated region.
 *
 * Memory allocated by this function will not be deallocated until the Buffer is
 * destructed, so too many calls to this function may eat up a lot of memory.
 *
 * \param[in]  offset  The offset into the logical Buffer.
 * \param[in]  length  The length in bytes of the region we want to obtain.
 * \return  The pointer to the memory region we have made available.
 * \retval  NULL, if the <offset, length> tuple specified an invalid range of
 *          memory.
 */
void* Buffer::getRange(uint32_t offset, uint32_t length) {
    if (length == 0) return NULL;
    if (offset + length > totalLength) return NULL;

    Chunk* current = chunks;
    while (offset >= current->length) {
        offset -= current->length;
        current = current->next;
    }

    if (offset + length <= current->length) { // no need to copy
        char* data = static_cast<char*>(current->data);
        return (data + offset);
    } else {
        void* data = allocateScratchRange(length);
        copy(current, offset, length, data);
        return data;
    }
}

/**
 * Copies the memory block identified by the <offset, length> tuple into the
 * region pointed to by dest.
 *
 * If the <offset, length> memory block extends beyond the end of the logical
 * Buffer, we copy all the bytes from offset up to the end of the Buffer.
 *
 * \param[in]  offset  The offset in the Buffer of the first byte to copy.
 * \param[in]  length  The number of bytes to copy.
 * \param[in]  dest    The pointer to which we should copy the logical memory
 *                     block. The caller must make sure that 'dest' contains at
 *                     least 'length' bytes.
 * \return  The actual number of bytes copied. This will be less than length if
 *          the requested range of bytes overshoots the end of the Buffer.
 * \retval  0, if the given 'offset' is invalid.
 */
uint32_t Buffer::copy(uint32_t offset, uint32_t length,
                      void* dest) {  // NOLINT
    if (chunks == NULL || offset >= totalLength)
        return 0;

    if (offset + length > totalLength)
        length = totalLength - offset;

    Chunk* current = chunks;
    while (offset >= current->length) {
        offset -= current->length;
        current = current->next;
    }

    copy(current, offset, length, dest);
    return length;
}

/**
 * Generate a string describing the contents of the buffer.
 *
 * This method is intended primarily for use in tests.
 *
 * \return A string that describes the contents of the buffer. It
 *         consists of the contents of the various chunks separated
 *         by " | ", with long chunks abbreviated and non-printing
 *         characters converted to something printable.
 */
string Buffer::toString() {
    // The following declaration defines the maximum number of characters
    // to display from each chunk.
    static const uint32_t CHUNK_LIMIT = 20;
    const char *separator = "";
    char temp[20];
    uint32_t chunkLength;
    string s;

    for (uint32_t offset = 0; ; offset += chunkLength) {
        char *chunk;
        chunkLength = peek(offset, reinterpret_cast<void **>(&chunk));
        if (chunkLength == 0)
            break;
        s.append(separator);
        separator = " | ";
        for (uint32_t i = 0; i < chunkLength; i++) {
            if (i >= CHUNK_LIMIT) {
                // This chunk is too big to print in its entirety;
                // just print a count of the remaining characters.
                snprintf(temp, sizeof(temp), "(+%d chars)", chunkLength-i);
                s.append(temp);
                break;
            }

            // In printing out the characters, the goal here is
            // to produce a string that is reasonably readable and has
            // no special characters in it; this makes it easy to cut
            // and paste from test output to an "expected results" string
            // without having to add additional quote characters, etc.
            // That's why we print "/n" instead of "\n", for example.
            char c = chunk[i];
            if ((c >= 0x20) && (c < 0x7f)) {
                s.append(&c, 1);
            } else if (c == '\n') {
                s.append("/n");
            } else {
                uint32_t value = c & 0xff;
                snprintf(temp, sizeof(temp), "/x%x", value);
                s.append(temp);
            }
        }
    }
    return s;
}

/**
 * Create an iterator for the contents of a Buffer.
 * The iterator starts on the first chunk of the Buffer, so you should use
 * #isDone(), #getData(), and #getLength() before the first call to #next().
 * \param[in] buffer
 *      The Buffer over which to iterate.
 */
Buffer::Iterator::Iterator(const Buffer& buffer)
    : current(buffer.chunks) {
}

/**
 * Copy constructor for Buffer::Iterator.
 */
Buffer::Iterator::Iterator(const Iterator& other)
    : current(other.current) {
}

/**
 * Destructor for Buffer::Iterator.
 */
Buffer::Iterator::~Iterator() {
    current = NULL;
}

/**
 * Assignment for Buffer::Iterator.
 */
Buffer::Iterator&
Buffer::Iterator::operator=(const Iterator& other) {
    current = other.current;
    return *this;
}

/**
 * Return whether the current chunk is past the end of the Buffer.
 * \return
 *      Whether the current chunk is past the end of the Buffer. If this is
 *      \c true, it is illegal to use #next(), #getData(), and #getLength().
 */
bool Buffer::Iterator::isDone() const {
    return (current == NULL);
}

/**
 * Advance to the next chunk in the Buffer.
 */
void Buffer::Iterator::next() {
    if (current != NULL)
        current = current->next;
}

/**
 * Return the data pointer at the current chunk.
 */
void* Buffer::Iterator::getData() const {
    assert(current != NULL);
    return current->data;
}

/**
 * Return the length of the data at the current chunk.
 */
uint32_t Buffer::Iterator::getLength() const {
    assert(current != NULL);
    return current->length;
}

}  // namespace RAMCLoud
