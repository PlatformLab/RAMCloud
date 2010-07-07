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
#include <algorithm>

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
 * This constructor initializes an empty Buffer.
 */
Buffer::Buffer()
    : totalLength(0), numberChunks(0), chunks(NULL),
      allocations(NULL), bigAllocations(NULL) {
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
            current->~Chunk();
            current = next;
        }
    }

    { // free the list of big allocations
        BigAllocation* current = bigAllocations;
        while (current != NULL) {
            BigAllocation* next;
            next = current->next;
            free(current);
            current = next;
        }
    }

    { // free the list of allocations
        Allocation* current = allocations;
        while (current != NULL) {
            Allocation* next;
            next = current->next;
            delete current;
            current = next;
        }
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
 * destructor.
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
        return allocateBigAllocation(size);
    }
}

/**
 * Allocate space for prepend data that will be deallocated in the Buffer's
 * destructor.
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
        return allocateBigAllocation(size);
}

/**
 * Allocate space for append data that will be deallocated in the Buffer's
 * destructor.
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
        return allocateBigAllocation(size);
}

/**
 * Add a new memory chunk to front of the Buffer. The memory region physically
 * described by the chunk will be added to the logical beginning of the Buffer.
 *
 * \param[in] newChunk
 *      The Chunk describing the memory region to be added. The caller must
 *      arrange for the memory storing this Chunk instance to extend through
 *      the life of this Buffer.
 */
void Buffer::prependChunk(Chunk* newChunk) {
    ++numberChunks;
    totalLength += newChunk->length;

    newChunk->next = chunks;
    chunks = newChunk;
}


/**
 * Adds a new memory chunk to end of the Buffer. The memory region physically
 * described by the chunk will be added to the logical end of the Buffer.
 * See #prependChunk(), which is analogous.
 *
 * \param[in] newChunk
 *      See #prependChunk().
 */
void Buffer::appendChunk(Chunk* newChunk) {
    ++numberChunks;
    totalLength += newChunk->length;

    newChunk->next = NULL;
    Chunk* lastChunk = getLastChunk();
    if (lastChunk == NULL)
        chunks = newChunk;
    else
        lastChunk->next = newChunk;
}

/**
 * Return a range of contiguous bytes at the requested location in the buffer.
 * This is more efficient that #getRange() or #copy(), since a pointer is
 * returned to existing memory (no copy is done).
 *
 * \param[in]   offset     The offset into the logical memory represented by
 *                         this Buffer.
 * \param[out]  returnPtr  A pointer to the first byte of the contiguous bytes
 *                         available at the requested location, or NULL if the
 *                         given offset is invalid. This is returned to the
 *                         caller.
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
 * chunk. This is common code shared by #copy(uint32_t, uint32_t, void*) and
 * #getRange().
 *
 * \param[in] start
 *      The first chunk in the Buffer having data to be copied out. This may
 *      not be \c NULL.
 * \param[in] offset
 *      The physical offset relative to \a start of the first byte to be
 *      copied. The caller must ensure this offset is within the range of the
 *      \a start chunk.
 * \param[in] length
 *      The number of bytes to copy from \a start and possibly subsequent
 *      chunks. The caller must ensure that this does not extend beyond the end
 *      of the Buffer.
 * \param[in] dest
 *      The first byte where to copy the logical memory block. The caller must
 *      make sure that \a dest contains at least \a length bytes.
 */
void
Buffer::copyChunks(const Chunk* start, uint32_t offset, // NOLINT
                   uint32_t length, void* dest) const
{
    assert(start != NULL && offset < start->length);

    const Chunk* current = start;
    uint32_t bytesRemaining = length;
    // offset is the physical offset from 'current' at which to start copying.
    // This may be non-zero for the first Chunk but will be 0 for every
    // subsequent Chunk.

    while (bytesRemaining > 0) {
        uint32_t bytesFromCurrent = std::min(current->length - offset,
                                             bytesRemaining);
        memcpy(dest, static_cast<char*>(current->data) + offset,
               bytesFromCurrent);
        dest = static_cast<char*>(dest) + bytesFromCurrent;
        bytesRemaining -= bytesFromCurrent;
        offset = 0;

        current = current->next;
        // The caller made sure length was within bounds,
        // but I don't trust the caller.
        assert(current != NULL || bytesRemaining == 0);
    }
}

/**
 * Allocates a scratch memory area to be freed when the Buffer is destroyed.
 * \param[in] length  The length of the memory area to allocate.
 * \return A pointer to the newly allocated memory area.
 *         This will never be \c NULL.
 */
void* Buffer::allocateBigAllocation(uint32_t length) {
    void* m = xmalloc(sizeof(BigAllocation) + length);
    BigAllocation* newBigAllocation = static_cast<BigAllocation*>(m);

    newBigAllocation->next = bigAllocations;
    bigAllocations = newBigAllocation;

    return newBigAllocation->data;
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
 * \return  A pointer to the first byte of the requested range. This memory may
 *          be dynamically allocated, in which case it will exist only as long
 *          as the Buffer object exists.
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
        char* data = new(this, MISC) char[length];
        copyChunks(current, offset, length, data);
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

    copyChunks(current, offset, length, dest);
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
            } else if (c == '\0') {
                s.append("/0");
            } else if (c == '\n') {
                s.append("/n");
            } else {
                uint32_t value = c & 0xff;
                snprintf(temp, sizeof(temp), "/x%02x", value);
                s.append(temp);
            }
        }
    }
    return s;
}

/**
 * Remove the first \a length bytes from the Buffer.
 * \param[in] length
 *      The number of bytes to be removed from the beginning of the Buffer.
 *      If this exceeds the size of the Buffer, the Buffer will become empty.
 */
void
Buffer::truncateFront(uint32_t length)
{
    Chunk* current = chunks;
    while (current != NULL) {
        if (current->length <= length) {
            totalLength -= current->length;
            length -= current->length;
            current->length = 0;
            if (length == 0)
                return;
            current = current->next;
        } else {
            totalLength -= length;
            current->data = static_cast<char*>(current->data) + length;
            current->length -= length;
            return;
        }
    }
}

/**
 * Remove the last \a length bytes from the Buffer.
 * \param[in] length
 *      The number of bytes to be removed from the end of the Buffer.
 *      If this exceeds the size of the Buffer, the Buffer will become empty.
 */
void
Buffer::truncateEnd(uint32_t length)
{
    uint32_t truncateAfter = 0;
    if (length < totalLength)
       truncateAfter = totalLength - length;
    Chunk* current = chunks;
    while (current != NULL) {
        if (current->length <= truncateAfter) {
            truncateAfter -= current->length;
            current = current->next;
            if (truncateAfter == 0)
                goto truncate_remaining;
        } else {
            totalLength += truncateAfter - current->length;
            current->length = truncateAfter;
            current = current->next;
            goto truncate_remaining;
        }
    }
    return;
  truncate_remaining:
    while (current != NULL) {
        totalLength -= current->length;
        current->length = 0;
        current = current->next;
    }
}

/**
 * Find the last Chunk of the Buffer's chunks list.
 * \return
 *      The last Chunk of the Buffer's chunk list, or NULL if the buffer is
 *      empty.
 */
// TODO(ongaro): Measure how long getLastChunk takes under real workloads.
// We could easily add a chunksTail pointer to optimize it out.
Buffer::Chunk*
Buffer::getLastChunk() const
{
    Chunk* current = chunks;
    if (current == NULL)
        return NULL;
    while (current->next != NULL)
        current = current->next;
    return current;
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

}  // namespace RAMCloud

/**
 * Allocate a contiguous region of memory and add it to the front of a Buffer.
 *
 * \param[in] numBytes
 *      The number of bytes to allocate.
 * \param[in] buffer
 *      The Buffer in which to prepend the memory.
 * \param[in] prepend
 *      This should be #::RAMCloud::PREPEND_T::PREPEND.
 * \return
 *      The newly allocated memory region of size \a numBytes, which will be
 *      automatically deallocated in this Buffer's destructor.
 */
void*
operator new(size_t numBytes, RAMCloud::Buffer* buffer,
             RAMCloud::PREPEND_T prepend)
{
    using namespace RAMCloud;
    if (numBytes == 0) {
        // We want no visible effects but should return a unique pointer.
        return buffer->allocatePrepend(1);
    }
    char* data = static_cast<char*>(buffer->allocatePrepend(numBytes));
    Buffer::Chunk* firstChunk = buffer->chunks;
    if (firstChunk != NULL && firstChunk->isRawChunk() &&
        data + numBytes == firstChunk->data) {
        // Grow the existing Chunk.
        firstChunk->data = data;
        firstChunk->length += numBytes;
        buffer->totalLength += numBytes;
    } else {
        Buffer::Chunk::prependToBuffer(buffer, data, numBytes);
    }
    return data;
}

/**
 * Allocate a contiguous region of memory and add it to the end of a Buffer.
 *
 * \param[in] numBytes
 *      The number of bytes to allocate.
 * \param[in] buffer
 *      The Buffer in which to append the memory.
 * \param[in] append
 *      This should be #::RAMCloud::APPEND_T::APPEND.
 * \return
 *      The newly allocated memory region of size \a numBytes, which will be
 *      automatically deallocated in this Buffer's destructor.
 */
void*
operator new(size_t numBytes, RAMCloud::Buffer* buffer,
             RAMCloud::APPEND_T append)
{
    using namespace RAMCloud;
    if (numBytes == 0) {
        // We want no visible effects but should return a unique pointer.
        return buffer->allocateAppend(1);
    }
    char* data = static_cast<char*>(buffer->allocateAppend(numBytes));
    Buffer::Chunk* lastChunk = buffer->getLastChunk();
    if (lastChunk != NULL && lastChunk->isRawChunk() &&
        data - lastChunk->length == lastChunk->data) {
        // Grow the existing Chunk.
        lastChunk->length += numBytes;
        buffer->totalLength += numBytes;
    } else {
        // TODO(ongaro): We've already done the work to find lastChunk but are
        // wasting it.
        Buffer::Chunk::appendToBuffer(buffer, data, numBytes);
    }
    return data;
}

/**
 * Allocate a small, contiguous region of memory in a Buffer for a Chunk
 * instance.
 *
 * \param[in] numBytes
 *      The number of bytes to allocate.
 * \param[in] buffer
 *      The Buffer in which to allocate the memory.
 * \param[in] chunk
 *      This should be #::RAMCloud::CHUNK_T::CHUNK.
 * \return
 *      The newly allocated memory region of size \a numBytes, which will be
 *      automatically deallocated in this Buffer's destructor.
 */
void*
operator new(size_t numBytes, RAMCloud::Buffer* buffer,
             RAMCloud::CHUNK_T chunk)
{
    assert(numBytes >= sizeof(RAMCloud::Buffer::Chunk));
    return buffer->allocateChunk(numBytes);
}

/**
 * Allocate a contiguous region of memory in a Buffer.
 *
 * \param[in] numBytes
 *      The number of bytes to allocate.
 * \param[in] buffer
 *      The Buffer in which to allocate the memory.
 * \param[in] misc
 *      This should be #::RAMCloud::MISC_T::MISC.
 * \return
 *      The newly allocated memory region of size \a numBytes, which will be
 *      automatically deallocated in this Buffer's destructor.
 */
void*
operator new(size_t numBytes, RAMCloud::Buffer* buffer,
             RAMCloud::MISC_T misc)
{
    if (numBytes == 0) {
        // We want no visible effects but should return a unique pointer.
        return buffer->allocateAppend(1);
    }
    return buffer->allocateAppend(numBytes);
}

/**
 * Allocate a contiguous region of memory and add it to the front of a Buffer.
 * See #::operator new(size_t, RAMCloud::Buffer*, RAMCloud::PREPEND_T).
 */
void*
operator new[](size_t numBytes, RAMCloud::Buffer* buffer,
               RAMCloud::PREPEND_T prepend)
{
    return operator new(numBytes, buffer, prepend);
}

/**
 * Allocate a contiguous region of memory and add it to the end of a Buffer.
 * See #::operator new(size_t, RAMCloud::Buffer*, RAMCloud::APPEND_T).
 */
void*
operator new[](size_t numBytes, RAMCloud::Buffer* buffer,
               RAMCloud::APPEND_T append)
{
    return operator new(numBytes, buffer, append);
}

/**
 * Allocate a contiguous region of memory in a Buffer for a Chunk instance.
 * See #::operator new(size_t, RAMCloud::Buffer*, RAMCloud::CHUNK_T).
 */
void*
operator new[](size_t numBytes, RAMCloud::Buffer* buffer,
               RAMCloud::CHUNK_T chunk)
{
    return operator new(numBytes, buffer, chunk);
}

/**
 * Allocate a contiguous region of memory in a Buffer.
 * See #::operator new(size_t, RAMCloud::Buffer*, RAMCloud::MISC_T).
 */
void*
operator new[](size_t numBytes, RAMCloud::Buffer* buffer,
               RAMCloud::MISC_T misc)
{
    return operator new(numBytes, buffer, misc);
}
