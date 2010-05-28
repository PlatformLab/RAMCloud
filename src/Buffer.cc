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
 * This constructor initializes an empty Buffer.
 */
Buffer::Buffer()
    : totalLength(0), numberChunks(0), chunks(NULL), scratchRanges(NULL) {
}

/**
 * This constructor initializes a Buffer with a single Chunk.
 *
 * \param[in]  firstChunk     The memory region to be added to the new Buffer.
 * \param[in]  firstChunkLen  The size, in bytes, of the memory region
 *                            represented by \a firstChunk.
 */
Buffer::Buffer(void *firstChunk, uint32_t firstChunkLen)
    : totalLength(0), numberChunks(0), chunks(NULL), scratchRanges(NULL) {
    prepend(firstChunk, firstChunkLen);
}

/**
 * Deallocate the memory allocated by this Buffer.
 * Regions of memory allocated outside this buffer and passed in to append and
 * prepend must be deallocated separately!
 * TODO(ongaro): Buffers should know how to deallocate memory that's passed in.
 */
Buffer::~Buffer() {
    { // free the list of chunks
        Chunk* current = chunks;
        while (current != NULL) {
            Chunk* next;
            next = current->next;
            totalLength -= current->length;
            --numberChunks;
            delete current;
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
}

/**
 * Add a new memory chunk to front of the Buffer. The memory region physically
 * described by the arguments will be added to the logical beginning of the
 * Buffer.
 *
 * \param[in]  src     The memory region to be added.
 * \param[in]  length  The size, in bytes, of the memory region represented by
 *                     \a src.
 */
void Buffer::prepend(void* src, uint32_t length) {
    assert(src != NULL || length == 0);

    Chunk* newChunk = new Chunk();
    newChunk->data = src;
    newChunk->length = length;

    ++numberChunks;
    totalLength += length;

    newChunk->next = chunks;
    chunks = newChunk;
}

/**
 * Adds a new memory chunk to end of the Buffer. The memory region physically
 * described by the arguments will be added to the logical end of the Buffer.
 *
 * \param[in]  src     The memory region to be added.
 * \param[in]  length  The size, in bytes, of the memory region represented by
 *                     \a src.
 */
void Buffer::append(void* src, uint32_t length) {
    assert(src != NULL || length == 0);

    Chunk* newChunk = new Chunk();
    newChunk->data = src;
    newChunk->length = length;

    ++numberChunks;
    totalLength += length;

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
 * Buffer, we copy all the bytes from offset upto the end of the Buffer, and
 * returns the number of bytes copied.
 *
 * \param[in]  offset  The offset of the first byte to be copied.
 * \param[in]  length  The number of bytes to copy.
 * \param[in]  dest    The pointer to which we should copy the logical memory
 *                     block. The caller must make sure that 'dest' contains at
 *                     least 'length' bytes.
 * \return  The actual number of bytes copied. This will be less than length, if
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
