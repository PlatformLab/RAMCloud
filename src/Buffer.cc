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
 * \file Contains the implementation for the Buffer class.
 */

#include <Buffer.h>

#include <string.h>

namespace RAMCloud {

/**
 * This constructor intitalizes an empty Buffer.
 */
Buffer::Buffer() : chunksUsed(0), chunksAvail(INITIAL_CHUNK_ARR_SIZE),
                   chunks(NULL), totalLen(0), extraBufs(NULL),
                   extraBufsAvail(0), extraBufsUsed(0) {
    chunks = reinterpret_cast<Chunk*>(
        xmalloc(sizeof(Chunk) * INITIAL_CHUNK_ARR_SIZE));
}

/**
 * Deallocate some or all of the memory used during the life of this Buffer.
 */
Buffer::~Buffer() {
    // TODO(aravindn): Come up with a better memory deallocation strategy.
    if (chunksAvail != INITIAL_CHUNK_ARR_SIZE) free(chunks);
    if (extraBufs != NULL) {
        for (uint32_t i = 0; i < extraBufsUsed; ++i)
            free(extraBufs[i]);
        free(extraBufs);
    }
}

/**
 * Add a new memory chunk to the Buffer. The memory region described logically
 * by the argumetns will be added to the beginning of the Buffer.
 *
 * \param[in]  src     The memory region to be added.
 * \param[in]  length  The size, in bytes, of the memory region represented by
 *                     'src'. If caller tries to add a chunk of size 0, we
 *                     return without performing any operation.
 */
void Buffer::prepend(const void* src, const uint32_t length) {
    if (length == 0) return;
    assert(src);

    if (chunksUsed == chunksAvail) allocateMoreChunks();

    // TODO(aravindn): This is a really slow implementation. Fix this. Maybe use
    // a linked list representation for the chunk array?

    // Right shift the chunks, since we have to add the new Chunk at the front
    // (left).
    for (int i = chunksUsed; i > 0; --i)
        chunks[i] = chunks[i-1];

    Chunk* c = chunks;
    c->data = src;
    c->len = length;
    ++chunksUsed;
    totalLen += length;
}

/**
 * Adds a new memory chunk to the Buffer. The memory region described logically
 * by the arguments will be added to the end of the Buffer.
 *
 * \param[in]  src     The memory region to be added.
 * \param[in]  length  The size, in bytes, of the memory region represented by
 *                     'src'. If the caller tries to add a chunk of size 0, we
 *                     return without performing any operation.
 */
void Buffer::append(const void* src, const uint32_t length) {
    if (length == 0) return;
    assert(src);

    if (chunksUsed == chunksAvail) allocateMoreChunks();

    Chunk* c = (chunks + chunksUsed++);
    c->data = src;
    c->len = length;
    totalLen += length;
}

/**
 * Returns the number of contiguous bytes available at the required memory
 * region. Also returns a pointer to this memory region. This is more efficient
 * that getRange() or copy(), since we return a pointer to existing memory.  We
 * also return the number of contiguous bytes available at the location pointed
 * to by the pointer we return.
 *
 * \param[in]   offset     The offset into the logical buffer represented by
 *                         this Buffer.
 * \param[out]  returnPtr  The pointer to the memory block requested. This is
 *                         returned to the caller.
 * \return  The number of contiguous bytes available at 'returnPtr'.
 * \retval  0, if the given 'offset' is invalid.
 */
uint32_t Buffer::peek(const uint32_t offset, void** returnPtr) {
    uint32_t chunkOffset;
    uint32_t chunkIndex = findChunk(offset, &chunkOffset);
    if (chunkIndex >= chunksUsed) return 0;

    uint32_t offsetInChunk = offset - chunkOffset;
    *returnPtr = (char *) chunks[chunkIndex].data + offsetInChunk;
    return (chunks[chunkIndex].len - offsetInChunk);
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
void* Buffer::getRange(const uint32_t offset, const uint32_t length) {
    if (length == 0) return NULL;
    if (offset + length > totalLen) return NULL;

    uint32_t chunkOffset;
    if (findChunk(offset, &chunkOffset) >= chunksUsed) return NULL;

    void *returnPtr;
    if (peek(offset, &returnPtr) >= length) return returnPtr;

    if (extraBufsUsed == extraBufsAvail) allocateMoreExtraBufs();
    extraBufs[extraBufsUsed] =
            reinterpret_cast<void*>(xmalloc(sizeof(char) * length));
    if (copy(offset, length,
             reinterpret_cast<void*>(extraBufs[extraBufsUsed])) <= 0)
        return NULL;
    return extraBufs[extraBufsUsed++];
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
uint32_t Buffer::copy(const uint32_t offset, const uint32_t length,
                      const void* dest) {
    void *currDest = const_cast<void*>(dest);
    uint32_t chunkOffset;
    uint32_t currChunk = findChunk(offset, &chunkOffset);
    if (currChunk >= chunksUsed) return 0;

    // The offset from which we start copying in the current chunk. For the
    // first chunk, this may not be 0. For all subsequent chunks this is set to
    // 0, since we will start copying from the beginning of that chunk.
    uint32_t offsetInChunk = offset - chunkOffset;

    uint32_t bytesCopied = 0;   // The number of bytes we have copied so far.
    uint32_t chunkBytesToCopy;  // The number of bytes to copy from the current
                                // chunk.

    while (bytesCopied < length && currChunk < chunksUsed) {
        chunkBytesToCopy = chunks[currChunk].len - offsetInChunk;
        if (chunkBytesToCopy > (length - bytesCopied))
            chunkBytesToCopy = (length - bytesCopied);

        memcpy(currDest, reinterpret_cast<const char*>(chunks[currChunk].data) +
               offsetInChunk, chunkBytesToCopy);

        bytesCopied += chunkBytesToCopy;
        currDest = (reinterpret_cast<char*>(currDest) + chunkBytesToCopy);
        ++currChunk;
        offsetInChunk = 0;
    }

    return bytesCopied;
}

/**
 * Find the chunk that corresponds to the supplied offset.
 * 
 * \param[in]   offset       The offset in bytes of the point to identify.
 * \param[out]  chunkOffset  The offset of the beginning of the chunk we
 *                           found. This is returned to the caller.
 * \return  The chunk index of the chunk in which this offset lies.
 * \retval  chunksUsed, if the given offset does not fall in any existing chunk.
 */
uint32_t Buffer::findChunk(const uint32_t offset, uint32_t* chunkOffset) {
    *chunkOffset = 0;

    for (uint32_t i = 0; i < chunksUsed; ++i) {
        if ((uint32_t) (*chunkOffset + chunks[i].len) > offset)
            return i;
        *chunkOffset += chunks[i].len;
    }

    return chunksUsed;
}

/**
 * Grows the chunk array by a factor of 2.
 */
void Buffer::allocateMoreChunks() {
    chunksAvail *= 2;
    chunks = reinterpret_cast<Chunk*>(
        xrealloc(chunks, sizeof(Chunk) * chunksAvail));
}

/**
 * Grows the extraBufs array by a factor of 2, or allocates some initial
 * extraBufs.
 */
void Buffer::allocateMoreExtraBufs() {
    if (extraBufsAvail == 0)
        extraBufsAvail = INITIAL_EXTRA_BUFS_ARR_SIZE;
    else
        extraBufsAvail *= 2;
    extraBufs = reinterpret_cast<void **>(
        xrealloc(extraBufs, sizeof(void *) * extraBufsAvail));
}

}  // namespace RAMCLoud
