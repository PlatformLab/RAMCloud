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
 * This constructor initializes all of the variables to their default
 * values. It also allocates memory for some initial chunks, so that we do
 * not have to allocates chunks on each append or prepend operation.
 */
Buffer::Buffer() : chunksUsed(0), chunksAvail(INITIAL_CHUNK_ARR_SIZE),
                   chunks(NULL), totalLen(0), bufRead(NULL), bufReadSize(0) {
    chunks = (Chunk*) xmalloc(sizeof(Chunk) * INITIAL_CHUNK_ARR_SIZE);
}

/**
 * Deallocate some or all of the memory used during the life of this Buffer.
 */
Buffer::~Buffer() {
    // TODO(aravindn): Come up with a better memory deallocation strategy.
    free(chunks);
    free(bufRead);
}

/**
 * Add a new memory chunk to the Buffer. This new chunk is added to the
 * beginning of the chunk list instead of at the end (like in the appeand
 * function).
 *
 * Note: If the caller tries to add a chunk of size 0, it is still a valid
 * op. As long as the pointer is also valid, we add this chunk of size 0 to the
 * list.
 * 
 * \param[in] src The memory region to be added.
 * \param[in] size The size in bytes of the memory region represented by
 *                  buf.
 * \return Returns true or false depending on teh success of the prpepend
 *         op.
 */
void Buffer::prepend(void* src, uint32_t size) {
    assert(src);

    // If we have used all of the available chunks, allocate more.
    if (chunksUsed == chunksAvail) allocateMoreChunks();

    // TODO(aravindn): This is a really slow implementation. Fix this. Maybe use
    // a linked list representation for the chunk array?

    // Right shift the chunks, since we have to add the new Chunk at the front
    // (left).
    for (int i = chunksUsed; i > 0; --i) {
        chunks[i].ptr = chunks[i-1].ptr;
        chunks[i].size = chunks[i-1].size;
    }
    
    Chunk* c = chunks;  // Point to the first (empty) chunk.
    c->ptr = src;
    c->size = size;
    ++chunksUsed;
    totalLen += size;
}

/**
 * Adds a new memory chunk to the Buffer. This new chunk is added to the end of
 * the chunk list instead of at the beginning (like in the prepend function).
 *
 * Note: If the caller tries to add a chunk of size 0, it is still a valid
 * op. As long as the pointer is also valid, we add this chunk of size 0 to the
 * list.
 *
 * \param[in] src The memory region to be added.
 * \param[in] size The size of the memory region represented by buf.
 * \return Returns true or false depending on the success of the append op.
 */
void Buffer::append(void* src, uint32_t size) {
    assert(src);

    // If we have used all of the available chunks, allocate more.
    if (chunksUsed == chunksAvail) allocateMoreChunks();

    // Add the chunk to the end of the chunks array.
    Chunk* c = (chunks + chunksUsed);
    c->ptr = src;
    c->size = size;
    ++chunksUsed;
    totalLen += size;
}

/**
 * Returns the pointer to the buffer at the given offset. It does not copy. The
 * combination of the offset and length parameters represent a logical block of
 * memory that we return. If the block spans multiple chunks, we only return the
 * number of bytes upto the end of the first chunk.
 *
 * \param[in] offset The offset into the logical buffer represented by this
 *                   Buffer.
 * \param[in] length The length of the memory block to return.
 * \param[out] return_ptr The pointer to the memory block requested.
 * \return The number of bytes that region of memory pointed to by
 *         return_ptr has.
 */
uint32_t Buffer::peek(uint32_t offset, uint32_t length, void** returnPtr) {
    uint32_t startChunk = findChunk(offset);
    if (startChunk >= chunksUsed) return 0;

    uint32_t offsetInChunk = offset - offsetOfChunk(startChunk);
    *returnPtr = (void *) (((char *) chunks[startChunk].ptr) + offsetInChunk);
    return (length < chunks[startChunk].size - offsetInChunk) ?
            length : chunks[startChunk].size - offsetInChunk;
}

/**
 * If the logical memory region represented by the <offset, length> tuple does
 * not span any chunk boundaries, this function performs the same functionality
 * as peek.
 *
 * However, if the logical memory region represented by the <offset, length>
 * tuple spans one or more chunk boundaries, then we allocate buffer space which
 * can hold 'length' bytes, and 'copy' the required bytes into this buffer space
 * before returning a pointer to it.
 *
 * \param[in] offset The offset into the logical Buffer.
 * \param[in] length The length in bytes of the region we want to obtain.
 * \param[out] returnPtr The pointer to the memory region that we return.
 * \return The size in bytes of the memory region pointed to by returnPtr.
 */
uint32_t Buffer::read(uint32_t offset, uint32_t length, void **returnPtr) {
    if (findChunk(offset) >= chunksUsed) return 0;

    uint32_t readRetVal = peek(offset, length, returnPtr);
    if (readRetVal == length) return readRetVal;

    if (offset + length > totalLen) return 0;

    uint32_t bufReadSizeOld = bufReadSize;
    bufReadSize += length;
    if (bufRead == NULL) bufRead = malloc(bufReadSize);
    else bufRead = realloc(bufRead, bufReadSizeOld + length);
    uint32_t copyRetVal = copy(offset, length, (char*)bufRead + bufReadSizeOld);
    *returnPtr = (char*) bufRead + bufReadSizeOld;
    return copyRetVal;
}

/**
 * Copies the memory block identified by the <offset, length> tuple into the
 * region pointed to by dest.
 *
 * If the <offset, length> memory block extends beyond the end of the logical
 * Buffer, we copy all the bytes from offset upto the end of the Buffer, and
 * returns the number of bytes copied.
 *
 * \param[in] offset The offset at which the memory block we should copy
 *                   starts.
 * \param[in] length The length of the memory block we should copy.
 * \param[in] dest The pointer to which we should copy the logical memory
 *                 block.
 * \return The actual number of bytes copied.
 */
uint32_t Buffer::copy(uint32_t offset, uint32_t length, void* dest) {
    // Find the chunk which corresponds to the supplied offset.
    uint32_t currChunk = findChunk(offset);
    if (currChunk >= chunksUsed) return 0;

    // The offset from which we start copying in the current chunk. For the
    // first chunk, this may not be 0. For all subsequent chunks this is set to
    // 0, since we will start copying from the beginning of that chunk.
    uint32_t offsetInChunk = offset - offsetOfChunk(currChunk);

    uint32_t currOff = offset;  // The current offset from which we are copying.
    uint32_t bytesCopied = 0;  // The number of bytes we have copied so far.
    uint32_t chunkBytesToCopy;  // The number of bytes to copy from the current
                                // chunk.

    while (bytesCopied < length && currChunk < chunksUsed) {
        if (bytesCopied + chunks[currChunk].size > length)
            chunkBytesToCopy = length - bytesCopied - offsetInChunk;
        else
            chunkBytesToCopy = chunks[currChunk].size - offsetInChunk;

        memcpy(dest, (char *) chunks[currChunk].ptr + offsetInChunk,
               chunkBytesToCopy);

        bytesCopied += chunkBytesToCopy;
        dest = ((char *) dest + chunkBytesToCopy);
        currOff += chunks[currChunk].size;
        ++currChunk;
        offsetInChunk = 0;
    }

    return bytesCopied;
}

/**
 * Find the chunk which corresposnds to the supplied offset.
 * 
 * \param[in] offset The offset in bytes of the point to identify.
 * \return The chunk index of the chunk in which this offset lies.
 */
uint32_t Buffer::findChunk(uint32_t offset) {
    uint32_t currOff = 0;

    for (uint32_t i = 0; i < chunksUsed; ++i) {
        if ((uint32_t) (currOff + chunks[i].size) > offset) return i;
        currOff += chunks[i].size;
    }

    return chunksUsed;
}

/**
 * Returns the chunk's offset from the beginning of the Buffer.
 *
 * \param[in] chunkIndex The index of the chunk whose offset we calculate.
 * \return The offset, in bytes, of the beginning of the chunk.
 */
uint32_t Buffer::offsetOfChunk(uint32_t chunkIndex) {
    uint32_t offset = 0;
    for (uint32_t i = 0; i < chunkIndex && i < chunksUsed; ++i)
        offset += chunks[i].size;
    return offset;
}

/**
 * Grows the chunk array by a factor of 2.
 */
void Buffer::allocateMoreChunks() {
    chunksAvail *= 2;
    chunks = (Chunk *) realloc(chunks, sizeof(Chunk) * chunksAvail);
}

}  // namespace RAMCLoud
