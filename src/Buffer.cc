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

Buffer::Buffer() : chunksUsed(0), chunksAvail(INITIAL_CHUNK_ARR_SIZE),
                   chunks(NULL), totalLen(0), bufRead(NULL), bufReadSize(0) {
    chunks = (Chunk*) xmalloc(sizeof(Chunk) * INITIAL_CHUNK_ARR_SIZE);
    bufRead = NULL;
    bufReadSize = 0;
}

Buffer::~Buffer() {
    // TODO(aravindn): Come up with a better memory deallocation strategy.
    free(chunks);
    free(bufRead);
}

void Buffer::prepend(void* src, uint32_t size) {
    if (size == 0) return;
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

void Buffer::append(void* src, uint32_t size) {
    if (size == 0) return; 
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

uint32_t Buffer::peek(uint32_t offset, uint32_t length, void** returnPtr) {
    if (length == 0) return 0;

    uint32_t startChunk = findChunk(offset);
    if (startChunk >= chunksUsed) return 0;

    uint32_t offsetInChunk = offset - offsetOfChunk(startChunk);
    *returnPtr = (void *) (((char *) chunks[startChunk].ptr) + offsetInChunk);
    return chunks[startChunk].size - offsetInChunk;
}

uint32_t Buffer::read(uint32_t offset, uint32_t length, void **returnPtr) {
    if (findChunk(offset) >= (uint32_t) chunksUsed) return 0;
    if (length == 0) return 0;

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

uint32_t Buffer::copy(uint32_t offset, uint32_t length, void* dest) {
    // Find the chunk which corresponds to the supplied offset.
    uint32_t currChunk = findChunk(offset);
    if (currChunk >= chunksUsed) return 0;
    uint32_t offsetInChunk = offset - offsetOfChunk(currChunk);

    uint32_t currOff = offset;
    uint32_t bytesCopied = 0;
    uint32_t chunkBytesToCopy;

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

uint32_t Buffer::findChunk(uint32_t offset) {
    uint32_t currOff = 0;
    uint32_t i;

    for (i = 0; i < chunksUsed; ++i) {
        if ((uint32_t) (currOff + chunks[i].size) > offset) return i;
        currOff += chunks[i].size;
    }

    return i;
}

uint32_t Buffer::offsetOfChunk(uint32_t chunkIndex) {
    if (chunkIndex >= chunksUsed) return totalLen;
    
    uint32_t offset = 0;
    for (uint32_t i = 0; i < chunkIndex; ++i)
        offset += chunks[i].size;
    return offset;
}

void Buffer::allocateMoreChunks() {
    chunksAvail *= 2;
    chunks = (Chunk *) realloc(chunks, sizeof(Chunk) * chunksAvail);
}

}  // namespace RAMCLoud
