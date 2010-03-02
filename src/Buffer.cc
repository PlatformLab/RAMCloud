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
          chunks(NULL), totalLen(0) {
    chunks = (Chunk*) xmalloc(sizeof(Chunk) * INITIAL_CHUNK_ARR_SIZE);
    bufRead = NULL;
    bufReadSize = 0;
}

Buffer::~Buffer() {
    // TODO(aravindn): Come up with a better memory deallocation strategy.
    free(chunks);
    free(bufRead);
}

void Buffer::prepend(void* src, size_t size) {
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

void Buffer::append(void* src, size_t size) {
    if (size == 0) return; 
    assert(src);

    // If we have used all of the available chunks, allocate more.
    if (chunksUsed == chunksAvail) allocateMoreChunks();

    Chunk* c = (chunks + chunksUsed);
    c->ptr = src;
    c->size = size;
    ++chunksUsed;
    totalLen += size;
}

size_t Buffer::peek(off_t offset, size_t length, void** returnPtr) {
    if (length == 0) return 0;

    int startChunk = findChunk(offset);
    if (startChunk == -1) return 0;

    off_t offsetInChunk = offset - offsetOfChunk(startChunk);
    *returnPtr = (void *) (((char *) chunks[startChunk].ptr) + offsetInChunk);
    return chunks[startChunk].size - offsetInChunk;
}

size_t Buffer::read(off_t offset, size_t length, void **returnPtr) {
    if (findChunk(offset) < 0 || findChunk(offset) >= chunksUsed) return 0;
    if (offset + length > totalLen) return 0;
    if (length == 0) return 0;

    size_t readRetVal = read(offset, length, returnPtr);
    if (readRetVal == length) return readRetVal;

    size_t bufReadSizeOld = bufReadSize;
    bufReadSize += length;
    bufRead = realloc(bufRead, bufReadSizeOld + readRetVal);
    size_t copyRetVal = copy(offset, length, (char*)bufRead + bufReadSizeOld);
    *returnPtr = (char*) bufRead + bufReadSizeOld;
    return copyRetVal;
}

size_t Buffer::copy(off_t offset, size_t length, void* dest) {
    // Find the chunk which corresponds to the supplied offset.
    int currChunk = findChunk(offset);
    if (currChunk == -1) return 0;
    off_t offsetInChunk = offset - offsetOfChunk(currChunk);

    off_t currOff = offset;
    size_t bytesCopied = 0;
    size_t chunkBytesToCopy;

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

int Buffer::findChunk(off_t offset) {
    off_t currOff = 0;
    for (int i = 0; i < chunksUsed; ++i) {
        if ((off_t) (currOff + chunks[i].size) > offset) return i;
        currOff += chunks[i].size;
    }

    return -1;
}

off_t Buffer::offsetOfChunk(int chunkIndex) {
    if (chunkIndex < 0) return -1;
    if (chunkIndex >= chunksUsed) return -1;
    
    off_t offset = 0;
    for (int i = 0; i < chunkIndex; ++i)
        offset += chunks[i].size;
    return offset;
}

void Buffer::allocateMoreChunks() {
    chunksAvail *= 2;
    chunks = (Chunk *) realloc(chunks, sizeof(Chunk) * chunksAvail);
}

}  // namespace RAMCLoud
