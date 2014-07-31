/* Copyright (c) 2010-2014 Stanford University
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

#include "Buffer.h"
#include "Memory.h"

namespace RAMCloud {

uint32_t Buffer::allocationLogThreshold = 4000;

/**
 * Default object used to make system calls.
 */
static Syscall defaultSyscall;

/**
 * Used by this class to make all system calls.  In normal production
 * use it points to defaultSyscall; for testing it points to a mock
 * object.
 */
Syscall* Buffer::sys = &defaultSyscall;

/**
 * Constructor for Buffer: the Buffer starts out empty.
 */
Buffer::Buffer()
    : totalLength(0)
    , firstChunk(NULL)
    , lastChunk(NULL)
    , cursorChunk(NULL)
    , cursorOffset(~0)
    , extraAppendBytes(0)
    , allocations()
    , availableLength(sizeof32(internalAllocation) - PREPEND_SPACE)
    , firstAvailable(reinterpret_cast<char*>(internalAllocation)
            + PREPEND_SPACE)
    , totalAllocatedBytes(0)
//  , internalAllocation()   Do not initialize! (expensive & unnecessary)
{
}

/**
 * Deallocate the memory allocated by this Buffer.
 */
Buffer::~Buffer() {
    resetInternal(false);
}

/**
 * Extend a buffer with a contiguous region of storage; the storage is
 * managed internally by the buffer.
 *
 * \param[in] numBytes
 *      The number of bytes to allocate.
 * \return
 *      The newly allocated memory region of size \a numBytes, which will be
 *      automatically deallocated in this Buffer's destructor. Not
 *      necessarily aligned: it is allocated at the end of the logical
 *      buffer.
 */
void*
Buffer::alloc(size_t numBytes)
{
    uint32_t numBytes32 = downCast<uint32_t>(numBytes);

    // The C++ documentation for new says that if numBytes is 0 we should still
    // return a unique pointer (e.g. by allocating a one-byte object).
    // However, this requires an extra test here; in order to save a few ns in
    // this performance-critical routine, the test is omitted for now (5/2014).
    // If a problem situation comes up in the future, we can add the test then.
    assert(numBytes32 != 0);

    if (extraAppendBytes >= numBytes32) {
        // There is extra space at the end of the current last chunk,
        // so we can just allocate the new region there.
        Buffer::Chunk* chunk = lastChunk;
        void* result = chunk->data + chunk->length;
        chunk->length += numBytes32;
        extraAppendBytes -= numBytes32;
        totalLength += numBytes32;
        return result;
    }

    // We're going to have to create a new chunk.  Compute the total
    // amount of space we need, including space for a Chunk object.
    uint32_t totalBytesNeeded = numBytes32 + sizeof32(Buffer::Chunk);

    // Get some space.
    char* firstByte;
    uint32_t allocatedLength;
    if (availableLength >= totalBytesNeeded) {
        // We can just use the existing "extra" space. Take it all
        // for this chunk (the extras may get used later).
        allocatedLength = availableLength;
        availableLength = 0;
        firstByte = firstAvailable;
    } else {
        firstByte = getNewAllocation(totalBytesNeeded, &allocatedLength);
    }

    // Create the new Chunk at the end of the available space.
    allocatedLength -= sizeof32(Buffer::Chunk);
    Buffer::Chunk *chunk = new(firstByte + allocatedLength) Buffer::Chunk(
            firstByte, numBytes32);
    extraAppendBytes = allocatedLength - numBytes32;
    if (lastChunk != NULL) {
        // Not the first chunk in the Buffer.
        lastChunk->next = chunk;
    } else {
        // This is the first chunk in the Buffer.
        firstChunk = chunk;
    }
    lastChunk = chunk;
    totalLength += numBytes32;
    return chunk->data;
}

/**
 * Allocate a contiguous region of memory from the storage managed by the
 * Buffer. The resulting allocation will not be part of the logical
 * buffer.  This method is intended primarily for internal use for
 * allocating metadata structures, but it is also made available externally.
 *
 * \param numBytes
 *      The number of bytes to allocate.
 * \return
 *      The newly allocated memory region of size \a numBytes, which will be
 *      automatically deallocated in this Buffer's destructor. The region
 *      will be 8-byte aligned.
 */
void*
Buffer::allocAux(size_t numBytes)
{
    // Round up the length to a multiple of 8 bytes, to ensure alignment.
    uint32_t numBytes32 = (downCast<uint32_t>(numBytes) + 7) & ~0x7;
    assert(numBytes32 != 0);

    // If there is enough memory at firstAvailable, use that.  Work down
    // from the top, because this memory is guaranteed to be aligned
    // (memory at the bottom may have been used for variable-size chunks).
    if (availableLength >= numBytes32) {
        availableLength -= numBytes32;
        return firstAvailable + availableLength;
    }

    // Next, see if there is extra space at the end of the last chunk.
    if (extraAppendBytes >= numBytes32) {
        extraAppendBytes -= numBytes32;
        return lastChunk->data + lastChunk->length + extraAppendBytes;
    }

    // Must create a new space allocation.
    uint32_t allocatedLength;
    firstAvailable = getNewAllocation(numBytes32, &allocatedLength);

    // Allocate the requested space.
    availableLength = allocatedLength - numBytes32;
    return firstAvailable + availableLength;
}

/**
 * Allocate a contiguous region of memory from internal buffer storage,
 * and make it the first bytes of the logical buffer. If possible, the
 * new region will be contiguous to the previous start of the buffer.
 *
 * \param numBytes
 *      The number of bytes to allocate.
 * \return
 *      The newly allocated memory region of size \a numBytes, which will be
 *      automatically deallocated in this Buffer's destructor.
 */
void*
Buffer::allocPrepend(size_t numBytes)
{
    uint32_t numBytes32 = downCast<uint32_t>(numBytes);

    if (firstChunk == NULL) {
        // Buffer is empty: no difference between append and prepend.
        return alloc(numBytes);
    }
    totalLength += numBytes32;
    cursorOffset = ~0;

    // Fast path: see if we can extend the existing first chunk to add new
    // storage at its beginning.
    char* internalAlloc = reinterpret_cast<char*>(internalAllocation);
    if (((firstChunk->data - PREPEND_SPACE) <= internalAlloc)
            && ((firstChunk->data - numBytes32) >= internalAlloc)) {
        firstChunk->data -= numBytes32;
        firstChunk->length += numBytes32;
        return firstChunk->data;
    }

    // Must create a new chunk.
    char* newSpace = static_cast<char*>(allocAux(sizeof(Chunk) + numBytes32));
    char* data = newSpace + sizeof(Chunk);
    Buffer::Chunk *chunk = new(newSpace) Buffer::Chunk(data, numBytes32);
    chunk->next = firstChunk;
    firstChunk = chunk;
    return data;
}

/*
 * Append to this buffer a range of bytes from another buffer. The append
 * is done virtually, in that the chunks of this buffer will refer to the
 * same data as the chunks from the other buffer. Use this method carefully:
 * it can result in subtle bugs if the source buffer changes after this
 * method has been invoked
 *
 * \param src
 *      Buffer from which data is to be shared. The data referred to by
 *      this buffer (either internal or external) must remain stable for
 *      the lifetime of the current buffer. For example, it may not be
 *      safe to invoke src->reset().
 * \param offset
 *      Index in src of the first byte of data to be added to the current
 *      buffer.
 * \param length
 *      Total number of bytes of data to append.
 */
void
Buffer::appendExternal(Buffer* src, uint32_t offset, uint32_t length)
{
    Iterator it(src, offset, length);
    while (!it.isDone()) {
        appendExternal(it.getData(), it.getLength());
        it.next();
    }
}

/**
 * Given a Chunk object already created by the caller, append it to
 * the Buffer as the new last Chunk. The caller must ensure that the
 * Chunk and its data remain stable until the Buffer is reset.
 *
 * \param chunk
 *      Describes new data to add to the Buffer.
 */
void
Buffer::appendChunk(Chunk* chunk)
{
    chunk->next = NULL;
    if (firstChunk == NULL) {
        firstChunk = chunk;
    } else {
        // Extra space in the old last chunk is no longer useful;
        // save it, unless there's already a larger block of saved space.
        if (extraAppendBytes > availableLength) {
            availableLength = extraAppendBytes;
            firstAvailable = lastChunk->data + lastChunk->length;
        }
        extraAppendBytes = 0;
        lastChunk->next = chunk;
    }
    lastChunk = chunk;
    totalLength += chunk->length;
}

/**
 * Copy a range of bytes from a buffer to an external location.
 * 
 * \param offset
 *      Index within the buffer of the first byte to copy.
 * \param length
 *      Number of bytes to copy.
 * \param dest
 *      Where to copy the bytes: must have room for at least length bytes.
 * 
 * \return
 *      The return value is the actual number of bytes copied, which may be
 *      less than length if the requested range of bytes exceeds the length
 *      of the buffer. 0 is returned if there is no overlap between the
 *      requested range and the actual buffer.
 */
uint32_t
Buffer::copy(uint32_t offset, uint32_t length, void* dest)
{
    if ((offset+length) >= totalLength) {
        if (offset >= totalLength) {
            return 0;
        }
        length = totalLength - offset;
    }

    // Find the chunk containing the first byte to copy.
    if (offset < cursorOffset) {
        cursorOffset = 0;
        cursorChunk = firstChunk;
    }
    while ((offset - cursorOffset) >= cursorChunk->length) {
        cursorOffset += cursorChunk->length;
        cursorChunk = cursorChunk-> next;
    }

    // Each iteration through the following loop copies bytes from one chunk.
    char* out = static_cast<char*>(dest);
    uint32_t bytesLeft = length;
    char* chunkData = cursorChunk->data + (offset - cursorOffset);
    uint32_t bytesThisChunk = cursorChunk->length - (offset - cursorOffset);
    while (1) {
        if (bytesThisChunk > bytesLeft) {
            bytesThisChunk = bytesLeft;
        }
        memcpy(out, chunkData, bytesThisChunk);
        out += bytesThisChunk;
        bytesLeft -= bytesThisChunk;
        if (bytesLeft == 0) {
            break;
        }
        cursorOffset += cursorChunk->length;
        cursorChunk = cursorChunk->next;
        chunkData = cursorChunk->data;
        bytesThisChunk = cursorChunk->length;
    }
    return length;
}

/**
 * Add data specified in a string to the end of a buffer.  This method
 * was designed primarily for use in tests (e.g. to specify network
 * packets).
 *
 * \param s
 *      Describes what to put in the buffer. Consists of one or more
 *      substrings separated by spaces:
 *      - If a substring starts with a digit or "-" it is assumed to
 *        be a decimal number, which is converted to a 4-byte signed
 *        integer in the buffer.
 *      - If a substring starts with "0x" it is assumed to be a
 *        hexadecimal number, which is converted to a 4-byte integer
 *        in the buffer.
 *      - Otherwise the characters of the substring are appended to
 *        the buffer, with an additional null terminating character.
 */
void
Buffer::fillFromString(const char* s) {
    // Note: this method used to clear the buffer before adding the
    // new data, but this breaks some uses, such as when MockTransport
    // calls it (the buffer contains an RPC object in its MISC area,
    // which get overwritten after a reset).
    uint32_t i, length;
    length = downCast<uint32_t>(strlen(s));
    for (i = 0; i < length; ) {
        char c = s[i];
        if ((c == '0') && (s[i+1] == 'x')) {
            // Hexadecimal number
            int value = 0;
            i += 2;
            while (i < length) {
                char c = s[i];
                i++;
                if (c == ' ') {
                    break;
                }
                if (c <= '9') {
                    value = 16*value + (c - '0');
                } else if ((c >= 'a') && (c <= 'f')) {
                    value = 16*value + 10 + (c - 'a');
                } else {
                    value = 16*value + 10 + (c - 'A');
                }
            }
            emplaceAppend<int32_t>(value);
        } else if ((c == '-') || ((c >= '0') && (c <= '9'))) {
            // Decimal number
            int value = 0;
            int sign = (c == '-') ? -1 : 1;
            if (c == '-') {
                sign = -1;
                i++;
            }
            while (i < length) {
                char c = s[i];
                i++;
                if (c == ' ') {
                    break;
                }
                value = 10*value + (c - '0');
            }
            emplaceAppend<int32_t>(value*sign);
        } else {
            // String
            while (i < length) {
                char c = s[i];
                i++;
                if (c == ' ') {
                    break;
                }
                emplaceAppend<char>(c);
            }
            emplaceAppend<char>('\0');
        }
    }
}

/**
 * Allocate another chunk of memory for the internal use of this
 * buffer.  This method is for internal use only by the Buffer
 * class. It handles such issues as deciding whether to allocate
 * more bytes than are currently needed, and recording the allocation
 * so it can be freed later.
 *
 * \param bytesNeeded
 *      Minimum number of bytes needed by the caller. The method
 *      may choose to allocate more bytes than this.
 * \param [out] bytesAllocated
 *      Points to a variable that will be  filled in with the actual
 *      number of bytes allocated
 *
 * \return
 *      The address of the first byte of the newly allocated storage.
 */
char*
Buffer::getNewAllocation(uint32_t bytesNeeded, uint32_t* bytesAllocated)
{
    // Allocate more space than needed, in the hope that this will
    // make it unnecessary to create more allocations in the future.
    // Each new allocation roughly doubles the total amount of storage
    // allocated for the buffer.
    bytesNeeded += sizeof32(internalAllocation) + totalAllocatedBytes;
    bytesNeeded = (bytesNeeded+7) & ~0x7;
    char* newAllocation = static_cast<char*>(Memory::xmalloc(HERE,
            bytesNeeded));
    totalAllocatedBytes += bytesNeeded;
    if (totalAllocatedBytes >= Buffer::allocationLogThreshold) {
        RAMCLOUD_LOG(NOTICE, "buffer has consumed %u bytes of extra storage, "
                "current allocation: %d bytes",
                totalAllocatedBytes, bytesNeeded);
        Buffer::allocationLogThreshold = 2*totalAllocatedBytes;
    }
    if (!allocations) {
        allocations.construct();
    }
    allocations->push_back(newAllocation);
    *bytesAllocated = bytesNeeded;
    return newAllocation;
}

/**
 * Return the number of discontiguous chunks of storage used by the buffer.
 */
uint32_t
Buffer::getNumberChunks()
{
    uint32_t count = 0;
    for (Chunk* chunk = firstChunk; chunk != NULL; chunk = chunk->next) {
        count++;
    }
    return count;
}

/**
 * Make a subrange of the buffer available as contiguous bytes. If this
 * range is currently split across multiple chunks, it gets copied into
 * auxiliary memory associated with the buffer. Two warnings:
 * - You really shouldn't invoke this method on large subranges due to
 *   the high cost of copying; try to find a way to process such ranges
 *   piecewise.
 * - Memory allocated for this method will not be reclaimed until the
 *   buffer is reset or destroyed. Thus, if you call this method many
 *   times, it could accumulate a large amount of auxiliary memory.
 *
 * \param offset
 *      Index within the buffer of the first byte of the desired range.
 * \param length
 *      Number of bytes in the range.
 *
 * \return
 *      A pointer to the first byte of the requested range, or NULL if the
 *      buffer does not contain the desired range. Note: this pointer
 *      becomes invalid if the buffer is reset or destroyed.
 */
void*
Buffer::getRange(uint32_t offset, uint32_t length)
{
    // Try the fast path first: is the desired range available in the
    // current chunk?
    uint32_t offsetInChunk;
    Chunk* chunk = cursorChunk;
    if (offset >= cursorOffset) {
        offsetInChunk = offset - cursorOffset;
        if ((offsetInChunk + length) <= chunk->length) {
            return chunk->data + offsetInChunk;
        }
    } else {
        // The cached info is past the desired point; must start
        // searching at the beginning of the buffer.
        chunk = firstChunk;
        offsetInChunk = offset;
    }

    if ((offset >= totalLength) || ((offset+length) > totalLength)) {
        return NULL;
    }

    // Find the chunk containing the first byte of the range.
    while (offsetInChunk >= chunk->length) {
        offsetInChunk -= chunk->length;
        chunk = chunk->next;
    }
    cursorChunk = chunk;
    cursorOffset = offset - offsetInChunk;

    if ((offsetInChunk + length) <= cursorChunk->length) {
        return cursorChunk->data + offsetInChunk;
    }

    // The desired range is not contiguous. Copy it.
    char* data = static_cast<char*>(allocAux(length));
    copy(offset, length, data);
    return data;
}

/**
 * Find a given byte in the buffer. This method is more efficient than
 * #getRange() or #copy() because no copying is done.
 *
 * \param offset
 *      Index within the buffer of the desired byte.
 * \param[out] returnPtr
 *      *returnPtr is filled in with the address of the byte corresponding
 *      to offset, or NULL if there is no such offset in the buffer.
 *
 * \return
 *      The number of contiguous bytes available at *returnPtr (may be 0).
 */
uint32_t
Buffer::peek(uint32_t offset, void** returnPtr)
{
    if (offset >= totalLength) {
        *returnPtr = NULL;
        return 0;
    }

    // Try the fast path first: is the desired byte in the current chunk?
    uint32_t bytesToSkip;
    Chunk* chunk;
    if (offset >= cursorOffset) {
        bytesToSkip = offset - cursorOffset;
        chunk = cursorChunk;
    } else {
        // The cached info is past the desired point; must start
        // searching at the beginning of the buffer.
        chunk = firstChunk;
        bytesToSkip = offset;
    }

    // Find the chunk containing the first byte of the range.
    while (bytesToSkip >= chunk->length) {
        bytesToSkip -= chunk->length;
        chunk = chunk->next;
    }
    cursorChunk = chunk;
    cursorOffset = offset - bytesToSkip;
    *returnPtr = chunk->data + bytesToSkip;
    return chunk->length - bytesToSkip;
}

/**
 * Given a Chunk object already created by the caller, add it to the
 * beginning of the Buffer as the new first Chunk. The caller must ensure
 * that the Chunk and its data remain stable until the Buffer is reset.
 *
 * \param chunk
 *      Describes new data to add to the Buffer.
 */
void
Buffer::prependChunk(Chunk* chunk)
{
    cursorOffset = ~0;
    chunk->next = firstChunk;
    if (chunk->next == NULL) {
        lastChunk = chunk;
    }
    firstChunk = chunk;
    totalLength += chunk->length;
}

/**
 * Restore the Buffer to its initial pristine state: it will have 0 length,
 * and all existing internal storage for the Buffer will be freed.
 */
void
Buffer::reset()
{
    resetInternal(true);
}

/*
 * Reduce the length of a buffer.
 * 
 * \param newLength
 *      The number of bytes to retain in the buffer; all bytes with offsets
 *      higher than this will be removed from the buffer. Note: this
 *      method does not necessarily free internal storage that had been
 *      dedicated to the removed bytes (unless the buffer is truncated
 *      to zero length).
 */
void
Buffer::truncate(uint32_t newLength)
{
    uint32_t bytesLeft = newLength;
    if (bytesLeft >= totalLength) {
        return;
    }
    if (bytesLeft == 0) {
        reset();
        return;
    }

    // Cancel any promises about extra bytes after the last chunk.  Note:
    // it is possible to optimize this to recover the storage for the removed
    // bytes in some cases, but it's not clear that it's worth the extra
    // complexity.
    if (extraAppendBytes > availableLength) {
        availableLength = extraAppendBytes;
        firstAvailable = lastChunk->data + lastChunk->length;
    }
    extraAppendBytes = 0;

    // Find the last chunk that we will retain.
    Chunk* chunk = firstChunk;
    while (bytesLeft > chunk->length) {
        bytesLeft -= chunk->length;
        chunk = chunk->next;
    }

    // Shorten the last chunk, if needed.
    chunk->length = bytesLeft;

    // Delete all the chunks after this one.
    lastChunk = chunk;
    chunk = chunk->next;
    lastChunk->next = NULL;
    while (chunk != NULL) {
        Chunk* temp = chunk->next;
        chunk->~Chunk();
        chunk = temp;
    }
    totalLength = newLength;

    // Flush cached location information.
    if (cursorOffset >= totalLength) {
        cursorOffset = ~0;
        cursorChunk = NULL;
    }
}

/*
 * Shorten a buffer by removing data at the beginning.
 * 
 * \param bytesToDelete
 *      The number of bytes to remove from the beginning the buffer; the
 *      byte that used to have this offset will now be the first byte
 *      in the logical buffer. Note: this method does not necessarily
 *      free internal storage that had been dedicated to the removed
 *      bytes (unless the buffer is truncated to zero length).
 */
void
Buffer::truncateFront(uint32_t bytesToDelete)
{
    if (bytesToDelete >= totalLength) {
        reset();
        return;
    }
    totalLength -= bytesToDelete;
    cursorChunk = NULL;
    cursorOffset = ~0;

    // Work through the initial chunks, one at a time, until we've
    // deleted the right number of bytes.
    while (1) {
        Chunk* chunk = firstChunk;
        if (bytesToDelete < chunk->length) {
            chunk->length -= bytesToDelete;
            chunk->data += bytesToDelete;
            break;
        }
        bytesToDelete -= chunk->length;
        firstChunk = chunk->next;
        chunk->~Chunk();
    }
}

/**
 * Writes a contiguous block of data from a buffer to a FILE.
 *
 * \param offset
 *      The offset in the Buffer of the first byte to write.
 * \param length
 *      The number of bytes to write to the file. If this is larger
 *      then the number of bytes in the buffer after offset, then
 *      all of the remaining bytes of the buffer are written.
 * \param f
 *      File into which the requested bytes are written.
 *
 * \return
 *      The actual number of bytes written. This will be less than
 *      length if the requested range of bytes overshoots the end of
 *      the Buffer, or if an I/O error occurred (ferror can be used
 *      to determine whether an error occurred). The return value
 *      will be 0 if offset is outside the range of the Buffer.
 */
uint32_t
Buffer::write(uint32_t offset, uint32_t length, FILE* f) {
    if (offset >= totalLength)
        return 0;

    if (offset + length > totalLength)
        length = totalLength - offset;

    Chunk* current = firstChunk;
    while (offset >= current->length) {
        offset -= current->length;
        current = current->next;
    }

    // offset is the physical offset from 'current' at which to start copying.
    // This may be non-zero for the first Chunk but will be 0 for every
    // subsequent Chunk.
    size_t bytesRemaining = length;
    while (bytesRemaining > 0) {
        size_t bytesFromCurrent = current->length - offset;
        bytesFromCurrent = std::min(bytesFromCurrent, bytesRemaining);
        size_t written = sys->fwrite(
                static_cast<const char*>(current->data) + offset,
                1, bytesFromCurrent, f);
        bytesRemaining -= written;
        if (written != bytesFromCurrent) {
            return length - downCast<uint32_t>(bytesRemaining);
        }
        offset = 0;
        current = current->next;
    }
    return length;
}

//------------------------------------------------------
//   Buffer::Iterator methods
//------------------------------------------------------

/**
 * Create an iterator for the entire contents of a Buffer.
 * The iterator starts on the first chunk of the Buffer, so you should use
 * #isDone(), #getData(), and #getLength() before the first call to #next().
 * \param buffer
 *      The Buffer over which to iterate.
 */
Buffer::Iterator::Iterator(const Buffer* buffer)
    : current(buffer->firstChunk)
    , currentData()
    , currentLength()
    , bytesLeft(buffer->totalLength)
{
    if (current != NULL) {
        currentData = current->data;
        currentLength = current->length;
    } else {
        currentData = NULL;
        currentLength = 0;
    }
}

/**
 * Construct an iterator that only walks chunks with data corresponding
 * to a byte range in the Buffer.
 *
 * Any calls to getData(), getLength(), getNumberChunks(), or size()
 * are appropriately adjusted even when iteration end points don't correspond
 * to chunk boundaries.
 *
 * \param buffer
 *      The Buffer over which to iterate.
 * \param offset
 *      The offset into the Buffer that should be returned by the first
 *      call to getData().
 * \param length
 *      The number of bytes to iterate across before the iterator isDone().
 *      Notice if this exceeds the bounds of the buffer then isDone() may occur
 *      before length number of bytes have been iterated over.
 */
Buffer::Iterator::Iterator(const Buffer* buffer, uint32_t offset,
        uint32_t length)
    : current(buffer->firstChunk)
    , currentData()
    , currentLength()
    , bytesLeft()
{
    // Clip offset and length if they are out of range.
    uint32_t bytesToSkip = offset;
    if (offset >= buffer->totalLength) {
        // The iterator's range is empty.
        currentData = 0;
        currentLength = 0;
        return;
    }
    bytesLeft = std::min(length, buffer->totalLength - bytesToSkip);

    // Advance Iterator up to the first chunk with data from the subrange.
    while ((current != NULL) && (bytesToSkip >= current->length)) {
        bytesToSkip -= current->length;
        current = current->next;
    }
    currentData = current->data + bytesToSkip;
    currentLength = current->length - bytesToSkip;
    if (bytesLeft < currentLength) {
        currentLength = bytesLeft;
    }
}

/**
 * Copy constructor for Buffer::Iterator.
 */
Buffer::Iterator::Iterator(const Iterator& other)
    : current(other.current)
    , currentData(other.currentData)
    , currentLength(other.currentLength)
    , bytesLeft(other.bytesLeft)
{
}

/**
 * Destructor for Buffer::Iterator.
 */
Buffer::Iterator::~Iterator()
{
}

/**
 * Assignment for Buffer::Iterator.
 */
Buffer::Iterator&
Buffer::Iterator::operator=(const Iterator& other)
{
    if (&other == this)
        return *this;
    current = other.current;
    currentData = other.currentData;
    currentLength = other.currentLength;
    bytesLeft = other.bytesLeft;
    return *this;
}

/**
 * Count the number of distinct chunks of storage covered by the
 * remaining bytes of this iterator.
 */
uint32_t
Buffer::Iterator::getNumberChunks()
{
    uint32_t bytesLeft = this->bytesLeft;
    if (bytesLeft == 0) {
        return 0;
    }
    if (bytesLeft <= currentLength) {
        return 1;
    }
    bytesLeft -= currentLength;
    Chunk* chunk = current->next;
    uint32_t count = 2;

    while (bytesLeft > chunk->length) {
        bytesLeft -= chunk->length;
        chunk = chunk->next;
        count++;
    }
    return count;
}

/**
 * Advance to the next chunk in the Buffer.
 */
void
Buffer::Iterator::next()
{
    if (bytesLeft > currentLength) {
        bytesLeft -= currentLength;
        current = current->next;
        currentData = current->data;
        currentLength = std::min(current->length, bytesLeft);
    } else {
        bytesLeft = 0;
        currentData = NULL;
        currentLength = 0;
    }
}

}  // namespace RAMCloud
