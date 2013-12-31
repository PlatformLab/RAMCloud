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

#include <string.h>
#include <algorithm>

#include "Buffer.h"
#include "Memory.h"

namespace RAMCloud {

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
 * Malloc and construct an Allocation.
 * \param[in] prependSize
 *      See constructor.
 * \param[in] totalSize
 *      See constructor. Will round-up to the nearest 8 bytes.
 */
Buffer::Allocation*
Buffer::Allocation::newAllocation(uint32_t prependSize, uint32_t totalSize) {
    totalSize = (totalSize + 7) & ~7U;
    void* a = Memory::xmalloc(HERE, sizeof(Allocation) + totalSize);
    return new(a) Allocation(prependSize, totalSize);
}

/**
 * Constructor for Allocation.
 * The Allocation must be 8-byte aligned.
 * \param[in] prependSize
 *      The number of bytes of the Allocation for prepend data. The rest will
 *      be used for append data and Chunk instances.
 * \param[in] totalSize
 *      The number of bytes of total data the Allocation manages. The
 *      Allocation will assume it can use totalSize bytes directly following
 *      itself (i.e., the caller has allocated sizeof(Allocation) + totalSize).
 *      Must be 8-byte aligned.
 */
Buffer::Allocation::Allocation(uint32_t prependSize, uint32_t totalSize)
    : next(NULL),
      prependTop(prependSize),
      appendTop(prependSize),
      chunkTop(totalSize) {
    assert((reinterpret_cast<uint64_t>(this) & 0x7) == 0);
    assert((totalSize & 0x7) == 0);
    assert(prependSize <= totalSize);
}

/**
 * Destructor for Allocation.
 */
Buffer::Allocation::~Allocation() {
}

/**
 * Reinitialize an Allocation, as if it had been newly constructed.
 * \param[in] prependSize
 *      The number of bytes of the Allocation for prepend data. Same
 *      meaning as constructor argument.
 * \param[in] totalSize
 *      The number of bytes of total data the Allocation manages. Same
 *      meaning as constructor argument.
 */
void
Buffer::Allocation::reset(uint32_t prependSize, uint32_t totalSize)
{
    next = NULL;
    prependTop = prependSize;
    appendTop = prependSize;
    chunkTop = totalSize;
    assert((totalSize & 0x7) == 0);
    assert(prependSize <= totalSize);
}

/**
 * Try to allocate space for prepend data of a given size.
 * \param[in] size  The size in bytes to allocate for prepend data.
 * \return  A pointer to the allocated space or \c NULL if there is not enough
 *          space in this Allocation.
 */
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
void*
Buffer::Allocation::allocateChunk(uint32_t size) {
    size = (size + 7) & ~7U;
    if (static_cast<DataIndex>(chunkTop - appendTop) < size)
        return NULL;
    chunkTop = static_cast<DataIndex>(chunkTop - size);
    assert((chunkTop & 7) == 0);
    return &data[chunkTop];
}

/**
 * This constructor initializes an empty Buffer.
 */
Buffer::Buffer()
    : totalLength(0), numberChunks(0), chunks(NULL), chunksTail(NULL),
      initialAllocationContainer(),
      allocations(&initialAllocationContainer.allocation),
      nextAllocationSize(INITIAL_ALLOCATION_SIZE << 1) {
    assert((reinterpret_cast<uint64_t>(allocations) & 7) == 0);
}

/**
 * Deallocate the memory allocated by this Buffer.
 */
Buffer::~Buffer() {
    reset();
}

/**
 * Truncate the buffer to zero length and free all resources associated
 * with it. The Buffer will end up in the same state it had immediately
 * after initial construction.
 */
void
Buffer::reset() {
    { // free the list of chunks
        Chunk* current = chunks;
        while (current != NULL) {
            Chunk* next;
            next = current->next;
            current->~Chunk();
            current = next;
        }
    }

    { // free the list of allocations
        Allocation* current = allocations;
        // Skip the last allocation in the list (initialAllocationContainer's
        // allocation) since it's not allocated with xmalloc.
        if (current != NULL) {
            while (current->next != NULL) {
                Allocation* next;
                next = current->next;
                free(current);
                current = next;
            }
        }
    }

    // Restore state to what it was at construction time.
    totalLength = 0;
    numberChunks = 0;
    chunks = NULL;
    chunksTail = NULL;
    initialAllocationContainer.reset();
    allocations = &initialAllocationContainer.allocation;
    nextAllocationSize = INITIAL_ALLOCATION_SIZE << 1;
}

/**
 * Prepend a new Allocation object to the #allocations list.
 * This is used in #allocateChunk(), #allocatePrepend(), and #allocateAppend()
 * when the existing Allocation(s) have run out of space.
 * \param[in] minPrependSize
 *      The minimum size in bytes of the new Allocation's prepend data region.
 *      I got lazy, so either this or minAppendSize must be 0.
 * \param[in] minAppendSize
 *      The minimum size in bytes of the new Allocation's append data/Chunk
 *      instance region. I got lazy, so either this or minPrependSize must be 0.
 * \return
 *      The newly allocated Allocation object.
 */
Buffer::Allocation*
Buffer::newAllocation(uint32_t minPrependSize,
                      uint32_t minAppendSize) {
    assert(minPrependSize == 0 || minAppendSize == 0);

    uint32_t totalSize = nextAllocationSize;
    uint32_t prependSize = totalSize >> 3;
    nextAllocationSize <<= 1;

    if (prependSize < minPrependSize) {
        if (totalSize < minPrependSize)
            totalSize = minPrependSize;
        prependSize = minPrependSize;
    }

    if ((totalSize - prependSize) < minAppendSize) {
        if (totalSize < minAppendSize)
            totalSize = minAppendSize;
        prependSize = totalSize - minAppendSize;
    }

    Allocation* newAllocation;
    newAllocation = Allocation::newAllocation(prependSize, totalSize);
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
    void* data;
    if (allocations != NULL) {
        data = allocations->allocateChunk(size);
        if (data != NULL)
            return data;
    }
    data = newAllocation(0, size)->allocateChunk(size);
    assert(data != NULL);
    return data;
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
    void* data;
    if (allocations != NULL) {
        data = allocations->allocatePrepend(size);
        if (data != NULL)
            return data;
    }
    data = newAllocation(size, 0)->allocatePrepend(size);
    assert(data != NULL);
    return data;
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
    void* data;
    if (allocations != NULL) {
        data = allocations->allocateAppend(size);
        if (data != NULL)
            return data;
    }
    data = newAllocation(0, size)->allocateAppend(size);
    assert(data != NULL);
    return data;
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

    if (chunksTail == NULL)
        chunksTail = newChunk;
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
    if (chunksTail == NULL)
        chunks = newChunk;
    else
        chunksTail->next = newChunk;
    chunksTail = newChunk;
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
uint32_t Buffer::peek(uint32_t offset, const void** returnPtr) {
    for (Chunk* current = chunks; current != NULL; current = current->next) {
        if (offset < current->length) {
            *returnPtr = static_cast<const char*>(current->data) + offset;
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
        memcpy(dest, static_cast<const char*>(current->data) + offset,
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
const void* Buffer::getRange(uint32_t offset, uint32_t length) {
    if (length == 0) return NULL;
    if (offset + length > totalLength) return NULL;

    Chunk* current = chunks;
    while (offset >= current->length) {
        offset -= current->length;
        current = current->next;
    }

    if (offset + length <= current->length) { // no need to copy
        const char* data = static_cast<const char*>(current->data);
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

    Chunk* current = chunks;
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
            *(new(this, APPEND) int32_t) = value;
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
            *(new(this, APPEND) int32_t) = value * sign;
        } else {
            // String
            while (i < length) {
                char c = s[i];
                i++;
                if (c == ' ') {
                    break;
                }
                *(new(this, APPEND) char) = c;
            }
            *(new(this, APPEND) char) = 0;
        }
    }
}

/**
 * Remove the first \a length bytes from the Buffer.  This reduces the
 * amount of information visible in the Buffer but does not free
 * memory allocations such as those created with allocateChunk or
 * allocateAppend.
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
            current->data = static_cast<const char*>(current->data)
                    + length;
            current->length -= length;
            return;
        }
    }
}

/**
 * Remove the last \a length bytes from the Buffer.  This reduces the
 * amount of information visible in the Buffer but does not free
 * memory allocations such as those created with allocateChunk or
 * allocateAppend.
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
 * Create an iterator for the contents of a Buffer.
 * The iterator starts on the first chunk of the Buffer, so you should use
 * #isDone(), #getData(), and #getLength() before the first call to #next().
 * \param buffer
 *      The Buffer over which to iterate.
 */
Buffer::Iterator::Iterator(const Buffer& buffer)
    : current(buffer.chunks)
    , currentOffset(0)
    , length(buffer.totalLength)
    , offset(0)
    , numberChunks(buffer.numberChunks)
    , numberChunksIsValid(true)
{
}

/**
 * Construct an iterator that only walks chunks with data corresponding
 * to a byte range in the Buffer.
 *
 * Any calls to getData(), getLength(), getNumberChunks(), or getTotalLength()
 * are appropriately adjusted even when iteration end points don't correspond
 * to chunk boundaries.
 *
 * \param buffer
 *      The Buffer over which to iterate.
 * \param off
 *      The offset into the Buffer which should be returned by the first
 *      call to getData().
 * \param len
 *      The number of bytes to iterate across before the iterator isDone().
 *      Notice if this exceeds the bounds of the buffer then isDone() may occur
 *      before len number of bytes have been iterated over.
 */
Buffer::Iterator::Iterator(const Buffer& buffer,
                           uint32_t off,
                           uint32_t len)
    : current(buffer.chunks)
    , currentOffset(0)
    , length(len)
    , offset(off)
    , numberChunks(0)
    , numberChunksIsValid(false)
{
    // Clip offset and length if they are out of range.
    offset = std::min(off, buffer.totalLength);
    length = std::min(len, buffer.totalLength - offset);

    // Advance Iterator up to the first chunk with data from the subrange.
    while (!isDone() && currentOffset + current->length <= offset)
        next();
}

/**
 * Copy constructor for Buffer::Iterator.
 */
Buffer::Iterator::Iterator(const Iterator& other)
    : current(other.current)
    , currentOffset(other.currentOffset)
    , length(other.length)
    , offset(other.offset)
    , numberChunks(other.numberChunks)
    , numberChunksIsValid(other.numberChunksIsValid)
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
    currentOffset = other.currentOffset;
    offset = other.offset;
    length = other.length;
    numberChunks = other.numberChunks;
    return *this;
}

/**
 * Return whether the current chunk is past the end of the Buffer or
 * requested subrange.
 * \return
 *      Whether the current chunk is past the end of the Buffer or
 *      requested subrange. If this is \c true, it is illegal to
 *      use #next(), #getData(), and #getLength().
 */
bool
Buffer::Iterator::isDone() const
{
    // Done if the current chunk start is beyond end point
    return currentOffset >= offset + length;
}

/**
 * Advance to the next chunk in the Buffer.
 */
void
Buffer::Iterator::next()
{
    if (isDone())
        return;
    currentOffset += current->length;
    current = current->next;
}

/**
 * Return a pointer to the first byte of data in the current chunk that is
 * part of the range of iteration. If this iterator is covering only a
 * subrange of the buffer, then this may not be the first byte in the chunk.
 */
const void*
Buffer::Iterator::getData() const
{
    uint32_t startOffset = 0;
    uint32_t endOfCurrentChunk = currentOffset + current->length;

    // Does current contain offset?  If so, adjust bytes on front not returned.
    //   In detail: If we are on the start chunk and we have a non-zero offset
    //   (the only case where offset > currentOffset is possible) then
    //   tweak the return value.
    //   Note: It's not possible for an offset into current to happen except
    //   on the starting chunk since the constructor guarantees to seek the
    //   Iterator up to the first Chunk that will have data accessed from it.
    if (offset > currentOffset && offset < endOfCurrentChunk)
        startOffset = offset - currentOffset;
    return static_cast<const char *>(current->data) + startOffset;
}

/**
 * Return the length of the data at the current chunk.
 */
uint32_t
Buffer::Iterator::getLength() const
{
    // Just the length of the current chunk with a few possible adjustments.
    uint32_t result = current->length;

    uint32_t end = offset + length;
    uint32_t endOfCurrentChunk = currentOffset + current->length;

    // Does current contain offset?  If so, adjust bytes on front not returned.
    // See the comment in getData for detailed explanation, if needed.
    if (offset > currentOffset && offset < endOfCurrentChunk)
        result -= offset - currentOffset;

    // Does current contain end?  If so, adjust bytes on end not returned.
    if (end > currentOffset && end < endOfCurrentChunk)
        result -= endOfCurrentChunk - end;

    return result;
}

/**
 * Return the total number of bytes this iterator will run across.
 * This number is adjusted depending on the subrange of the buffer
 * requested, if any.
 */
uint32_t
Buffer::Iterator::getTotalLength() const
{
    return length;
}

/**
 * Return the total number of chunks this iterator will run across.
 * This number is adjusted depending on the starting offset into the
 * buffer requested, if any.  This number may be higher than the
 * number of chunks visited if a length is specified as part of the
 * iterator subrange.
 * If this is an Iterator on a Buffer subrange then this value is
 * computed lazily on the first call to this method and cached for
 * subsequent calls.
 */
uint32_t
Buffer::Iterator::getNumberChunks() const
{
    if (!numberChunksIsValid) {
        // Determine the number of chunks this iter will visit and
        // cache for future calls
        Buffer::Iterator* self = const_cast<Buffer::Iterator*>(this);
        Chunk* c = current;
        uint32_t o = currentOffset;
        while (c && (offset + length > o)) {
            o += c->length;
            c = c->next;
            self->numberChunks++;
        }
        self->numberChunksIsValid = true;
    }

    return numberChunks;
}

/**
 * Two Buffer::Iterators are equal if the logical array of bytes they would
 * iterate through is the same, regardless of their chunk layouts.
 * \param leftIter
 *      A Buffer::Iterator that has never had #Buffer::Iterator::next() called
 *      on it.
 * \param rightIter
 *      A Buffer::Iterator that has never had #Buffer::Iterator::next() called
 *      on it.
 * \todo(ongaro): These semantics are a bit weird, but they're useful for
 * comparing subranges of Buffers. What we probably want is a Buffer::Range
 * struct with equality defined in terms of an iterator.
 */
bool
operator==(Buffer::Iterator leftIter, Buffer::Iterator rightIter)
{
    if (leftIter.getTotalLength() != rightIter.getTotalLength())
        return false;
    if (leftIter.getTotalLength() == 0)
        return true;

    const char* leftData = static_cast<const char*>(leftIter.getData());
    uint32_t leftLength = leftIter.getLength();
    const char* rightData = static_cast<const char*>(rightIter.getData());
    uint32_t rightLength = rightIter.getLength();

    while (true) {
        if (leftLength <= rightLength) {
            if (memcmp(leftData, rightData, leftLength) != 0)
                return false;
            rightData += leftLength;
            rightLength -= leftLength;
            leftIter.next();
            if (leftIter.isDone())
                return true;
            leftData = static_cast<const char*>(leftIter.getData());
            leftLength = leftIter.getLength();
        } else {
            if (memcmp(leftData, rightData, rightLength) != 0)
                return false;
            leftData += rightLength;
            leftLength -= rightLength;
            rightIter.next();
            rightData = static_cast<const char*>(rightIter.getData());
            rightLength = rightIter.getLength();
        }
    }
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
 *      This should be #::RAMCloud::PREPEND.
 * \return
 *      The newly allocated memory region of size \a numBytes, which will be
 *      automatically deallocated in this Buffer's destructor. May not be
 *      aligned.
 */
void*
operator new(size_t numBytes, RAMCloud::Buffer* buffer,
             RAMCloud::PREPEND_T prepend)
{
    using namespace RAMCloud;
    uint32_t numBytes32 = downCast<uint32_t>(numBytes);
    if (numBytes32 == 0) {
        // We want no visible effects but should return a unique pointer.
        return buffer->allocatePrepend(1);
    }
    char* data = static_cast<char*>(buffer->allocatePrepend(numBytes32));
    Buffer::Chunk* firstChunk = buffer->chunks;
    if (firstChunk != NULL && firstChunk->isRawChunk() &&
        data + numBytes32 == firstChunk->data) {
        // Grow the existing Chunk.
        firstChunk->data = data;
        firstChunk->length += numBytes32;
        buffer->totalLength += numBytes32;
    } else {
        buffer->prepend(data, numBytes32);
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
 *      This should be #::RAMCloud::APPEND.
 * \return
 *      The newly allocated memory region of size \a numBytes, which will be
 *      automatically deallocated in this Buffer's destructor. May not be
 *      aligned.
 */
void*
operator new(size_t numBytes, RAMCloud::Buffer* buffer,
             RAMCloud::APPEND_T append)
{
    using namespace RAMCloud;
    uint32_t numBytes32 = downCast<uint32_t>(numBytes);
    if (numBytes32 == 0) {
        // We want no visible effects but should return a unique pointer.
        return buffer->allocateAppend(1);
    }
    char* data = static_cast<char*>(buffer->allocateAppend(numBytes32));
    Buffer::Chunk* const lastChunk = buffer->chunksTail;
    if (lastChunk != NULL && lastChunk->isRawChunk() &&
        data - lastChunk->length == lastChunk->data) {
        // Grow the existing Chunk.
        lastChunk->length += numBytes32;
        buffer->totalLength += numBytes32;
    } else {
        buffer->append(data, numBytes32);
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
 *      This should be #::RAMCloud::CHUNK.
 * \return
 *      The newly allocated memory region of size \a numBytes, which will be
 *      automatically deallocated in this Buffer's destructor. Will be aligned
 *      to 8 bytes.
 */
void*
operator new(size_t numBytes, RAMCloud::Buffer* buffer,
             RAMCloud::CHUNK_T chunk)
{
    using namespace RAMCloud;
    assert(numBytes >= sizeof(Buffer::Chunk));
    return buffer->allocateChunk(downCast<uint32_t>(numBytes));
}

/**
 * Allocate a contiguous region of memory in a Buffer.
 *
 * \warning This memory will not necessarily be aligned.
 *
 * \param[in] numBytes
 *      The number of bytes to allocate.
 * \param[in] buffer
 *      The Buffer in which to allocate the memory.
 * \param[in] misc
 *      This should be #::RAMCloud::MISC.
 * \return
 *      The newly allocated memory region of size \a numBytes, which will be
 *      automatically deallocated in this Buffer's destructor. May not be
 *      aligned.
 */
void*
operator new(size_t numBytes, RAMCloud::Buffer* buffer,
             RAMCloud::MISC_T misc)
{
    using namespace RAMCloud;
    if (numBytes == 0) {
        // We want no visible effects but should return a unique pointer.
        return buffer->allocateAppend(1);
    }
    return buffer->allocateAppend(downCast<uint32_t>(numBytes));
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
