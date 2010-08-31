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
 * Malloc and construct an Allocation.
 * \param[in] prependSize
 *      See constructor.
 * \param[in] totalSize
 *      See constructor. Will round-up to the nearest 8 bytes.
 */
Buffer::Allocation*
Buffer::Allocation::newAllocation(uint32_t prependSize, uint32_t totalSize) {
    totalSize = (totalSize + 7) & ~7U;
    void* a = xmalloc(sizeof(Allocation) + totalSize);
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
    : totalLength(0), numberChunks(0), chunks(NULL),
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
 * Create a printable representation of the contents of the buffer.
 * The string representation was designed primarily for printing
 * network packets during testing.
 *
 * \result string
 *      The return value is a string describing the contents of the
 *      buffer. The string consists of one or more items separated
 *      by white space, with each item representing a range of bytes
 *      in the buffer (these ranges do not necessarily correspond to
 *      the buffer's internal chunks).  A chunk can be either an integer
 *      representing 4 contiguous bytes of the buffer or a null-terminated
 *      string representing any number of bytes.  String format is preferred,
 *      but is only used for things that look like strings.  Integers
 *      are printed in decimal if they are small, otherwise hexadecimal.
 */
string
Buffer::toString() {
    string s;
    uint32_t length = getTotalLength();
    uint32_t i = 0;
    char temp[20];
    const char* separator = "";

    // Each iteration through the following loop processes a piece
    // of the buffer consisting of either:
    // * 4 bytes output as a decimal integer
    // * or, a string output as a string
    while (i < length) {
        s.append(separator);
        separator = " ";
        if ((i+4) <= length) {
            const char *p = static_cast<const char*>(getRange(i, 4));
            if ((p[0] < ' ') || (p[1] < ' ')) {
                int value = *reinterpret_cast<const int*>(p);
                snprintf(temp, sizeof(temp),
                        ((value > 10000) || (value < -1000)) ? "0x%x" : "%d",
                        value);
                s.append(temp);
                i += 4;
                continue;
            }
        }

        // This chunk of data looks like a string, so output it out as one.
        while (i < length) {
            char c = *static_cast<const char*>(getRange(i, 1));
            i++;
            convertChar(c, &s);
            if (c == '\0') {
                break;
            }
        }
    }
    return s;
}

/**
 * Generate a string describing the contents of the buffer in a way
 * that displays its internal chunk structure.
 *
 * This method is intended primarily for use in tests for this class.
 *
 * \return A string that describes the contents of the buffer. It
 *         consists of the contents of the various chunks separated
 *         by " | ", with long chunks abbreviated and non-printing
 *         characters converted to something printable.
 */
string Buffer::debugString() {
    // The following declaration defines the maximum number of characters
    // to display from each chunk.
    static const uint32_t CHUNK_LIMIT = 20;
    const char *separator = "";
    char temp[20];
    uint32_t chunkLength;
    string s;

    for (uint32_t offset = 0; ; offset += chunkLength) {
        const char *chunk;
        chunkLength = peek(offset, reinterpret_cast<const void **>(&chunk));
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
            convertChar(chunk[i], &s);
        }
    }
    return s;
}

/**
 * Replace the contents of the buffer with data specified in a string.
 * This method was designed primarily for use in tests (e.g. to specify
 * network packets).
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
    reset();
    uint32_t i, length;
    length = strlen(s);
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
 * Convert a character to a printable form (if it isn't already) and append
 * to a string. This method is used by other methods such as debugString
 * and toString.
 *
 * \param c
 *      Character to convert.
 * \param[out] out
 *      Append the converted result here. Non-printing characters get
 *      converted to a form using "/" (not "\"!).  This produces a result
 *      that can be cut and pasted from test output into test code: the
 *      result will never contain any characters that require quoting
 *      if used in a C string, such as backslashes or quotes.
 */
void
Buffer::convertChar(char c, string *out) {
    if ((c >= 0x20) && (c < 0x7f) && (c != '"') && (c != '\\')) {
        out->append(&c, 1);
    } else if (c == '\0') {
        out->append("/0");
    } else if (c == '\n') {
        out->append("/n");
    } else {
        char temp[20];
        uint32_t value = c & 0xff;
        snprintf(temp, sizeof(temp), "/x%02x", value);
        out->append(temp);
    }
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
const void* Buffer::Iterator::getData() const {
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
 *      automatically deallocated in this Buffer's destructor. May not be
 *      aligned.
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
 *      automatically deallocated in this Buffer's destructor. May not be
 *      aligned.
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
 *      automatically deallocated in this Buffer's destructor. Will be aligned
 *      to 8 bytes.
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
 * \warning This memory will not necessarily be aligned.
 *
 * \param[in] numBytes
 *      The number of bytes to allocate.
 * \param[in] buffer
 *      The Buffer in which to allocate the memory.
 * \param[in] misc
 *      This should be #::RAMCloud::MISC_T::MISC.
 * \return
 *      The newly allocated memory region of size \a numBytes, which will be
 *      automatically deallocated in this Buffer's destructor. May not be
 *      aligned.
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
