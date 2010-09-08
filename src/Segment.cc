/* Copyright (c) 2009 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
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
 * Implementation of #RAMCloud::Segment.
 */

#include "Segment.h"

namespace RAMCloud {

Segment::Segment(void *buf,
                 const uint64_t len,
                 BackupClient *backupClient)
    : base(buf),
      isMutable(false),
      id(SEGMENT_INVALID_ID),
      totalBytes(len),
      freeBytes(totalBytes),
      tailBytes(totalBytes),
      backup(backupClient),
      next(0),
      prev(0)
{
    assert(buf != NULL);
    assert(len > 0);
}

Segment::~Segment()
{
}

/**
 * Ready an empty, inactive segment by repurposing it with a new segment
 * identifier and making it mutable. This must be called before a previously
 * reset or newly allocated Segment is used.
 *
 * \param newId
 *      The new segment identifier.
 */
void
Segment::ready(uint64_t newId)
{
    assert(!isMutable);
    assert(id == SEGMENT_INVALID_ID);

    isMutable = true;
    id        = newId;
}

/**
 * Reset the Segment by marking all storage as free and invalidating its
 * identifier. The Segment cannot be used until it is made ready again and
 * assigned a new identifier.
 */
void
Segment::reset()
{
    assert(!isMutable);

    if (id != SEGMENT_INVALID_ID)
        backup->freeSegment(id);

    freeBytes  = totalBytes;
    tailBytes  = totalBytes;
    isMutable   = false;
    id          = SEGMENT_INVALID_ID;
    memset(base, 0xcc, totalBytes);
}

/**
 * Append data to the Segment. 
 *
 * \param  buf
 *      Pointer to the data. 
 * \param len
 *      Byte length of the data pointed to by buf.
 * \return
 *      An immutable pointer to the location of the data in the Segment,
 *      or NULL if there is insufficient space in the Segment.
 */
const void *
Segment::append(const void *buf, const uint64_t len)
{
    assert(isMutable);
    assert(id != SEGMENT_INVALID_ID);

    if (tailBytes < len)
        return NULL;

    assert(freeBytes >= len);

    uint64_t offset = totalBytes - tailBytes;
    void *loc = static_cast<uint8_t*>(base) + offset;

    memcpy(loc, buf, len);
    backup->writeSegment(id, offset, buf, len);
    freeBytes -= len;
    tailBytes -= len;

    return loc;
}

/**
 * Mark space the Segment as free (no longer used). The Segment's contents are
 * left unmodified. This only affects metadata used for maintaining utilisation
 * information.
 *
 * \param len
 *      Number of bytes newly freed bytes.
 */
void
Segment::free(uint64_t len)
{
    assert((len + freeBytes) <= totalBytes);
    freeBytes += len;
}

/**
 * Obtain an immutable pointer to the first of this Segment's contiguous
 * data bytes.
 *
 * \return
 *      An immutable pointer to the Segment data's start address.
 */
const void *
Segment::getBase() const
{
    return base;
}

/**
 * Obtain the Segment's segment identifier.
 *
 * \return
 *      The segment's identifier, or SEGMENT_INVALID_ID if the Segment
 *      has not been readied.
 *
 */
uint64_t
Segment::getId() const
{
    return id;
}

/**
 * Obtain the number of bytes left to be written at the end of this Segment.
 *
 * \return
 *      The number of free bytes at the tail.
 */
uint64_t
Segment::getFreeTail() const
{
    return tailBytes;
}

/**
 * Obtain the Segment's length in bytes.
 *
 * \return
 *      The number of bytes the Segment can store.
 */
uint64_t
Segment::getLength() const
{
    return totalBytes;
}

/**
 * Obtain the Segment's utilisation, i.e. the difference between the Segment's
 * size and the number of total free bytes in the Segment (not only free bytes
 * at the tail).
 *
 * \return
 *      The number of bytes the Segment is currently using.
 */
uint64_t
Segment::getUtilization() const
{
    return totalBytes - freeBytes;
}

/**
 * Determine whether the provided pointer points within the Segment and that
 * the specified number of bytes are also within the Segment, i.e. given an
 * address range, check to see if it all fits within the Segment.
 *
 * \param p
 *      A pointer to anywhere.
 * \param len
 *      The number of bytes from the pointer to check.
 * \return
 *      True if the range is valid, or false if the range is invalid.
 */
bool
Segment::checkRange(const void *p, uint64_t len) const
{
    uintptr_t up = (uintptr_t)p;
    uintptr_t ub = (uintptr_t)base;

    return (up >= ub && (up + len) <= (ub + totalBytes));
}

/**
 * Finalise a Segment when done with it. The Segment is marked as immutable
 * and committed to the backup.
 */
void
Segment::finalize()
{
    assert(id != SEGMENT_INVALID_ID);
    isMutable = false;
    backup->commitSegment(id);
}

/**
 * Restore a previously backed-up Segment into the present Segment.
 *
 * \param restoreSegId
 *      The segment identifier to restore as.
 */
void
Segment::restore(uint64_t restoreSegId)
{
    assert(id == SEGMENT_INVALID_ID);

    //printf("Segment restoring from %llu:\n", restoreSegId);
    backup->retrieveSegment(restoreSegId, base);
    // TODO(stutsman) restore all sorts of state/invariants
    // It seems we want to restore this information by making a single
    // pass which happens in the server to rebuild the hashtable
    id = restoreSegId;
}

/**
 * Link the Segment into a doubly-linked list.
 *
 * \param n
 *      The Segment to insert before, or NULL to insert at the end.
 * \return
 *      A pointer to this Segment (useful for inlining insertions).
 */
Segment *
Segment::link(Segment *n)
{
    assert(prev == NULL && next == NULL);
    assert(n == NULL || n->prev == NULL);

    if (n != NULL)
        n->prev = this;
    next = n;

    return this;
}

/**
 * Remove the Segment from a doubly-linked list.
 *
 * \return
 *      A pointer to the following Segment (useful for inlining removals),
 *      or NULL if there is none. 
 */
Segment *
Segment::unlink()
{
    if (prev != NULL)
        prev->next = next;
    if (next != NULL)
        next->prev = prev;

    Segment *n = next;
    prev = next = NULL;
    return n;
}

} // namespace
