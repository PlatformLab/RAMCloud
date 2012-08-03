/* Copyright (c) 2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TableEnumerator.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Constructor for TableEnumerator objects.
 *
 * \param ramcloud
 *      Overall information about the RAMCloud cluster to use for this
 *      enumeration.
 * \param tableId
 *      Identifier for the table to enumerate.
 */
TableEnumerator::TableEnumerator(RamCloud& ramcloud, uint64_t tableId)
    : ramcloud(ramcloud)
    , tableId(tableId)
    , tabletStartHash(0)
    , done(false)
    , state()
    , objects()
    , nextOffset(0)
{
}

/**
 * Test if any objects remain to be enumerated from the table.
 *
 * \result
 *      True if any objects remain, or false otherwise.
 */
bool
TableEnumerator::hasNext()
{
    requestMoreObjects();
    return !done;
}

/**
 * Return the next object in the table.  Note: each object that existed
 * throughout the entire lifetime of the enumeration is guaranteed to
 * be returned exactly once.  Objects that are created after the enumeration
 * starts, or that are deleted before the enumeration completes, will be
 * returned either 0 or 1 time.
 *
 * \param[out] size
 *      After a successful return, this field will hold the size of
 *      the object in bytes.
 * \param[out] object
 *      After a successful return, this will point to contiguous
 *      memory containing an instance of Object immediately followed
 *      by its key and data payloads. NULL is returned to indicate
 *      that the enumeration is complete.
 */
void
TableEnumerator::next(uint32_t* size, const void** object)
{
    *size = 0;
    *object = NULL;

    requestMoreObjects();
    if (done) return;

    uint32_t objectSize = *objects.getOffset<uint32_t>(nextOffset);
    nextOffset += downCast<uint32_t>(sizeof(uint32_t));

    const void* blob = objects.getRange(nextOffset, objectSize);
    nextOffset += objectSize;

    // Store result in out params.
    *size = objectSize;
    *object = blob;
}

/**
 * Used internally by #hasNext() and #next() to retrieve objects. Will
 * set the #done field if enumeration is complete. Otherwise the
 * #objects Buffer will contain at least one object.
 */
void
TableEnumerator::requestMoreObjects()
{
    if (done || nextOffset < objects.getTotalLength()) return;

    nextOffset = 0;
    while (true) {
        tabletStartHash = ramcloud.enumerateTable(tableId, tabletStartHash,
                state, objects);
        if (objects.getTotalLength() > 0) {
            return;
        }
        // End of table?
        if (objects.getTotalLength() == 0 && tabletStartHash == 0) {
            done = true;
            return;
        }

        // If we get here it means that the last server we contacted
        // has no more objects for us, but there are probably some other
        // objects in a different server. Try again with a new server.
    }
}

} // namespace RAMCloud
