/* Copyright (c) 2012-2014 Stanford University
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
 * \param keysOnly
 *      False means that full objects are returned, containing both keys
 *      and data. True means that the returned objects have
 *      been truncated so that the object data (normally the last
 *      field of the object) is omitted.
 */
TableEnumerator::TableEnumerator(RamCloud& ramcloud,
                                uint64_t tableId,
                                bool keysOnly)
    : ramcloud(ramcloud)
    , tableId(tableId)
    , keysOnly(keysOnly)
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
 * Returns the next object in the enumeration, if any, with a more
 * convenient interface than hasNext and next. Note: each object that existed
 * throughout the entire lifetime of the enumeration is guaranteed to
 * be returned exactly once.  Objects that are created after the enumeration
 * starts, or that are deleted before the enumeration completes, will be
 * returned either 0 or 1 time.
 *
 * \param[out] keyLength
 *      After successful return, this field holds the size of the key in bytes.
 * \param[out] key
 *      After a successful return, this points to contiguous memory containing
 *      the key. NULL is returned to indicate enumeration is complete.
 * \param[out] dataLength
        After successful return, this field holds the size of the data in bytes.
 * \param[out] data
 *      After a successful return, this points to contiguous memory containing
 *      the data. If the keysOnly flag is set while constructing TableEnumerator 
 *      , NULL is returned.
 */
void
TableEnumerator::nextKeyAndData(uint32_t* keyLength, const void** key,
                             uint32_t* dataLength, const void** data)
{
    *keyLength = 0;
    *key= NULL;
    *dataLength = 0;
    *data= NULL;

    uint32_t size = 0;
    const void* buffer = 0;
    next(&size, &buffer);
    if (done) return;

    Object object(buffer, size);
    *keyLength = object.getKeyLength();
    *key = object.getKey();

    if (!keysOnly) {
        *data = object.getValue(dataLength);
    }
}

/**
 * Used internally by #hasNext() and #next() to retrieve objects. Will
 * set the #done field if enumeration is complete. Otherwise the
 * #objects Buffer will contain at least one object.
 */
void
TableEnumerator::requestMoreObjects()
{
    if (done || nextOffset < objects.size()) return;

    nextOffset = 0;
    while (true) {
        tabletStartHash = ramcloud.enumerateTable(tableId, keysOnly,
                                            tabletStartHash, state, objects);
        if (objects.size() > 0) {
            return;
        }
        // End of table?
        if (objects.size() == 0 && tabletStartHash == 0) {
            done = true;
            return;
        }

        // If we get here it means that the last server we contacted
        // has no more objects for us, but there are probably some other
        // objects in a different server. Try again with a new server.
    }
}

} // namespace RAMCloud
