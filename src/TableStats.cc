/* Copyright (c) 2013 Stanford University
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

#include "TableStats.h"
#include "MasterTableMetadata.h"

namespace RAMCloud {

namespace TableStats {
/**
 * Update table stats information, incrementing the exisitng stats values by the
 * values provided.  If no stats information previously existed for the
 * referenced table, a new entry is added.  In this way, this method will always
 * succeed.  This method is invoked whenever new data related to a table is
 * written to the log.
 *
 * \param mtm
 *      Pointer to MasterTableMetadata container that is storing the current
 *      stats information.  Must not be NULL.
 * \param tableId
 *      Id of table whose stats information will be updated.
 * \param byteCount
 *      Number of bytes of new data (related to tableId) added to the log.
 * \param recordCount
 *      Number of new records (related to tableId) added to the log.
 */
void
increment(MasterTableMetadata* mtm,
          uint64_t tableId,
          uint64_t byteCount,
          uint64_t recordCount)
{
    MasterTableMetadata::Entry* entry;
    entry = mtm->findOrCreate(tableId);

    Lock guard(entry->stats.lock);
    entry->stats.byteCount += byteCount;
    entry->stats.recordCount += recordCount;
}

/**
 * Update table stats information, decrementing the existing stats values by the
 * values provided.  If no stats information previously existed for the
 * referenced table, this method has no effect.  In this way, this method will
 * either succeed or have no effect.  This method is invoked whenever records
 * related to a table is cleaned from the log.
 *
 * \param mtm
 *      Pointer to MasterTableMetadata container that is storing the current
 *      stats information.  Must not be NULL.
 * \param tableId
 *      Id of table whose stats information will be updated.
 * \param byteCount
 *      Number of bytes of data (related to tableId) cleaned from the log.
 * \param recordCount
 *      Number of records (related to tableId) cleaned to the log.
 */
void
decrement(MasterTableMetadata* mtm,
          uint64_t tableId,
          uint64_t byteCount,
          uint64_t recordCount)
{
    MasterTableMetadata::Entry* entry;
    entry = mtm->find(tableId);

    if (entry != NULL) {
        Lock guard(entry->stats.lock);
        entry->stats.byteCount -= byteCount;
        entry->stats.recordCount -= recordCount;
    }
}


/**
 * Compress and serialize all table stats information in the MasterTableMetadata
 * container into a buffer.  This method should be called during a log segment
 * rollover to serialize the latest stats to the beginning of the segment.
 *
 * \param buf
 *      Buffer where compressed and serialized table stats information will be
 *      appended.
 * \param mtm
 *      Pointer to MasterTableMetadata container that is storing the current
 *      stats information.
 */
void
serialize(Buffer *buf, MasterTableMetadata *mtm)
{
    /*
     * This function should serialize the table stats information into the
     * buffer so that it conforms to the Digest structure.  This structure is
     * implicitly used in this method because actually creating a structure,
     * populating it, and then adding it to the buffer introduces an additional
     * round of copies.  While we avoid this by adding the data directly into
     * the buffer, it is important that the structure is still implicitly
     * enforced so that deserialization can expect the same structure.
     */

    DigestHeader* header = new(buf, APPEND) DigestHeader({0, 0, 0});

    MasterTableMetadata::scanner sc = mtm->getScanner();
    while (sc.hasNext()) {
        MasterTableMetadata::Entry* entry = sc.next();
        {
            Lock guard(entry->stats.lock);
            if (entry->stats.byteCount >= threshold) {
                header->entryCount++;
                new(buf, APPEND) DigestEntry({entry->tableId,
                                              entry->stats.byteCount,
                                              entry->stats.recordCount});


            } else {
                header->otherByteCount += entry->stats.byteCount;
                header->otherRecordCount += entry->stats.recordCount;
            }
        }
    }
}

} // namespace TableStats

} // namespace RAMCloud
