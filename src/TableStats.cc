/* Copyright (c) 2013-2015 Stanford University
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
#include "ShortMacros.h"

namespace RAMCloud {

namespace TableStats {

/**
 * Update the table status information in the event a tablet is added to keep
 * track of the number of key hash values that this master owns for the given
 * tableId.  This is called by callers of TabletManager::addTablet.  This method
 * does no sanity checking; it assumes that callers ensure tablet ranges do not
 * overlap causing double counting (by calling TabletManager::addTablet first).
 *
 * \param mtm
 *      Pointer to MasterTableMetadata container that is storing the current
 *      stats information.  Must not be NULL.
 * \param tableId
 *      Id of table whose stats information will be updated.
 * \param startKeyHash
 *      First key hash value of the to-be-added tablet.
 * \param endKeyHash
 *      Last Key hash value of the to-be-added tablet.
 */
void
addKeyHashRange(MasterTableMetadata* mtm,
                uint64_t tableId,
                uint64_t startKeyHash,
                uint64_t endKeyHash)
{
    MasterTableMetadata::Entry* entry;
    entry = mtm->findOrCreate(tableId);

    Lock guard(entry->stats.lock);
    // We assuming that calls to addTablet will only overflow keyHashCount if
    // the master has total ownership (in that case the keyHashCount will
    // overflow to 0).
    entry->stats.keyHashCount += (endKeyHash - startKeyHash);
    entry->stats.keyHashCount += 1;
    if (entry->stats.keyHashCount == 0) {
        entry->stats.totalOwnership = true;
    }
    TEST_LOG("tableId %lu range [0x%lx,0x%lx]",
            tableId, startKeyHash, endKeyHash);
}

/**
 * Update the table status information in the event a tablet is deleted to keep
 * track of the number of key hash values that this master owns for the given
 * tableId.  This is called by callers of TabletManager::deleteTablet.  This
 * method does no sanity checking; it assumes that callers ensure tablet ranges
 * do not overlap causing double counting (by calling
 * TabletManager::deleteTablet first).
 *
 * \param mtm
 *      Pointer to MasterTableMetadata container that is storing the current
 *      stats information.  Must not be NULL.
 * \param tableId
 *      Id of table whose stats information will be updated.
 * \param startKeyHash
 *      First key hash value of the to-be-deleted tablet.
 * \param endKeyHash
 *      Last Key hash value of the to-be-deleted tablet.
 */
void
deleteKeyHashRange(MasterTableMetadata* mtm,
                   uint64_t tableId,
                   uint64_t startKeyHash,
                   uint64_t endKeyHash)
{
    MasterTableMetadata::Entry* entry;
    entry = mtm->find(tableId);

    if (entry != NULL) {
        Lock guard(entry->stats.lock);
        // We assuming that calls to deleteTablet will not cause keyHashCount
        // underflow except when moving from a state where the master owns the
        // whole table.
        entry->stats.keyHashCount -= (endKeyHash - startKeyHash);
        entry->stats.keyHashCount -= 1;
        entry->stats.totalOwnership = false;
    }
    TEST_LOG("tableId %lu range [0x%lx,0x%lx]",
            tableId, startKeyHash, endKeyHash);
}

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

    DigestHeader* header = buf->emplaceAppend<DigestHeader>();
    *header = {0, 0, 0};

    double otherKeyHashCount = 0;
    uint64_t otherByteCount = 0;
    uint64_t otherRecordCount = 0;
    bool hasOtherEntries = false;

    MasterTableMetadata::scanner sc = mtm->getScanner();
    while (sc.hasNext()) {
        MasterTableMetadata::Entry* entry = sc.next();
        {
            Lock guard(entry->stats.lock);
            double keyHashCount;
            if (entry->stats.totalOwnership) {
                keyHashCount = double(entry->stats.keyHashCount - 1);
                keyHashCount += 1;
            } else {
                // Skip this entry if it is empty
                if (entry->stats.keyHashCount == 0) {
                    continue;
                }

                keyHashCount = double(entry->stats.keyHashCount);
            }


            if (entry->stats.byteCount >= threshold) {
                header->entryCount++;
                double bytesPerKeyHash = double(entry->stats.byteCount) /
                                         keyHashCount;
                double recordsPerKeyHash = double(entry->stats.recordCount) /
                                           keyHashCount;
                *(buf->emplaceAppend<DigestEntry>()) =
                        {entry->tableId, bytesPerKeyHash, recordsPerKeyHash};

            } else {
                otherKeyHashCount += keyHashCount;
                otherByteCount += entry->stats.byteCount;
                otherRecordCount += entry->stats.recordCount;
                hasOtherEntries = true;
            }
        }
    }

    if (hasOtherEntries) {
            header->otherBytesPerKeyHash = double(otherByteCount) /
                                           otherKeyHashCount;
            header->otherRecordsPerKeyHash = double(otherRecordCount) /
                                             otherKeyHashCount;
    }
}

/**
 * Constructs Estimator object.
 *
 * \param digest
 *      A master will store summarized table stats information at the beginning
 *      of each of its segments.  This digest is the summarized table stats
 *      information extracted from the head segment of the failed master that is
 *      now to be recovered.
 */
Estimator::Estimator(const Digest* digest)
    : valid(false)
    , tableStats()
    , otherStats()
{
    // If for some reason, the digest the estimator needs is not available, we
    // should log an error as this should never be the case during normal
    // operation.  This error might occur if the digest, for instance, was for
    // some reason missing from the head segment.  In this case, we  would still
    // like to try to recover (using a degraded recovery method) so we simply
    // construct an invalid estimator and allow recovery to continue without
    // crashing.
    if (digest == NULL) {
        LOG(ERROR, "Table digest missing during recovery.");
        return;
    }

    for (uint64_t i = 0; i < digest->header.entryCount; i++) {
        const DigestEntry* entry = &digest->entries[i];
        tableStats[entry->tableId] = {entry->bytesPerKeyHash,
                                      entry->recordsPerKeyHash};
    }

    otherStats = {digest->header.otherBytesPerKeyHash,
                  digest->header.otherRecordsPerKeyHash};

    valid = true;
}

/**
 * Given a tablet, this method estimates how much log space and how many log
 * records the tablet consumes; it's used by the coordinator during recovery to
 * partition a dead master's tablets among recovery masters.
 *
 * \param tablet
 *      Pointer to tablet for which an estimate will be provided.
 * \return
 *      Returns an Entry containing the estimated stats information for the
 *      provided tablet.  If information specific to the tablet's table is
 *      available, the table information is used.  If no specific table
 *      information is found, the cumulative stats information is used.
 */
Estimator::Estimate
Estimator::estimate(Tablet *tablet)
{
    Estimate est = {0, 0};
    double keyRange = double(tablet->endKeyHash - tablet->startKeyHash) + 1;

    StatsMap::iterator entry = tableStats.find(tablet->tableId);
    if (entry != tableStats.end()) {
        // The table was large enough that statistics were kept specifically
        // for this table.
        est.byteCount = uint64_t(entry->second.bytesPerKeyHash * keyRange);
        est.recordCount = uint64_t(entry->second.recordsPerKeyHash * keyRange);
    } else {
        // This table was so small that statistics were not kept separately for
        // it; instead, its statistics were lumped together with all of the
        // other small tables. Use that aggregate information to estimate the
        // info for this table.
        est.byteCount = uint64_t(otherStats.bytesPerKeyHash * keyRange);
        est.recordCount = uint64_t(otherStats.recordsPerKeyHash * keyRange);
    }

    return est;
}

} // namespace TableStats

} // namespace RAMCloud
