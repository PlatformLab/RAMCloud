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
#include "ShortMacros.h"

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

    DigestHeader* header = buf->emplaceAppend<DigestHeader>();
    *header = {0, 0, 0};

    MasterTableMetadata::scanner sc = mtm->getScanner();
    while (sc.hasNext()) {
        MasterTableMetadata::Entry* entry = sc.next();
        {
            Lock guard(entry->stats.lock);
            if (entry->stats.byteCount >= threshold) {
                header->entryCount++;
                *(buf->emplaceAppend<DigestEntry>()) =
                        {entry->tableId, entry->stats.byteCount,
                         entry->stats.recordCount};


            } else {
                header->otherByteCount += entry->stats.byteCount;
                header->otherRecordCount += entry->stats.recordCount;
            }
        }
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
 * \param tablets
 *      Contains the tablets of the failed master that are to be recovered.
 */
Estimator::Estimator(const Digest* digest, vector<Tablet>* tablets)
    : tableStats()
    , otherStats()
    , valid(false)
{
    // If for some reason, the digest or vector the estimator needs is not
    // avilable, we should log an error as this should never be the case during
    // normal operation.  This error might occur if the digest, for instance,
    // was for some reason missing from the head segment.  In this case, we
    // would still like to try to recover (using a degraded recovery method)
    // so we simply construct an invalid estimator and allow recovery to
    // continue without crashing.
    if (digest == NULL || tablets == NULL) {
        LOG(ERROR,
            "Table digest or tablet information missing during recovery.");
        return;
    }

    // First, the estimator is populated using table digest information to fill
    // in the byteCount and recordCount information.  KeyHash range information
    // will be filled in later.
    for (uint64_t i = 0; i < digest->header.entryCount; i++) {
        const DigestEntry* entry = &digest->entries[i];
        tableStats[entry->tableId] = {0, entry->byteCount, entry->recordCount};
    }

    otherStats = {0,
                  digest->header.otherByteCount,
                  digest->header.otherRecordCount};

    // Second, the estimator uses the tablet infromation to collect aggragate
    // keyHash range information.
    for (vector<Tablet>::iterator it = tablets->begin();
         it != tablets->end();
         it++) {
        StatsMap::iterator entry = tableStats.find(it->tableId);
        if (entry != tableStats.end()) {
            // Increment performed after conversion to avoid overflow.
            entry->second.keyRange += double(it->endKeyHash
                                             - it->startKeyHash) + 1;
        } else {
            // Increment performed after conversion to avoid overflow.
            otherStats.keyRange += double(it->endKeyHash
                                          - it->startKeyHash) + 1;
        }
    }

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
Estimator::Entry
Estimator::estimate(Tablet *tablet)
{
    Entry est = {0, 0, 0};
    est.keyRange = double(tablet->endKeyHash - tablet->startKeyHash) + 1;

    StatsMap::iterator entry = tableStats.find(tablet->tableId);
    if (entry != tableStats.end()) {
        // The table was large enough that statistics were kept specifically
        // for this table.
        if (entry->second.keyRange > 0) {
            // This tablet's fraction of its table's byte and record counts are
            // estimated to be proportional to the tablet's fraction of the
            // table's keyRange.
            est.byteCount = uint64_t(double(entry->second.byteCount)
                                     * est.keyRange
                                     / entry->second.keyRange);
            est.recordCount = uint64_t(double(entry->second.recordCount)
                                       * est.keyRange
                                       / entry->second.keyRange);
        } else {
            // If the keyRange is 0 (or less than 0) then we will assume the
            // byteCount and recordCount is also 0. Note: they are initialized
            // to 0.
            // THIS CASE SHOULD NEVER BE REACHED.
        }
    } else {
        // This table was so small that statistics were not kept separately for
        // it; instead, its statistics were lumped together with all of the
        // other small tables. Use that aggregate information to estimate the
        // info for this table.
        if (otherStats.keyRange > 0) {
            // This tablet's fraction of the small table aggregate byte and
            // record counts are estimated to be proportional to the tablet's
            // fraction of the small table aggregate keyRange.
            est.byteCount = uint64_t(double(otherStats.byteCount)
                                     * est.keyRange
                                     / otherStats.keyRange);
            est.recordCount = uint64_t(double(otherStats.recordCount)
                                       * est.keyRange
                                       / otherStats.keyRange);
        } else {
            // If the keyRange is 0 (or less than 0) then we will assume the
            // byteCount and recordCount is also 0. Note: they are initialized
            // to 0.
            // This might be the case if a new table is created and a new table
            // stats digest was not written afterward because the head segment
            // did not roll over yet.
        }
    }

    return est;
}

} // namespace TableStats

} // namespace RAMCloud
