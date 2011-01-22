/* Copyright (c) 2010 Stanford University
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

#include "Common.h"
#include "Table.h"
#include "Will.h"

namespace RAMCloud {

//////////////////////////////////////////////
// Will class
//////////////////////////////////////////////

// Public Methods

/**
 * Create and compute a new will, given our server's list of Tablets.
 * This expects that the ProtoBuf's user_data field points to a valid
 * Table object, so that it can use its TabletProfiler.
 *
 * \param[in] tablets
 *      Reference to the list of tablets that will be used when computing
 *      the Will. 
 * \param[in] maxBytesPerPartition
 *      The maximum number of bytes to allow in any partition.
 * \param[in] maxReferantsPerPartition
 *      The maximum number of referants to allow in any partition.
 */
Will::Will(ProtoBuf::Tablets &tablets, uint64_t maxBytesPerPartition,
    uint64_t maxReferantsPerPartition)
    : currentId(0),
      currentMaxBytes(0),
      currentMaxReferants(0),
      currentCount(0),
      maxBytesPerPartition(maxBytesPerPartition),
      maxReferantsPerPartition(maxReferantsPerPartition),
      entries()
{
    foreach (const ProtoBuf::Tablets::Tablet& tablet, tablets.tablet())
        addTablet(tablet);
}

/**
 * Dump a string representation of the Will to the debug log.
 */
void
Will::debugDump()
{
    LOG(DEBUG, "                                       L A S T    W I L L    "
               "A N D    T E S T A M E N T");
    LOG(DEBUG, "------------------------------------------------------------"
               "------------------------------------------------------------"
               "-----");
    LOG(DEBUG, "Partition             TableId            FirstKey             "
               "LastKey     MinBytes     MaxBytes   MinReferants   "
               "MaxReferants");
    LOG(DEBUG, "------------------------------------------------------------"
               "------------------------------------------------------------"
               "-----");
    for (unsigned int i = 0; i < entries.size(); i++) {
        WillEntry *we = &entries[i];
        LOG(DEBUG, "%9lu  0x%016lx  0x%016lx  0x%016lx  %9luKB  %9luKB      "
            "%9lu      %9lu", we->partitionId, we->tableId, we->firstKey,
            we->lastKey, we->minBytes / 1024, we->maxBytes/ 1024,
            we->minReferants, we->maxReferants);
    }
}

// Private Methods

/**
 * Add a tablet to the Will. This will invoke the TabletProfiler's
 * partitioning function, then pass the results into Will:addPartition
 * to integrate them into this Will.
 *
 * \param[in] tablet
 *      Reference to the tablet to add.
 */
void
Will::addTablet(const ProtoBuf::Tablets::Tablet& tablet)
{
    Table& t = *reinterpret_cast<Table*>(tablet.user_data());
    PartitionList *parts = t.profiler.getPartitions(maxBytesPerPartition,
        maxReferantsPerPartition, currentMaxBytes, currentMaxReferants);
    assert(parts->size() > 0);
    for (unsigned int i = 0; i < parts->size(); i++)
        addPartition((*parts)[i], tablet);
    delete parts;
}

/**
 * Add a TabletProfiler's Partition to this Will. This method will
 * increment the current partition number if necessary (i.e. if the
 * partition would grow too large with this addition).
 *
 * \param[in] partition
 *      The TabletProfiler Partition to add to this Will.
 * \param[in] tablet
 *      The Tablet corresponding to this Partition.
 */ 
void
Will::addPartition(Partition& partition,
    const ProtoBuf::Tablets::Tablet& tablet)
{
    uint64_t maxBytes = partition.maxBytes + currentMaxBytes;
    uint64_t maxReferants = partition.maxReferants + currentMaxReferants;

    if ((maxBytes > maxBytesPerPartition ||
         maxReferants > maxReferantsPerPartition) && currentCount > 0) {
        currentId++;
        currentMaxBytes = currentMaxReferants = currentCount = 0;
    }

    WillEntry we;
    we.partitionId = currentId;
    we.tableId = tablet.table_id();

    // TabletProfiler will always track the entire key space, so it may
    // list ranges that are outside of the Tablet's scope. It's easier
    // that way (simplifies growing a Tablet), so compensate here.
    we.firstKey = std::max(tablet.start_object_id(), partition.firstKey);
    we.lastKey = std::min(tablet.end_object_id(), partition.lastKey);

    we.minBytes = partition.minBytes;
    we.maxBytes = partition.maxBytes;
    we.minReferants = partition.minReferants;
    we.maxReferants = partition.maxReferants;
    entries.push_back(we);

    currentMaxBytes += partition.maxBytes;
    currentMaxReferants += partition.maxReferants;
    currentCount++;
}

} // namespace
