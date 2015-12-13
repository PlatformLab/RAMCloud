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

#ifndef RAMCLOUD_TABLESTATS_H
#define RAMCLOUD_TABLESTATS_H

#include <unordered_map>

#include "Common.h"
#include "SpinLock.h"
#include "Buffer.h"
#include "Tablet.h"

namespace RAMCloud {

/**
 * Forward declaration of MasterTableMetadata so that this type can be used as a
 * parameter.  MasterTableMetadata.h was not included here to avoid a circular
 * include since MasterTableMetadata.h includes this header file.
 */
class MasterTableMetadata;

/**
 * This namespace holds all relevant methods and structures to collect,
 * compress, store, and retrieve table stats information used to estimate and
 * provide tablet stats information.  This estimated tablet stats information is
 * then used to divide a master's tablets into partitions during crash recovery,
 * in order to facilitate "reliably fast" recovery.
 *
 * Since the number of tables on a given master is large and the amount of
 * space available to store this information is limited, the table stats
 * information needs to be compress before it is serialized into segments.  For
 * tables at least as large as a given THRESHOLD, exact information is kept.
 * For those "other" tables that fall below the THRESHOLD aggregate information
 * is kept.
 */
namespace TableStats {
/**
 * This structure represents a block of stats information for an individual
 * table on a given master.  One of these blocks is stored in each entry
 * of the MasterTableMetadata container.  Thread-safe access to this block
 * should be maintained by "acquiring" the block's SpinLock before accessing
 * the block's members.
 */
struct Block {
    SpinLock lock;          /// Aquire this monitor lock before member access.
    uint64_t keyHashCount;  /// Number of key hashes that reside on this master.
                            /// If a master has total ownership this value is
                            /// assumed to be 2^64.
    bool totalOwnership;    /// True if this master completely owns this table.
    uint64_t byteCount;     /// Number of bytes of data related to a table.
    uint64_t recordCount;   /// Number of log records related to a table.

    Block()
        : lock("TableStats::lock")
        , keyHashCount(0)
        , totalOwnership(false)
        , byteCount(0)
        , recordCount(0)
    {}
};

void addKeyHashRange(MasterTableMetadata* mtm,
                     uint64_t tableId,
                     uint64_t startKeyHash,
                     uint64_t endKeyHash);
void deleteKeyHashRange(MasterTableMetadata* mtm,
                        uint64_t tableId,
                        uint64_t startKeyHash,
                        uint64_t endKeyHash);
void increment(MasterTableMetadata* mtm,
               uint64_t tableId,
               uint64_t byteCount,
               uint64_t recordCount);
void decrement(MasterTableMetadata* mtm,
               uint64_t tableId,
               uint64_t byteCount,
               uint64_t recordCount);
void serialize(Buffer* buf, MasterTableMetadata *mtm);

/**
 * This threshold defines the size below which tables stats information will be
 * stored in aggregate.  If a table contains fewer bytes than this, then the
 * table's statistics are not recorded separately in the log; instead, a single
 * set of aggregated statistics is recorded for all sub-threshold tables.
 *
 * A threshold of 24MB was selected to both bound the max number of entries we
 * would store in the stats block and ensure that the error the results would be
 * "relatively small" compared to the target partition size.  Some assumed
 * values include:
 *      SegmentSize 8MB
 *      MasterCapacity 64GB
 *      StatsEntrySize 24B
 *      TargetPartitionSize 600MB
 * If we want to keep the stats block under 1% of segment usage we will show:
 *      MaxStatsBlockSize = SegmentSize /128 = 64KB
 * This means we should have always have fewer than MaxEntryCount number of
 * entries:
 *      MaxEntryCount = MaxStatsBlockSize / StatsEntrySize = ((64 * 1024) / 24)B
 * Thus the threshold should be:
 *      threshold = MasterCapacity / MaxEntryCount = 24MB
 * 24MB is also reasonably small compared to the target partition size.  It
 * should be noted that if the MasterCapasity grows we will either need to
 * increase the threshold or increase the number of entries we are willing to
 * store in order to compensate.
 */
const uint64_t threshold = 24*1024*1024;  // 24 MB.

/// Lock guard type used to hold the monitor spinlock and automatically
/// release it.
typedef std::lock_guard<SpinLock> Lock;

/**
 * Simple structure representing a stats information entry in the serialized
 * format of TableStats (aka TableStats::Digest).
 */
struct DigestEntry {
    /// Id of table whose stats are contained herein.
    uint64_t tableId;
    /// Avg num bytes per key hash for the tables with given tableId.
    double bytesPerKeyHash;
    /// Avg num log records per key hash for the tables with given tableId.
    double recordsPerKeyHash;
} __attribute__((__packed__));

/**
 * Simple structure defining the header of a TableStats::Digest structure.
 * Contains the aggregated stats for those "other" tables that are "small"
 * enough to fall below the threshold.  Also contains a count of how many
 * TableStats::Entry structures are serialized into the digest.
 */
struct DigestHeader {
    /// Avg num bytes per key hash for all "small" tables.
    double otherBytesPerKeyHash;
    /// Avg num log records per key hash for all "small" tables.
    double otherRecordsPerKeyHash;
    /// Number of DigestEntry structures in the Digest.
    uint64_t entryCount;
} __attribute__((__packed__));

/**
 * Represents the compressed and serialized form of table stats information.
 */
struct Digest {
    /// See DigestHeader struct.
    DigestHeader header;
    /// Memory location of entries immediately following this struct. Must be
    ///last member struct.
    DigestEntry entries[0];
} __attribute__((__packed__));

/**
 * Provides estimated tablet size and record count information.  Estimates are
 * only used to inform the tablet partitioning algorithm on the coordinator
 * during master crash recovery.
 */
class Estimator {
  PUBLIC:
    /**
     * Represents estimated statistics information for a single tablet.
     */
    struct Estimate {
        uint64_t byteCount;
        uint64_t recordCount;
    };

    explicit Estimator(const Digest* digest);
    Estimate estimate(Tablet *tablet);

    /// Flag indicating whether the estimator contains valid estimates
    bool valid;

  PRIVATE:
    /**
     * Represents the statistics information for a single table or a collection
     * of tables.  When representing a single table, the statistics represent
     * the aggregate of all tablets of said table on the crashed master. When
     * representing a collection of tables, the statistics represent the
     * aggregate of all tablets of all collected tables on the crashed master.
     */
    struct Entry {
        double bytesPerKeyHash;     /// Avg num bytes per key hash
        double recordsPerKeyHash;   /// Avg num log records per key hash
    };

    /// Type defining map between tableId and Estimator::Entry
    typedef std::unordered_map<uint64_t, Entry> StatsMap;

    /// Contains Entry information for each table.
    StatsMap tableStats;

    /// Contains cumulative Entry information for tables below threshold.
    Entry otherStats;
};

} // namespace TableStats

} // namespace RAMCloud

#endif /* RAMCLOUD_TABLESTATS_H */
