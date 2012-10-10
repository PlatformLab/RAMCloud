/* Copyright (c) 2009-2012 Stanford University
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

#ifndef RAMCLOUD_LOG_H
#define RAMCLOUD_LOG_H

#include <stdint.h>
#include <unordered_map>
#include <vector>

#include "BoostIntrusive.h"
#include "LogCleaner.h"
#include "LogEntryTypes.h"
#include "LogEntryHandlers.h"
#include "Segment.h"
#include "SegmentManager.h"
#include "LogSegment.h"
#include "SpinLock.h"
#include "ReplicaManager.h"
#include "HashTable.h"

#include "LogMetrics.pb.h"

namespace RAMCloud {

class ServerConfig;

/**
 * An exception that is thrown when the Log class is provided invalid
 * method arguments.
 */
struct LogException : public Exception {
    LogException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
};

/**
 * The log provides a replicated store for immutable and relocatable data in
 * a master server's memory. Data is stored by appending typed "entries" to the
 * log. Entries are simply <type, length> tuples and associated opaque data
 * blobs. Once written, they may not be later modified. However, they may be
 * freed and the space later reclaimed by a special garbage collection mechanism
 * called the "cleaner".
 *
 * The cleaner requires that entries be relocatable to deal with fragmentation.
 * That is, it may decide to copy an entry to another location in memory and
 * tell the module that appended it to update any references and stop using the
 * old location. A set of callbacks are invoked by the cleaner to test if
 * entries are still alive to and notify the user of the log when an entry has
 * been moved to another log location. See the LogEntryHandlers interface for
 * more details.
 *
 * This particular class provides a simple, thin interface for users of logs.
 * Much of the internals, most of which have to deal with replication and
 * cleaning, are handled by a suite of related classes such as Segment,
 * SegmentManager, LogCleaner, ReplicaManager, and BackupFailureMonitor.
 */
class Log {
  public:
    /**
     * Position is a (Segment Id, Segment Offset) tuple that represents a
     * position in the log. For example, it can be considered the logical time
     * at which something was appended to the Log. It can be used for things
     * like computing table partitions and obtaining a master's current log
     * position.
     */
    class Position {
      public:
        /**
         * Default constructor that creates a zeroed position. This refers to
         * the very beginning of a log.
         */
        Position()
            : pos(0, 0)
        {
        }

        /**
         * Construct a position given a segment identifier and offset within
         * the segment.
         */
        Position(uint64_t segmentId, uint64_t segmentOffset)
            : pos(segmentId, downCast<uint32_t>(segmentOffset))
        {
        }

        bool operator==(const Position& other) const {return pos == other.pos;}
        bool operator!=(const Position& other) const {return pos != other.pos;}
        bool operator< (const Position& other) const {return pos <  other.pos;}
        bool operator<=(const Position& other) const {return pos <= other.pos;}
        bool operator> (const Position& other) const {return pos >  other.pos;}
        bool operator>=(const Position& other) const {return pos >= other.pos;}

        /**
         * Return the segment identifier component of this position object.
         */
        uint64_t getSegmentId() const { return pos.first; }

        /**
         * Return the offset component of this position object.
         */
        uint32_t getSegmentOffset() const { return pos.second; }

      PRIVATE:
        std::pair<uint64_t, uint32_t> pos;
    };

    Log(Context* context,
        const ServerConfig& config,
        LogEntryHandlers& entryHandlers,
        SegmentManager& segmentManager,
        ReplicaManager& replicaManager);
    ~Log();

    void enableCleaner();
    void disableCleaner();
    void getMetrics(ProtoBuf::LogMetrics& m);
    bool append(LogEntryType type,
                uint32_t timestamp,
                const void* data,
                uint32_t length,
                bool sync,
                HashTable::Reference* outReference = NULL);
    bool append(LogEntryType type,
                uint32_t timestamp,
                Buffer& buffer,
                bool sync,
                HashTable::Reference* outReference = NULL);
    void free(HashTable::Reference reference);
    LogEntryType getEntry(HashTable::Reference reference,
                          Buffer& outBuffer);
    void sync();
    uint64_t getSegmentId(HashTable::Reference reference);
    Log::Position rollHeadOver();
    bool containsSegment(uint64_t segmentId);

  PRIVATE:
    INTRUSIVE_LIST_TYPEDEF(LogSegment, listEntries) SegmentList;
    typedef std::lock_guard<SpinLock> Lock;

    HashTable::Reference buildReference(uint32_t slot, uint32_t offset);
    uint32_t referenceToSlot(HashTable::Reference reference);
    uint32_t referenceToOffset(HashTable::Reference reference);

    /// Shared RAMCloud information.
    Context* context;

    /// Various handlers for entries appended to this log. Used to obtain
    /// timestamps, check liveness, and notify of entry relocation during
    /// cleaning.
    LogEntryHandlers& entryHandlers;

    /// The SegmentManager allocates and keeps track of our segments. It
    /// also mediates mutation of the log between this class and the
    /// LogCleaner.
    SegmentManager& segmentManager;

    /// Class responsible for handling the durability of segments. Segment
    /// objects don't themselves have any concept of replication, but the
    /// Log and SegmentManager classes ensure that the data is replicated
    /// consistently nonetheless.
    ReplicaManager& replicaManager;

    /// The garbage collector that will remove dead entries from the log in
    /// parallel with normal operation. Upon construction it will be in a
    /// stopped state. A call to enableCleaner() will be needed to kick it
    /// into action and it may later be disabled via the disableCleaner()
    /// method.
    LogCleaner cleaner;

    /// Current head of the log. Whatever this points to is owned by
    /// SegmentManager, which is responsible for its eventual deallocation.
    /// This pointer should never be NULL.
    LogSegment* head;

    /// Lock taken around log append operations. This is currently only used
    /// to delay appends to the log head while migration is underway.
    SpinLock appendLock;

    /// Various event counters and performance measurements taken during log
    /// operation.
    class Metrics {
      public:
        Metrics() :
            totalAppendTicks(0),
            totalSyncTicks(0),
            totalNoSpaceTicks(0),
            noSpaceTimer(),
            totalBytesAppended(0),
            totalMetadataBytesAppended(0)
        {
        }

        /// Total number of cpu cycles spent appending data. Includes any
        /// synchronous replication time, but does not include waiting for
        /// the log lock.
        uint64_t totalAppendTicks;

        /// Total number of cpu cycles spent syncing appended log entries.
        uint64_t totalSyncTicks;

        /// Total number of ticks spent out of memory and unable to service
        /// append operations.
        uint64_t totalNoSpaceTicks;

        /// Timer used to measure how long the log has spent unable to allocate
        /// memory. Constructed when we transition from being able to append to
        /// not, and destructed when we can append again.
        Tub<CycleCounter<uint64_t>> noSpaceTimer;

        /// Total number of useful user bytes appended to the log. This does not
        /// include any segment metadata.
        uint64_t totalBytesAppended;

        /// Total number of metadata bytes appended to the log. This, plus the
        /// #totalBytesAppended value is equal to the grand total of bytes
        /// appended to the log.
        uint64_t totalMetadataBytesAppended;
    } metrics;

    friend class LogIterator;

    DISALLOW_COPY_AND_ASSIGN(Log);
};

} // namespace

#endif // !RAMCLOUD_LOG_H
