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

// Forward declare our way around header dependency fun.
class LogCleaner;

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

    /**
     * Data appended to the log is referenced by instances of this class.
     * We simply pack the entry's segment slot and byte offset within the
     * segment into a uint64_t that can go in the hash table. The slot is
     * maintained by SegmentManager and is an index into an array of active
     * segments in the system.
     *
     * Direct pointers are not used because log entries are not necessarily
     * contiguous in memory. This indirection also makes it easy to find the
     * segment metadata associated with a specific entry so that we can update
     * state (for example, the free space counts in a Log::free() call). In
     * the past we aligned segments in memory and ANDed off bits from the
     * entry pointer to address the segment itself, but this was messy and
     * convoluted.
     */
    class Reference {
      public:
        Reference()
            : value(0)
        {
        }

        Reference(SegmentManager::Slot slot,
                  uint32_t offset,
                  uint32_t segmentSize)
            : value(0)
        {
            assert(offset < segmentSize);
            assert(BitOps::isPowerOfTwo(segmentSize));
            int shift = BitOps::findFirstSet(segmentSize) - 1;
            value = (static_cast<uint64_t>(slot) << shift) | offset;
        }

        explicit Reference(uint64_t value)
            : value(value)
        {
        }

        /**
         * Obtain the 64-bit integer value of the reference. The upper 16 bits
         * are guaranteed to be zero.
         */
        uint64_t
        toInteger() const
        {
            return value;
        }

        /**
         * Compare references for equality. Returns true if equal, else false.
         */
        bool
        operator==(const Reference& other) const
        {
            return value == other.value;
        }

        /**
         * Returns the exact opposite of operator==.
         */
        bool
        operator!=(const Reference& other) const
        {
            return !operator==(other);
        }

        uint32_t
        getSlot(uint32_t segmentSize) const
        {
            assert(BitOps::isPowerOfTwo(segmentSize));
            int shift = BitOps::findFirstSet(segmentSize) - 1;
            return downCast<uint32_t>(value >> shift);
        }

        uint32_t
        getOffset(uint32_t segmentSize) const
        {
            assert(BitOps::isPowerOfTwo(segmentSize));
            return downCast<uint32_t>(value & (segmentSize - 1));
        }

      PRIVATE:
        /// The integer value of the reference.
        uint64_t value;
    };

    /**
     * Structure used when appending multiple entries atomically. It is used to
     * both describe the entry, as well as return the log reference once it has
     * been appended. Callers will typically allocate an array of these on the
     * stack, set up each individual entry, and pass its pointer to the append
     * method.
     */
    class AppendVector {
      public:
        /**
         * This default constructor simply initializes to invalid values.
         */
        AppendVector()
            : type(LOG_ENTRY_TYPE_INVALID),
              timestamp(0),
              buffer(),
              reference(0)
        {
        }

        /// Type of the entry to append (see LogEntryTypes.h).
        LogEntryType type;

        /// Creation timestamp of the entry (see WallTime.h).
        uint32_t timestamp;

        /// Buffer describing the contents of the entry to append.
        Buffer buffer;

        /// A log reference to the entry once appended is returned here.
        Reference reference;
    };

    typedef std::lock_guard<SpinLock> Lock;

    Log(Context* context,
        const ServerConfig& config,
        LogEntryHandlers& entryHandlers,
        SegmentManager& segmentManager,
        ReplicaManager& replicaManager);
    ~Log();

    void enableCleaner();
    void disableCleaner();
    void getMetrics(ProtoBuf::LogMetrics& m);
    bool append(AppendVector* appends, uint32_t numAppends);
    void free(Reference reference);
    LogEntryType getEntry(Reference reference,
                          Buffer& outBuffer);
    void sync();
    uint64_t getSegmentId(Reference reference);
    Log::Position rollHeadOver();
    bool segmentExists(uint64_t segmentId);

    /*
     * The following overloaded append() methods are for convenience. Fast path
     * code (like the heart of segment replay during recovery) use a single
     * contiguous const void* buffer when appending. Other paths tend to use
     * Buffers for convenience and to avoid extra copies.
     *
     * These methods all call a common private append() core that is optimized
     * for the single const void* case. They also acquire append locks, since
     * the core function is lockless (to support atomic appends of multiple
     * entries).
     */

    /**
     * \overload
     */
    bool
    append(LogEntryType type,
           uint32_t timestamp,
           const void* buffer,
           uint32_t length,
           Reference* outReference = NULL)
    {
        Lock lock(appendLock);
        return append(lock, type, timestamp, buffer, length, outReference);
    }

    /**
     * \overload
     */
    bool
    append(LogEntryType type,
           uint32_t timestamp,
           Buffer& buffer,
           Reference* outReference = NULL)
    {
        Lock lock(appendLock);
        return append(lock,
                      type,
                      timestamp,
                      buffer.getRange(0, buffer.getTotalLength()),
                      buffer.getTotalLength(),
                      outReference);
    }

  PRIVATE:
    INTRUSIVE_LIST_TYPEDEF(LogSegment, listEntries) SegmentList;

    bool append(Lock& lock,
                LogEntryType type,
                uint32_t timestamp,
                const void* data,
                uint32_t length,
                Reference* outReference = NULL);
    bool append(Lock& lock,
                LogEntryType type,
                uint32_t timestamp,
                Buffer& buffer,
                Reference* outReference = NULL);

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
    LogCleaner* cleaner;

    /// Current head of the log. Whatever this points to is owned by
    /// SegmentManager, which is responsible for its eventual deallocation.
    /// This pointer should never be NULL.
    LogSegment* head;

    /// The size of each full segment in bytes. This is the exact amount of
    /// space allocated for segments on backups and the maximum amount of
    /// space each memory segment may contain.
    uint32_t segmentSize;

    /// Lock taken around log append operations. This ensures that parallel
    /// writers do not modify the head segment concurrently. The sync()
    /// method also uses this lock to get a consistent view of the head
    /// segment in the presence of multiple appending threads.
    SpinLock appendLock;

    /// Lock used to serialize calls to ReplicatedSegment::sync(). This both
    /// protects the ReplicatedSegment from concurrent access and queues up
    /// syncs in the log so that multiple appends can be flushed to backups
    /// in the same RPC.
    SpinLock syncLock;

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
