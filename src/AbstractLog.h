/* Copyright (c) 2009-2015 Stanford University
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

#ifndef RAMCLOUD_ABSTRACTLOG_H
#define RAMCLOUD_ABSTRACTLOG_H

#include <stdint.h>
#include <unordered_map>
#include <vector>

#include "BoostIntrusive.h"
#include "LogEntryTypes.h"
#include "Segment.h"
#include "SpinLock.h"
#include "ReplicaManager.h"
#include "HashTable.h"

#include "LogMetrics.pb.h"

namespace RAMCloud {

/// LogEntryHandlers needs the AbstractLog::Reference definition.
class LogEntryHandlers;

/// LogSegment needs the AbstractLog::Reference definition as well.
class LogSegment;

/// Avoid yet another circular dependency.
class SegmentManager;

/**
 * This class implements functionality common to the Log and SideLog subclasses.
 * The Log class is typically used when servers store individual entries such as
 * new objects or tombstones. The SideLog is used when a large number of entries
 * need to be added to the log all at once with maximum backup performance. For
 * instance, recovery uses that class to efficiently re-replicate data from a
 * failed master during replay. See the Log and SideLog documentation for more
 * details.
 *
 * This class is thread-safe.
 */
class AbstractLog {
  public:
    /// Our append operations will return the same Segment::Reference. Since
    /// callers shouldn't have to know about Segments, we'll alias it here.
    typedef Segment::Reference Reference;

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
              buffer(),
              reference()
        {
        }

        /// Type of the entry to append (see LogEntryTypes.h).
        LogEntryType type;

        /// Buffer describing the contents of the entry to append.
        Buffer buffer;

        /// A log reference to the entry once appended is returned here.
        Reference reference;
    };

    /**
     * Interface for log reference freer.
     * freeLogEntry() should call AbstractLog::free() appropriately.
     * Actual implementer can add additional jobs or make this no-op for test.
     * Primarily used for garbage collection of RpcResult log entries.
     */
    class ReferenceFreer {
      public:
        virtual ~ReferenceFreer() {}
        virtual void freeLogEntry(Reference ref) = 0;
    };

    typedef std::lock_guard<SpinLock> Lock;

    AbstractLog(LogEntryHandlers* entryHandlers,
                SegmentManager* segmentManager,
                ReplicaManager* replicaManager,
                uint32_t segmentSize);
    virtual ~AbstractLog() { }

    bool append(AppendVector* appends, uint32_t numAppends);
    bool append(Buffer* logBuffer, Reference *references, uint32_t numEntries);
    void free(Reference reference);
    void getMetrics(ProtoBuf::LogMetrics& m);
    LogEntryType getEntry(Reference reference,
                          Buffer& outBuffer);
    uint64_t getSegmentId(Reference reference);
    bool hasSpaceFor(uint64_t objectSize);
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
           const void* buffer,
           uint32_t length,
           Reference* outReference = NULL)
    {
        Lock lock(appendLock);
        metrics.totalAppendCalls++;
        return append(lock,
                      type,
                      buffer,
                      length,
                      outReference,
                      &metrics.totalAppendTicks);
    }

    /**
     * \overload
     */
    bool
    append(LogEntryType type,
           Buffer& buffer,
           Reference* outReference = NULL)
    {
        Lock lock(appendLock);
        metrics.totalAppendCalls++;
        return append(lock,
                      type,
                      buffer.getRange(0, buffer.size()),
                      buffer.size(),
                      outReference,
                      &metrics.totalAppendTicks);
    }


  PROTECTED:
    LogSegment* getSegment(Reference reference);

    /**
     * This virtual method is used to allocate the next segment to append
     * entries to when either there was no previous one (immediately following
     * construction) or the previous one had insufficient space.
     *
     * The Log subclass' implementation of this allocates a new log head from
     * the SegmentManager. The SideClass class allocates a non-head segment
     * that is not yet part of the log from SegmentManager. This allows the
     * latter class to ignore any segment ordering constraints until it is
     * time to merge with the log proper.
     *
     * \param mustNotFail
     *      If true, this method must return a valid LogSegment pointer. It may
     *      block indefinitely if necessary. If false, the method must return
     *      immediately and may provide a NULL pointer if no segment is
     *      available.
     * \return
     *      A new LogSegment pointer. If mustNotFail is true, this is guaranteed
     *      to be non-NULL.
     */
    virtual LogSegment* allocNextSegment(bool mustNotFail) = 0;

    bool append(Lock& lock,
                LogEntryType type,
                const void* data,
                uint32_t length,
                Reference* outReference = NULL,
                uint64_t* outTickCounter = NULL);
    bool append(Lock& lock,
                const void* data,
                uint32_t *entryLength = NULL,
                Reference* outReference = NULL,
                uint64_t* outTickCounter = NULL);
    bool append(Lock& lock,
                LogEntryType type,
                Buffer& buffer,
                Reference* outReference = NULL,
                uint64_t* outTickCounter = NULL);
    bool allocNewWritableHead();

    /// Various handlers for entries appended to this log. Used to obtain
    /// timestamps and to relocate entries during cleaning.
    LogEntryHandlers* entryHandlers;

    /// The SegmentManager allocates and keeps track of our segments. It
    /// also mediates mutation of the log between this class and the
    /// LogCleaner.
    SegmentManager* segmentManager;

    /// Class responsible for handling the durability of segments. Segment
    /// objects don't themselves have any concept of replication, but the
    /// Log and SegmentManager classes ensure that the data is replicated
    /// consistently nonetheless.
    ReplicaManager* replicaManager;

    /// The size of each full segment in bytes. This is the exact amount of
    /// space allocated for segments on backups and the maximum amount of
    /// space each memory segment may contain.
    uint32_t segmentSize;

    /// Current segment being appended to. Whatever this points to is owned by
    /// SegmentManager, which is responsible for its eventual deallocation.
    /// This pointer will be initalized to NULL in the constructor, but once
    /// the first segment is allocated it will never be NULL again.
    ///
    /// In the Log subclass, this is the actual head of the log. In the SideLog
    /// subclass, there is not quite the same concept of a log head. In that
    /// case, this is simply the segment currently being appended to.
    LogSegment* head;

    /// Lock taken around log append operations. This ensures that parallel
    /// writers do not modify the head segment concurrently. The sync()
    /// method also uses this lock to get a consistent view of the head
    /// segment in the presence of multiple appending threads.
    SpinLock appendLock;

    // Total amount of log space occupied by long-term data such as
    // objects. Excludes data that can eventually be cleaned, such
    // as tombstones.
    uint64_t totalLiveBytes;

    // Largest value of totalLiveBytes that is "safe" (i.e. the cleaner
    // can always make progress).
    uint64_t maxLiveBytes;

    /// Various event counters and performance measurements taken during log
    /// operation.
    class Metrics {
      public:
        Metrics()
            : totalAppendCalls(0),
              totalAppendTicks(0),
              totalNoSpaceTicks(0),
              noSpaceTimer(),
              totalBytesAppended(0),
              totalMetadataBytesAppended(0)
        {
        }

        /**
         * Add all of the metrics this instance to the fields in \a other.
         * This method is kind of inverted (merges into the arg's fields) due to
         * weird issues with accessing protected fields through member pointers.
         */
        void mergeInto(AbstractLog* other)
        {
            other->metrics.totalAppendCalls += totalAppendCalls;
            other->metrics.totalAppendTicks += totalAppendTicks;
            other->metrics.totalNoSpaceTicks += totalNoSpaceTicks;
            other->metrics.totalBytesAppended += totalBytesAppended;
            other->metrics.totalMetadataBytesAppended +=
                totalMetadataBytesAppended;
        }

        /// Reset the metrics for the log to the initial/empty state.
        void reset()
        {
            this->~Metrics();
            new(this) Metrics{};
        }

        /// Total number of times any of the public append() methods have been
        /// called.
        uint64_t totalAppendCalls;

        /// Total number of cpu cycles spent appending data. Includes any
        /// synchronous replication time, but does not include waiting for
        /// the log lock.
        uint64_t totalAppendTicks;

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

    DISALLOW_COPY_AND_ASSIGN(AbstractLog);
};

} // namespace

#endif // !RAMCLOUD_LOG_H
