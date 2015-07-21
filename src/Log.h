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

#ifndef RAMCLOUD_LOG_H
#define RAMCLOUD_LOG_H

#include <stdint.h>
#include <unordered_map>
#include <vector>

#include "AbstractLog.h"
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
 * old location. Callbacks are invoked by the cleaner to move entries and update
 * references.  See the LogEntryHandlers interface for more details.
 *
 * This particular class provides a simple, thin interface for users of logs.
 * Much of the internals, most of which have to deal with replication and
 * cleaning, are handled by a suite of related classes such as AbstractLog,
 * Segment, SegmentManager, LogCleaner, ReplicaManager, and
 * BackupFailureMonitor.
 *
 * Note that appending an entry does not guarantee that it has been durably
 * replicated. If the data must be made durable before continuing, code must
 * explicitly invoke the sync() method to flush all previous appends to backups.
 *
 * This class is thread-safe. Multiple threads may invoke append() in parallel,
 * but all appends are serialized by a single SpinLock. The sync() method will
 * batch multiple append operations to backups to improve throughput, especially
 * when individual entries are small.
 */
class Log : public AbstractLog {
  public:
    typedef std::lock_guard<SpinLock> Lock;

    Log(Context* context,
        const ServerConfig* config,
        LogEntryHandlers* entryHandlers,
        SegmentManager* segmentManager,
        ReplicaManager* replicaManager);
    ~Log();

    void enableCleaner();
    void disableCleaner();
    LogPosition getHead();
    void getMetrics(ProtoBuf::LogMetrics& m);
    void sync();
    LogPosition rollHeadOver();

  PRIVATE:
    LogSegment* allocNextSegment(bool mustNotFail);

    INTRUSIVE_LIST_TYPEDEF(LogSegment, listEntries) SegmentList;

    /// Shared RAMCloud information.
    Context* context;

    /// The garbage collector that will remove dead entries from the log in
    /// parallel with normal operation. Upon construction it will be in a
    /// stopped state. A call to enableCleaner() will be needed to kick it
    /// into action and it may later be disabled via the disableCleaner()
    /// method.
    LogCleaner* cleaner;

    /// Lock used to serialize calls to ReplicatedSegment::sync(). This both
    /// protects the ReplicatedSegment from concurrent access and queues up
    /// syncs in the log so that multiple appends can be flushed to backups
    /// in the same RPC.
    ///
    /// If both this lock and the AbstractLog::appendLock need to be taken,
    /// this one must be acquired first to avoid deadlock.
    SpinLock syncLock;

    /// Various event counters and performance measurements taken during log
    /// operation.
    class Metrics {
      public:
        Metrics()
            : totalSyncCalls(0)
            , totalSyncTicks(0)
        {
        }

        /// Total number of times sync() has been called.
        uint64_t totalSyncCalls;

        /// Total number of cpu cycles spent syncing appended log entries.
        uint64_t totalSyncTicks;
    } metrics;

    friend class LogIterator;
    friend class SideLog;
    friend class CleanerCompactionBenchmark;
    friend class ObjectManagerBenchmark;

    DISALLOW_COPY_AND_ASSIGN(Log);
};

} // namespace

#endif // !RAMCLOUD_LOG_H
