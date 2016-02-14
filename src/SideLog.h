/* Copyright (c) 2012-2016 Stanford University
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

#ifndef RAMCLOUD_SIDELOG_H
#define RAMCLOUD_SIDELOG_H

#include "AbstractLog.h"
#include "SegmentManager.h"
#include "Log.h"
#include "SpinLock.h"
#include "ReplicaManager.h"

namespace RAMCloud {

/**
 * This class is used to efficiently write many entries to a specific log (see
 * the Log class) in bulk. Normally, the log has objects appended to it one or
 * perhaps a handful at a time by MasterService. The log is generally synced
 * after each operation, ensuring that replicas have been updated (and the
 * entry or entries therefore made persistent) before a client request is
 * replied to. When a head segment fills up, a new head is allocated by the
 * ReplicaManager in a very particular way that is safe, ensuring that after
 * failure the log can either be properly recovered, or data loss discovered --
 * no ambiguity between those two should be possible.
 *
 * This head segment transition essentially creates a barrier synchronization
 * scenario every time a new head is allocated. For normal operation, when only
 * small numbers of entries are synchronously appended this is not a problem.
 * However, when very many entries are written, such as during master recovery,
 * this adversely impacts performance.
 *
 * This class provides a special mechanism for writing large numbers of entries
 * into a log very efficiently. It works by allowing entries to be appended to
 * an anonymous (unnamed) log that has no constraints on the order of segment
 * replication operations. Once all of the entries have been appended, this
 * anonymous log can be "committed" -- synced to backups and then fused on to
 * the master's log so that the data added becomes recoverable. The lack of
 * ordering constraints makes replication more efficient by avoiding straggler
 * backups every time a segment fills and another is allocated.
 *
 * What happens under the hood is that segments are allocated in memory and
 * backups on demand as entries are appended to the SideLog. These segments,
 * however, are not included in the master log's digest and so will not be used
 * during recovery. This class keeps track of all such segments, and, when the
 * commit() method is invoked, first ensures that all segments have finished
 * replicating to backups and then fuses the segments into the master's log.
 * This fusion is achieved by simply rolling over to a new head segment that
 * includes the SideLog's new segments.
 *
 * One can also think of this like an atomic transaction. When commit() is
 * called, all appends are committed at once. If commit() is not called and the
 * SideLog's destructor is invoked, the operation is aborted and the
 * segments are freed both in memory and on disk.
 */
class SideLog : public AbstractLog {
  public:
    explicit SideLog(Log* log);
    SideLog(Log* log, LogCleaner* cleaner);
    ~SideLog();
    void commit();

  PRIVATE:
    LogSegment* allocNextSegment(bool mustNotFail);

    /// Pointer to the log that this object will merge appended entries into if
    /// commit() is invoked. This is used to roll the head over after fusing
    /// entires appended in this class into the log. The constructor also takes
    /// advantage of this class being a friend class of Log by extracting the
    /// SegmentManager and ReplicaManager pointers.
    Log* log;

    /// This is simply a cache of log->segmentManager. Used to avoid peeking
    /// into log internals anywhere aside from the constructor.
    SegmentManager* segmentManager;

    /// This is simply a cache of log->replicaManager. Used to avoid peeking
    /// into log internals anywhere aside from the constructor.
    ReplicaManager* replicaManager;

    /// The segments allocated from segment manager in order to service append
    /// operations. On commit(), these will be closed, synced to backups, and
    /// submitted to the segment manager for inclusion in the log. The vector
    /// is emptied after each commit.
    LogSegmentVector segments;

    /// If true, allocate segments that have been reserved for cleaning. If
    /// false, segments will be allocated from the default pool of free memory.
    /// This is set to true only when the special constructor made for the log
    /// cleaner is invoked.
    const bool forCleaner;

    DISALLOW_COPY_AND_ASSIGN(SideLog);
};

} // namespace

#endif // !RAMCLOUD_SIDELOG_H
