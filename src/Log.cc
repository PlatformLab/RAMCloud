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

#include <assert.h>
#include <stdint.h>

#include "Log.h"
#include "LogCleaner.h"
#include "PerfStats.h"
#include "ServerConfig.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Constructor for Log. No segments are allocated in the constructor, so if
 * replicas are being used no backups will have been contacted yet and there
 * will be no durable evidence of this log having existed. Call sync() to
 * allocate and force the head segment (and others, perhaps) to backups.
 *
 * The reason for this behaviour is complicated. We want to ensure that the
 * log is made durable before assigning any tablets to the master, since we
 * want a lack of the log on disk to unambiguously mean data loss if the
 * coordinator thinks we have any tablets. However, the constructor cannot
 * allocate and replicate the first head, since there is the potential for
 * deadlock (transport manager isn't running yet, so we can't learn of any
 * backups from the coordinator).
 *
 * \param context
 *      Overall information about the RAMCloud server.
 * \param config
 *      The ServerConfig containing configuration options that affect this
 *      log instance. Rather than passing in various separate bits, the log
 *      will extract any parameters it needs from the global server config.
 * \param entryHandlers
 *      Class to query for various bits of per-object information. For instance,
 *      the log may want to know whether an object is still needed or if it can
 *      be garbage collected. Methods on the given class instance will be
 *      invoked to field such queries.
 * \param segmentManager
 *      The SegmentManager this log should allocate its head segments from.
 * \param replicaManager
 *      The ReplicaManager that will be used to make each of this Log's
 *      Segments durable.
 */
Log::Log(Context* context,
         const ServerConfig* config,
         LogEntryHandlers* entryHandlers,
         SegmentManager* segmentManager,
         ReplicaManager* replicaManager)
    : AbstractLog(entryHandlers,
                  segmentManager,
                  replicaManager,
                  config->segmentSize),
      context(context),
      cleaner(NULL),
      syncLock("Log::syncLock"),
      metrics()
{
    cleaner = new LogCleaner(context,
                             config,
                             *segmentManager,
                             *replicaManager,
                             *entryHandlers);
}

/**
 * Clean up after the Log.
 */
Log::~Log()
{
    delete cleaner;
}

/**
 * Enable the cleaner if it isn't already running.
 */
void
Log::enableCleaner()
{
    cleaner->start();
}

/**
 * Disable the cleaner if it's running. Blocks until the cleaner thread has
 * quiesced.
 */
void
Log::disableCleaner()
{
    cleaner->stop();
}

/**
 * Populate the given protocol buffer with various log metrics.
 *
 * \param[out] m
 *      The protocol buffer to fill with metrics.
 */
void
Log::getMetrics(ProtoBuf::LogMetrics& m)
{
    AbstractLog::getMetrics(m);
    m.set_total_sync_calls(metrics.totalSyncCalls);
    m.set_total_sync_ticks(metrics.totalSyncTicks);
    cleaner->getMetrics(*m.mutable_cleaner_metrics());
}

/**
 * Return the position of the current log head.
 */
LogPosition
Log::getHead() {
    Lock lock(appendLock);
    return LogPosition(head->id, head->getAppendedLength());
}

/**
 * Wait for all log appends made at the time this method is invoked to be fully
 * replicated to backups. If no appends have ever been done, this method will
 * allocate the first log head and sync it to backups.
 *
 * This method is thread-safe. If no sync operation is currently underway, the
 * caller will fully sync the log and return. Doing so will propagate any
 * appends done since the last sync, including those performed by other threads.
 *
 * If a sync operation is already underway, this method will block until it
 * completes. Afterwards it will either return immediately if a previous call
 * did the work for us, or, if more replication is needed, it will sync the full
 * log contents at that time, including any appends made since this method
 * started waiting. This lets us batch backup writes and improve throughput for
 * small entries.
 *
 * An alternative to batching writes would have been to pipeline replication
 * RPCs to backups. That would probably also work just fine, but results in
 * more RPCs and is more complicated (we'd need to keep track of various RPCs
 * that may arrive out-of-order). The log also currently operates strictly
 * in-order, so there'd be no opportunity for small writes to skip ahead of
 * large ones anyway.
 */
void
Log::sync()
{
    CycleCounter<uint64_t> __(&PerfStats::threadStats.logSyncCycles);

    Tub<Lock> lock;
    lock.construct(appendLock);
    metrics.totalSyncCalls++;

    // The only time 'head' should be NULL is after construction and before the
    // initial call to this method. Even if we run out of memory in the future,
    // head will remain valid.
    if (head == NULL) {
        assert(metrics.totalSyncCalls == 1);
        if (!allocNewWritableHead())
            throw FatalError(HERE, "Could not allocate initial head segment");
    }

    // Get the current log offset and assume this is the point we want to sync
    // to. It may include more data than our thread's previous appends due to
    // races between the append() call and this sync(), but it's both unlikely
    // and does not affect correctness. We simply may do a little more work
    // sometimes when the log really is synced up to our appends, but this logic
    // mistakes additional data from other threads' appends as stuff we care
    // about.
    uint32_t appendedLength = head->getAppendedLength();

    // Concurrent appends may cause the head segment to change while we wait
    // for another thread to finish syncing, so save the segment associated
    // with the appendedLength we just acquired.
    LogSegment* originalHead = head;

    // We have a consistent view of the current head segment, so drop the append
    // lock and grab the sync lock. This allows other writers to append to the
    // log while we wait. Once we grab the sync lock, take the append lock again
    // to ensure our new view of the head is consistent.
    lock.destroy();
    Lock _(syncLock);
    lock.construct(appendLock);

    // See if we still have work to do. It's possible that another thread
    // already did the syncing we needed for us.
    if (appendedLength > originalHead->syncedLength) {
        // Get the latest segment length and certificate. This allows us to
        // batch up other appends that came in while we were waiting.
        SegmentCertificate certificate;
        appendedLength = originalHead->getAppendedLength(&certificate);

        // Drop the append lock. We don't want to block other appending threads
        // while we sync.
        lock.destroy();

        originalHead->replicatedSegment->sync(appendedLength, &certificate);
        originalHead->syncedLength = appendedLength;
        TEST_LOG("log synced");
    } else {
        TEST_LOG("sync not needed: already fully replicated");
    }
}

/**
 * Force the log to roll over to a new head and return the new log position.
 * At the instant of the new head segment's creation, it will have the highest
 * segment identifier in the log. The log is guaranteed to be synced to this
 * point on backups.
 *
 * This method can be used to ensure that the head segment is advanced past any
 * survivor segments generated by the cleaner. This can be used to then
 * demarcate parts of the log that can and cannot contain certain data. For
 * example, when a tablet is migrated to a new machine, it rolls over the head
 * and records the position. All data in the tablet must exist at that position
 * or after it. If there were an old instance of the same tablet, we would not
 * want to recover that data if a failure occurrs. Fortunately, its data would
 * be at strictly lower positions in the log, so it's easy to filter during
 * recovery.
 */
LogPosition
Log::rollHeadOver()
{
    Lock lock(syncLock);
    Lock lock2(appendLock);

    // Allocate the new head and sync the log. This will ensure that the
    // position returned is stable on backups. This is paricularly important
    // for SideLog::commit(), which rolls the head over to inject a SideLog
    // into the main log (by adding segments to a new log digest and syncing
    // that to disk). See RAM-489.
    head = allocNextSegment(true);
    SegmentCertificate certificate;
    uint32_t appendedLength = head->getAppendedLength(&certificate);
    head->replicatedSegment->sync(appendedLength, &certificate);
    head->syncedLength = appendedLength;

    return LogPosition(head->id, head->getAppendedLength());
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

/**
 * Allocate a new head segment for the log. This is used by the AbstractLog
 * superclass when a new segment is needed.
 *
 * This method must be called with the AbstractLog's appendLock held.
 *
 * \param mustNotFail
 *      If true, this method must return a valid LogSegment pointer and may
 *      block as long as needed. If false, it should return immediately with
 *      a NULL pointer if no segments are available.
 */
LogSegment*
Log::allocNextSegment(bool mustNotFail)
{
    assert(!appendLock.try_lock());

    if (mustNotFail)
        return segmentManager->allocHeadSegment(SegmentManager::MUST_NOT_FAIL);
    else
        return segmentManager->allocHeadSegment();
}

} // namespace
