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

#include <assert.h>
#include <stdint.h>

#include "Log.h"
#include "LogCleaner.h"
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
         const ServerConfig& config,
         LogEntryHandlers& entryHandlers,
         SegmentManager& segmentManager,
         ReplicaManager& replicaManager)
    : context(context),
      entryHandlers(entryHandlers),
      segmentManager(segmentManager),
      replicaManager(replicaManager),
      cleaner(NULL),
      head(NULL),
      segmentSize(config.segmentSize),
      appendLock(),
      syncLock(),
      metrics()
{
    cleaner = new LogCleaner(context,
                             config,
                             segmentManager,
                             replicaManager,
                             entryHandlers);
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
    m.set_ticks_per_second(Cycles::perSecond());
    m.set_total_append_ticks(metrics.totalAppendTicks);
    m.set_total_sync_ticks(metrics.totalSyncTicks);
    m.set_total_no_space_ticks(metrics.totalNoSpaceTicks);
    m.set_total_bytes_appended(metrics.totalBytesAppended);
    m.set_total_metadata_bytes_appended(metrics.totalMetadataBytesAppended);

    cleaner->getMetrics(*m.mutable_cleaner_metrics());
    segmentManager.getMetrics(*m.mutable_segment_metrics());
    segmentManager.getAllocator().getMetrics(*m.mutable_seglet_metrics());
}

/**
 * Append multiple entries to the log atomically. This ensures that either
 * everything is written, or none of it is. Furthermore, recovery is
 * guaranteed to see either none or all of the entries.
 *
 * \param appends
 *      Array containing the entries to append. References to the entries
 *      are also returned here.
 * \param numAppends
 *      Number of entries in the appends array.
 * \return
 *      True if the append succeeded, false if there was insufficient space
 *      to complete the operation.
 */
bool
Log::append(AppendVector* appends, uint32_t numAppends)
{
    Lock lock(appendLock);

    uint32_t lengths[numAppends];
    for (uint32_t i = 0; i < numAppends; i++)
        lengths[i] = appends[i].buffer.getTotalLength();

    if (head == NULL || !head->hasSpaceFor(lengths, numAppends))
        head = segmentManager.allocHead(true);

    if (head == NULL || head->isEmergencyHead)
        return false;

    if (!head->hasSpaceFor(lengths, numAppends))
        throw LogException(HERE, "too much data to append to one segment");

    LogSegment* headBefore = head;
    for (uint32_t i = 0; i < numAppends; i++) {
        bool success = append(lock,
                              appends[i].type,
                              appends[i].timestamp,
                              appends[i].buffer,
                              &appends[i].reference);
        if (!success)
            throw FatalError(HERE, "Guaranteed append managed to fail");
    }
    assert(head == headBefore);

    return true;
}

/**
 * Mark bytes in log as freed. When a previously-appended entry is no longer
 * needed, this method may be used to notify the log that it may garbage
 * collect it.
 */
void
Log::free(Reference reference)
{
    uint32_t slot = reference.getSlot(segmentSize);
    uint32_t offset = reference.getOffset(segmentSize);
    LogSegment& segment = segmentManager[slot];
    Buffer buffer;
    LogEntryType type = segment.getEntry(offset, buffer);
    uint32_t timestamp = entryHandlers.getTimestamp(type, buffer);
    segment.statistics.decrement(buffer.getTotalLength(), timestamp);
}

/**
 * Given a reference to an entry previously appended to the log, return the
 * entry's type and fill in a buffer that points to the entry's contents.
 * This is the method to use to access something after it is appended to the
 * log.
 *
 * \param reference
 *      Reference to the entry requested. This value is returned in the append
 *      method. If this reference is invalid behaviour is undefined. The log
 *      will indicate when references become invalid via the LogEntryHandlers
 *      class.
 * \param outBuffer
 *      Buffer to append the entry being looked up to.
 * \return
 *      The type of the entry being looked up is returned here.
 */
LogEntryType
Log::getEntry(Reference reference, Buffer& outBuffer)
{
    uint32_t slot = reference.getSlot(segmentSize);
    uint32_t offset = reference.getOffset(segmentSize);
    return segmentManager[slot].getEntry(offset, outBuffer);
}

/**
 * Wait for all segment contents at the time this method is invoked to be fully
 * replicated to backups. If there has never been a head segment, allocate one
 * and sync it.
 *
 * This method is thread-safe. If no sync operation is currently underway, the
 * caller will fully sync the log and return. Doing so will propagate any
 * appends done since the last sync, including those performed by other threads.
 *
 * If a sync operation is underway, this method will block until it completes.
 * Afterwards it will either return immediately if a previous call did the work
 * for us, or if more replication is needed, it will sync the full log contents
 * at that time, including any appends made since this method started waiting.
 * This lets us batch backup writes and improve throughput for small entries.
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
    CycleCounter<uint64_t> __(&metrics.totalSyncTicks);

    Tub<Lock> lock;
    lock.construct(appendLock);

    // The only time 'head' should be NULL is after construction and before the
    // initial call to this method. Even if we run out of memory in the future,
    // head will remain valid.
    if (head == NULL) {
        head = segmentManager.allocHead(true);
        if (head == NULL)
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
        Segment::Certificate certificate;
        appendedLength = originalHead->getAppendedLength(certificate);

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
 * Given a reference to an appended entry, return the identifier of the segment
 * that contains the entry. An example use of this is tombstones, which mark
 * themselves with the segment id of the object they're deleting. When that
 * segment leaves the system, the tombstone may be garbage collected.
 */
uint64_t
Log::getSegmentId(Reference reference)
{
    uint32_t slot = reference.getSlot(segmentSize);
    return segmentManager[slot].id;
}

/**
 * Force the log to roll over to a new head and return the new log position.
 * At the instant of the new head segment's creation, it will have the highest
 * segment identifier in the log.
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
Log::Position
Log::rollHeadOver()
{
    Lock lock(appendLock);
    head = segmentManager.allocHead(true);
    return Position(head->id, head->getAppendedLength());
}

/**
 * Check if a segment is still in the system. This method can be used to
 * determine if data once written to the log is no longer present in the
 * RAMCloud system and hence will not appear again during either normal
 * operation or recovery. Tombstones use this to determine when they are
 * eligible for garbage collection.
 *
 * \param segmentId
 *      The Segment identifier to check for liveness.
 * \return
 *      True if the given segment is present in the log, otherwise false.
 */
bool
Log::containsSegment(uint64_t segmentId)
{
    TEST_LOG("%lu", segmentId);
    return segmentManager.doesIdExist(segmentId);
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

/**
 * Append a typed entry to the log by coping in the data. Entries are binary
 * blobs described by a simple <type, length> tuple.
 *
 * Note that the append operation is not synchronous. To ensure that the data
 * appended has been safely written to backups, the sync() method must be
 * invoked after appending.
 *
 * \param appendLock
 *      The append lock must have already been acquired before entering this
 *      method. This parameter exists in the hopes that you don't forget to
 *      do that.
 * \param type
 *      Type of the entry. See LogEntryTypes.h.
 * \param timestamp
 *      Creation time of the entry, as provided by WallTime. This is used by the
 *      log cleaner to choose more optimal segments to garbage collect from.
 * \param buffer
 *      Pointer to buffer containing the entry to be appended.
 * \param length
 *      Number of bytes to append from the provided buffer.
 * \param[out] outReference
 *      If the append succeeds, a reference to the created entry is returned
 *      here. This reference may be used to access the appended entry via the
 *      lookup method. It may also be inserted into a HashTable.
 * \return
 *      True if the append succeeded, false if there was either insufficient
 *      space to complete the operation.
 */
bool
Log::append(Lock& appendLock,
            LogEntryType type,
            uint32_t timestamp,
            const void* buffer,
            uint32_t length,
            Reference* outReference)
{
    CycleCounter<uint64_t> _(&metrics.totalAppendTicks);

    // This is only possible once after construction.
    if (head == NULL) {
        head = segmentManager.allocHead(false);
        if (head == NULL)
            throw FatalError(HERE, "Could not allocate initial head segment");
    }

    // Try to append. If we can't, try to allocate a new head to get more space.
    uint32_t segmentOffset;
    uint32_t bytesUsedBefore = head->getAppendedLength();
    bool success = head->append(type, buffer, length, &segmentOffset);
    if (!success) {
        LogSegment* newHead = segmentManager.allocHead(false);
        if (newHead != NULL)
            head = newHead;

        // If we're entirely out of memory or were allocated an emergency head
        // segment due to memory pressure, we can't service the append. Return
        // failure and let the client retry. Hopefully the cleaner will free up
        // more memory soon.
        if (newHead == NULL || head->isEmergencyHead) {
            if (!metrics.noSpaceTimer)
                metrics.noSpaceTimer.construct(&metrics.totalNoSpaceTicks);
            return false;
        }

        bytesUsedBefore = head->getAppendedLength();
        if (!head->append(type, buffer, length, &segmentOffset)) {
            // TODO(Steve): We should probably just permit up to 1/N'th of the
            // size of a segment in any single append. Say, 1/2th or 1/4th as
            // a ceiling. Then we could ensure that after opening a new head
            // we have at least as much space, or else throw a fatal error.
            LOG(ERROR, "Entry too big to append to log: %u bytes of type %d",
                length, static_cast<int>(type));
            throw FatalError(HERE, "Entry too big to append to log");
        }
    }

    if (metrics.noSpaceTimer)
        metrics.noSpaceTimer.destroy();

    if (outReference != NULL)
        *outReference = Reference(head->slot, segmentOffset, segmentSize);

    head->statistics.increment(head->getAppendedLength() - bytesUsedBefore,
                               timestamp);

    metrics.totalBytesAppended += length;
    metrics.totalMetadataBytesAppended +=
        (head->getAppendedLength() - bytesUsedBefore) - length;

    return true;
}

/**
 * Append a typed entry to the log by coping in the data. Entries are binary
 * blobs described by a simple <type, length> tuple.
 *
 * Note that the append operation is not synchronous. To ensure that the data
 * appended has been safely written to backups, the sync() method must be
 * invoked after appending.
 *
 * \param appendLock
 *      The append lock must have already been acquired before entering this
 *      method. This parameter exists in the hopes that you don't forget to
 *      do that.
 * \param type
 *      Type of the entry. See LogEntryTypes.h.
 * \param timestamp
 *      Creation time of the entry, as provided by WallTime. This is used by the
 *      log cleaner to choose more optimal segments to garbage collect from.
 * \param buffer
 *      Buffer object describing the entry to be appended.
 * \param[out] outReference
 *      If the append succeeds, a reference to the created entry is returned
 *      here. This reference may be used to access the appended entry via the
 *      lookup method. It may also be inserted into a HashTable.
 * \return
 *      True if the append succeeded, false if there was either insufficient
 *      space to complete the operation or the requested append was larger
 *      than the system supports.
 */
bool
Log::append(Lock& appendLock,
            LogEntryType type,
            uint32_t timestamp,
            Buffer& buffer,
            Reference* outReference)
{
    return append(appendLock,
                  type,
                  timestamp,
                  buffer.getRange(0, buffer.getTotalLength()),
                  buffer.getTotalLength(),
                  outReference);
}

} // namespace
