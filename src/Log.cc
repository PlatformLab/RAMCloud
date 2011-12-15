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
#include <exception>

#include "Log.h"
#include "LogCleaner.h"
#include "ServerRpcPool.h"
#include "ShortMacros.h"
#include "TransportManager.h" // for Log memory 0-copy registration hack

namespace RAMCloud {


/**
 * Constructor for Log.
 * \param[in] logId
 *      A unique numerical identifier for this Log. This should be globally
 *      unique in the RAMCloud system.
 * \param[in] logCapacity
 *      Total size of the Log in bytes.
 * \param[in] segmentCapacity
 *      Size of each Segment that will be used in this Log in bytes.
 * \param[in] maximumBytesPerAppend
 *      The maximum number of bytes that will ever be appended to this
 *      log in a single append operation.
 * \param[in] replicaManager
 *      The ReplicaManager that will be used to make each of this Log's
 *      Segments durable.
 * \param[in] cleanerOption
 *      Cleaner option from the Log::CleanerOption enum. This lets the
 *      user of this object specify whether to use a cleaner, as well as
 *      whether to run it in a separate thread or inlined with the Log
 *      code.
 * \throw LogException
 *      An exception is thrown if #logCapacity is not sufficient for
 *      a single segment's worth of log.
 */
Log::Log(const ServerId& logId,
         uint64_t logCapacity,
         uint32_t segmentCapacity,
         uint32_t maximumBytesPerAppend,
         ReplicaManager *replicaManager,
         CleanerOption cleanerOption)
    : stats(),
      logCapacity((logCapacity / segmentCapacity) * segmentCapacity),
      segmentCapacity(segmentCapacity),
      maximumBytesPerAppend(maximumBytesPerAppend),
      logId(logId),
      segmentMemory(this->logCapacity),
      nextSegmentId(0),
      head(NULL),
      emergencyCleanerList(),
      freeList(),
      cleanableNewList(),
      cleanableList(),
      cleaningIntoList(),
      cleanablePendingDigestList(),
      freePendingDigestAndReferenceList(),
      freePendingReferenceList(),
      activeIdMap(),
      activeBaseAddressMap(),
      logTypeMap(),
      listLock(),
      replicaManager(replicaManager),
      cleanerOption(cleanerOption),
      cleaner(this, replicaManager,
              cleanerOption == CONCURRENT_CLEANER)
{
    if (logCapacity == 0) {
        throw LogException(HERE,
                           "insufficient Log memory for even one segment!");
    }

    // This doesn't include the LogDigest, but is a reasonable sanity check.
    if (Segment::maximumAppendableBytes(segmentCapacity) <
      maximumBytesPerAppend) {
        throw LogException(HERE, "maximumBytesPerAppend too large "
            "for given segmentCapacity");
    }

    for (uint64_t i = 0; i < logCapacity / segmentCapacity; i++) {
        locklessAddToFreeList(static_cast<char*>(segmentMemory.get()) +
            i * segmentCapacity);
    }

    Context::get().transportManager->registerMemory(segmentMemory.get(),
                                                    segmentMemory.length);
}

/**
 * Clean up after the Log.
 */
Log::~Log()
{
    cleaner.halt();

    foreach (LogTypeMap::value_type& typeCallbackPair, logTypeMap)
        delete typeCallbackPair.second;

    cleanableNewList.clear_and_dispose(SegmentDisposer());
    cleanableList.clear_and_dispose(SegmentDisposer());
    cleanablePendingDigestList.clear_and_dispose(SegmentDisposer());
    freePendingDigestAndReferenceList.clear_and_dispose(SegmentDisposer());
    freePendingReferenceList.clear_and_dispose(SegmentDisposer());

    if (head)
        delete head;
}

/**
 * Allocate a new head Segment and write the LogDigest before returning.
 * The current head is closed and replaced with the new one.  All the
 * usual log durability constraints are enforced by the underlying
 * ReplicaManager for safety during the transition to the new head.
 *
 * As we think about allowing concurrent workers code should
 * move to using an interface more like allocateHeadIfStillOn().
 *
 * \throw LogOutOfMemoryException
 *      If no Segments are free.
 */
void
Log::allocateHead()
{
    Lock lock(listLock);
    allocateHeadInternal(lock, {});
}

/**
 * Allocate a new head Segment and write the LogDigest before returning if
 * the provided \a segmentId is still the current log head.
 * The current head is closed and replaced with the new one.  All the
 * usual log durability constraints are enforced by the underlying
 * ReplicaManager for safety during the transition to the new head.
 *
 * \param segmentId
 *      Only allocate a new log head if the current log head is the one
 *      specified.  This is used to prevent useless allocations in the
 *      case that multiple callers try to allocate new log heads at the
 *      same time.
 * \throw LogOutOfMemoryException
 *      If no Segments are free.
 */
void
Log::allocateHeadIfStillOn(uint64_t segmentId)
{
    Lock lock(listLock);
    allocateHeadInternal(lock, { segmentId });
}

/**
 * Determine whether or not the provided Segment identifier is currently
 * live. A live Segment is one that is still being used by the Log for
 * storage. This method can be used to determine if data once written to the
 * Log is no longer present in the RAMCloud system and hence will not appear
 * again during either normal operation or recovery.
 * \param[in] segmentId
 *      The Segment identifier to check for liveness.
 */
bool
Log::isSegmentLive(uint64_t segmentId)
{
    TEST_LOG("%lu", segmentId);
    std::lock_guard<SpinLock> lock(listLock);
    return (activeIdMap.find(segmentId) != activeIdMap.end());
}

/**
 * Given a live pointer to data in the Log provided by #append, obtain the
 * identifier of the Segment into which it points. The identifier can be later
 * checked for liveness using the #isSegmentLive method.
 * \param[in] p
 *      A pointer to anywhere within a live Segment of the Log, as provided
 *      by #append.
 * \throw LogException
 *      An exception is thrown if the pointer provided does not pointer into
 *      a live Log segment.
 */
uint64_t
Log::getSegmentId(const void *p)
{
    return getSegmentFromAddress(p)->getId();
}

/**
 * Append typed data to the Log and obtain a pointer to its identical Log
 * copy.
 * \param[in] type
 *      The type of entry to append. All types except LOG_ENTRY_TYPE_SEGFOOTER
 *      are permitted.
 * \param[in] buffer
 *      Data to be appended to this Segment.
 * \param[in] length
 *      Length of the data to be appended in bytes. This must be sufficiently
 *      small to fit within one Segment's worth of memory.
 * \param[in] sync
 *      If true then this write to replicated to backups before return,
 *      otherwise the replication will happen on a subsequent append()
 *      where sync is true or when the segment is closed.  This defaults
 *      to true.
 * \param[in] expectedChecksum
 *      The checksum we expect this entry to have once appended. If the
 *      actual calculated checksum does not match, an exception is
 *      thrown and nothing is appended. This parameter is optional.
 * \return
 *      A LogEntryHandle is returned, which points to the ``buffer''
 *      written. The handle is guaranteed to be valid, i.e. non-NULL.
 * \throw LogException
 *      An exception is thrown if the append is too large to fit in any
 *      one Segment of the Log.
 * \throw LogOutOfMemoryException
 *      This exception is thrown if the Log is full.
 */
LogEntryHandle
Log::append(LogEntryType type, const void *buffer, const uint32_t length,
    bool sync, Tub<SegmentChecksum::ResultType> expectedChecksum)
{
    if (length > maximumBytesPerAppend) {
        throw LogException(HERE, format("append length (%d) exceeds "
                "maximum allowed (%d)", length, maximumBytesPerAppend));
    }

    LogMultiAppendVector appends;
    appends.push_back({ type, buffer, length, expectedChecksum });
    SegmentEntryHandleVector handles = multiAppend(appends, sync);
    assert(handles.size() == 1);
    return handles[0];
}

LogEntryHandleVector
Log::multiAppend(LogMultiAppendVector& appends, bool sync)
{
    SegmentEntryHandleVector handles;
    bool allocatedHead = false;

    for (size_t i = 0; i < appends.size(); i++) {
        assert(getTypeInfo(appends[i].type) != NULL);
        if (appends[i].length > maximumBytesPerAppend) {
            throw LogException(HERE, format("append length (%d) exceeds "
                "maximum allowed (%d)", appends[i].length,
                maximumBytesPerAppend));
        }
    }

    do {
        if (head != NULL)
            handles = head->multiAppend(appends, sync);

        // If either the head Segment is full, or we've never allocated one,
        // get a new head.
        if (handles.size() == 0) {
            // If we couldn't fit in the old head and allocated a new one and
            // still cannot fit, then the request is simply too big.
            if (allocatedHead) {
                uint32_t appendSize = 0;
                for (size_t i = 0; i < appends.size(); i++)
                    appendSize += appends[i].length;
                throw LogException(HERE,
                   format("WARNING: multiAppend of length %u simply won't "
                          "fit in segment of size %u: object(s) too large",
                          appendSize,
                          maximumBytesPerAppend));
            }

            // allocateHead could throw if we're low on segments (we need to
            // keep spares so that the cleaner can make forward progress).
            try {
                allocateHead();
                allocatedHead = true;
            } catch (LogOutOfMemoryException& e) {
                if (cleanerOption == INLINED_CLEANER)
                    cleaner.clean();
                throw e;
            }
        }
    } while (handles.size() == 0);

    assert(handles.size() == appends.size());

    for (size_t i = 0; i < handles.size(); i++) {
        stats.totalAppends++;
        stats.totalBytesAppended += handles[i]->totalLength();
    }

    if (cleanerOption == INLINED_CLEANER)
        cleaner.clean();

    return handles;
}

/**
 * Mark bytes in Log as freed. This simply maintains a per-Segment tally that
 * can be used to compute utilisation of individual Log Segments.
 * \param[in] entry
 *      A LogEntryHandle as returned by an #append call.
 * \throw LogException
 *      An exception is thrown if the pointer provided is not valid.
 */
void
Log::free(LogEntryHandle entry)
{
    Segment* s = getSegmentFromAddress(reinterpret_cast<const void*>(entry));

    // This debug-only check should catch invalid free()s within legitimate
    // Segments.
    assert(entry->isChecksumValid());
    assert(getTypeInfo(entry->type()) != NULL);
    assert(getTypeInfo(entry->type())->explicitlyFreed);

    s->free(entry);
    stats.totalFrees++;
    stats.totalBytesFreed += entry->totalLength();
}

/**
 * Register a type with the Log. Types are used to differentiate data written
 * to the Log. When Segments are cleaned, all entries are scanned and the
 * relocation callback for each is fired to notify the owner that the data
 * previously appended will be removed from the system. Is it up to the
 * callback to re-append it to the Log and invalidate pointers to the old
 * location.
 *
 * Types that are not registered with the Log are simply purged during
 * cleaning. 
 *
 * \param[in] type
 *      The type to be registered with the Log. Types may only be registered
 *      once.
 * \param[in] explicitlyFreed
 *      Set to true if the user of this Log will explicitly free space
 *      when an entry is no longer need (via the #free method). If set
 *      to false, the LogCleaner will periodically invoke the liveness
 *      callback to determine which entries of this type are no longer
 *      in use.
 * \param[in] livenessCB
 *      The liveness callback to be registered with the provided type.
 *      The callback takes a handle to an entry of this type and must
 *      return true if the entry is still in use (live), or false if it
 *      is not and can be discarded.
 * \param[in] livenessArg
 *      A void* argument to be passed to the liveness callback.
 * \param[in] relocationCB
 *      The relocation callback to be registered with the provided type.
 *      The callback is invoked by the cleaner and takes a handle to an
 *      existing entry that will expire after the callback returns, and
 *      a handle to a new copy that will continue to exist. The callback
 *      must determine if the entry is still live. If it is, references
 *      must be switched to the new handle and the method must return
 *      true. If not, the method simply returns false and both handles
 *      are garbage collected. 
 * \param[in] relocationArg
 *      A void* argument to be passed to the relocation callback.
 * \param[in] timestampCB
 *      The callback to determine the modification time of entries of this
 *      type in RAMCloud seconds (see #secondsTimestamp).
 * \throw LogException
 *      An exception is thrown if the type has already been registered
 *      or if the parameters given are invalid.
 */
void
Log::registerType(LogEntryType type,
                  bool explicitlyFreed,
                  log_liveness_cb_t livenessCB,
                  void *livenessArg,
                  log_relocation_cb_t relocationCB,
                  void *relocationArg,
                  log_timestamp_cb_t timestampCB)
{
    if (contains(logTypeMap, type))
        throw LogException(HERE, "type already registered with the Log");

    if (!explicitlyFreed && livenessCB == NULL) {
        throw LogException(HERE, "types not explicitly freed require a "
            "liveness callback");
    }

    logTypeMap[type] = new LogTypeInfo(type,
                                       explicitlyFreed,
                                       livenessCB,
                                       livenessArg,
                                       relocationCB,
                                       relocationArg,
                                       timestampCB);
}

/**
 * Return the information that was registered with a specific type. This
 * includes callbacks, among other state.
 *
 * \param[in] type
 *      The type registered with the log.
 * \return
 *      NULL if 'type' was not registered, else a pointer to the
 *      associated LogTypeCallback.
 */
const LogTypeInfo*
Log::getTypeInfo(LogEntryType type)
{
    if (contains(logTypeMap, type))
        return logTypeMap[type];

    return NULL;
}

/**
 * Wait for all segments to be fully replicated.
 */
void
Log::sync()
{
    if (head)
        head->sync();
}

/**
 * Return total bytes concatenated to the Log so far including overhead.
 */
uint64_t
Log::getBytesAppended() const
{
    return stats.totalBytesAppended;
}

/**
 * Return the total number of bytes freed in the Log. Like #getBytesAppended,
 * this number includes metadata overheads.
 */
uint64_t
Log::getBytesFreed() const
{
    return stats.totalBytesFreed;
}

/**
 * Obtain Segment backing memory from the free list. This is only supposed
 * to be used by the LogCleaner.
 *
 * \param useEmergencyReserve
 *      When true, the cleaner is aware that we're tight on memory and will
 *      be allocated free segments from the emergency reserve pool. If false,
 *      allocate from the common free list as normal.
 *
 * \return
 *      On success, a pointer to Segment backing memory of #segmentCapacity
 *      bytes, as provided in the #addSegmentMemory method. If the boolean
 *      useEmergencyReserve is true, return NULL on failure instead of
 *      throwing an exception.
 *
 * \throw LogOutOfMemoryException
 *      If memory is exhausted.
 */
void *
Log::getSegmentMemoryForCleaning(bool useEmergencyReserve)
{
    if (useEmergencyReserve) {
        std::lock_guard<SpinLock> lock(listLock);

        if (emergencyCleanerList.empty())
            return NULL;

        void* ret = emergencyCleanerList.back();
        emergencyCleanerList.pop_back();
        return ret;
    }

    Lock lock(listLock);
    return getFromFreeList(lock, false);
}

/**
 * Obtain the number of free segment memory blocks left in the system.
 */
size_t
Log::freeListCount()
{
    std::lock_guard<SpinLock> lock(listLock);

    // We always save one for the next Log head, so adjust accordingly.
    return (freeList.size() > 0) ? freeList.size() - 1 : freeList.size();
}

/**
 * Obtain a list of the Segments newly created since the last call to
 * this method. This is used by the cleaner to obtain candidate Segments
 * that have been closed and are eligible for cleaning at any time.
 *
 * \param[out] out
 *      Vector of pointers with which to return the new cleanable Segments.
 */
void
Log::getNewCleanableSegments(SegmentVector& out)
{
    std::lock_guard<SpinLock> lock(listLock);

    while (!cleanableNewList.empty()) {
        Segment& s = cleanableNewList.front();
        cleanableNewList.pop_front();
        cleanableList.push_back(s);
        out.push_back(&s);
    }
}

/**
 * Alert the Log of a Segment that is about to contain live data. This
 * is used by the cleaner prior to relocating entries to new Segments.
 * The Log needs to be made aware of such Segments because it needs to
 * be able to handle #free calls on relocated objects, which can occur
 * immediately following the cleaner's relocation and before it completes
 * a cleaning pass.
 *
 * \param[in] segment
 *      Pointer to the Segment that will shortly host live data.
 */
void
Log::cleaningInto(Segment* segment)
{
    std::lock_guard<SpinLock> lock(listLock);

    cleaningIntoList.push_back(*segment);
    activeIdMap[segment->getId()] = segment;
    activeBaseAddressMap[segment->getBaseAddress()] = segment;
}

/**
 * Alert the Log that the following Segments have been cleaned. The Log
 * takes note of the newly cleaned Segments and any new Segments reported
 * via the #cleaningInto method. When the next head is allocated in
 * #allocateHead, the LogDigest will be updated to reflect all of the new
 * Segments and none of the cleaned ones. This method also checks previously
 * cleaned Segments and any that have been removed from the Log and are no
 * longer be referenced by outstanding RPCs are returned to the free list.
 *
 * \param[in] clean
 *      Vector of pointers to Segments that have been cleaned.
 *
 * \param[in] unusedSegmentMemory
 *      Vector of pointers to segment memory that were allocated for
 *      cleaning via #getSegmentMemoryForCleaning, but were not used. These
 *      will be immediately returned to the free list.
 */
void
Log::cleaningComplete(SegmentVector& clean,
                      std::vector<void*>& unusedSegmentMemory)
{
    std::lock_guard<SpinLock> lock(listLock);
    bool change = false;

    // Return any unused segment memory the cleaner ended up
    // not needing directly to the free list.
    while (!unusedSegmentMemory.empty()) {
        locklessAddToFreeList(unusedSegmentMemory.back());
        unusedSegmentMemory.pop_back();
    }

    // New Segments we've added during cleaning need to wait
    // until the next head is written before they become part
    // of the Log.
    while (!cleaningIntoList.empty()) {
        Segment& s = cleaningIntoList.front();
        cleaningIntoList.pop_front();
        cleanablePendingDigestList.push_back(s);
        change = true;
    }

    // Increment the current epoch and save the last epoch any
    // RPC could have been a part of so we can store it with
    // the Segments to be freed.
    uint64_t epoch = ServerRpcPool<>::incrementCurrentEpoch() - 1;

    // Cleaned Segments must wait until the next digest has been
    // written (i.e. the new Segments we cleaned into are part of
    // of the Log and the cleaned ones are not) and no outstanding
    // RPCs could reference the data in those Segments before they
    // may be cleaned.
    foreach (Segment* s, clean) {
        s->cleanedEpoch = epoch;
        cleanableList.erase(cleanableList.iterator_to(*s));
        freePendingDigestAndReferenceList.push_back(*s);
        change = true;
    }

    // This is a good time to check cleaned Segments that are no
    // longer part of the Log, but may or may not still be referenced
    // by outstanding RPCs.
    uint64_t earliestEpoch = ServerRpcPool<>::getEarliestOutstandingEpoch();
    SegmentVector freeSegments;
    foreach (Segment& s, freePendingReferenceList) {
        if (s.cleanedEpoch < earliestEpoch) {
            // not sure about mutating an intrusive list while iterating, so...
            freeSegments.push_back(&s);
        }
    }
    while (freeSegments.size() > 0) {
        Segment* s = freeSegments.back();
        freeSegments.pop_back();
        activeIdMap.erase(s->getId());
        activeBaseAddressMap.erase(s->getBaseAddress());
        freePendingReferenceList.erase(
            freePendingReferenceList.iterator_to(*s));
        locklessAddToFreeList(const_cast<void*>(s->getBaseAddress()));
        s->freeReplicas();
        delete s;
        change = true;
    }

    if (change)
        dumpListStats();
}

/**
 * Allocate a unique Segment identifier. This is used to generate identifiers
 * for new Segments of the Log.
 *
 * \returns
 *      The next valid Segment identifier.
 */
uint64_t
Log::allocateSegmentId()
{
    // TODO(Rumble): could just be an atomic op
    std::lock_guard<SpinLock> lock(listLock);
    return nextSegmentId++;
}

////////////////////////////////////
/// Private Methods
////////////////////////////////////

/**
 * Allocate a new head Segment and write the LogDigest before returning if
 * the optionally provided \a segmentId is still the current log head.
 * The current head is closed and replaced with the new one.  All the
 * usual log durability constraints are enforced by the underlying
 * ReplicaManager for safety during the transition to the new head.
 *
 * \param lock
 *      Not used; just here to prove that the caller at least acquired some
 *      lock, if not the correct one.  \a lock should own the lock on
 *      #listLock.  This provides consistency of the lists but also
 *      ensures two threads aren't trying to allocate a new head at the same
 *      time.
 * \param segmentId
 *      Only allocate a new log head if the current log head is the one
 *      specified.  If \a segmentId is empty a new head is allocated
 *      regardless of which segment is currently the log head.  This is
 *      used to prevent useless allocations in the case that multiple
 *      callers try to allocate new log heads at the same time.
 * \throw LogOutOfMemoryException
 *      If no Segments are free.
 */
void
Log::allocateHeadInternal(Lock& lock, Tub<uint64_t> segmentId)
{
    if (head && segmentId && head->getId() != *segmentId)
        return;

    // these currently also take listLock, so rather than have
    // unlocked versions of those methods or duplicating code,
    // just do them before taking the big lock for this method.
    void* baseAddress = getFromFreeList(lock, true);

    // NB: Allocate an ID _after_ having acquired memory. If we don't,
    //     the allocation could fail and we've just leaked a segment
    //     identifier (feel free to read the comments in allocateSegmentId
    //     if you don't know why this is bad.
    LogDigest::SegmentId newHeadId = nextSegmentId++;

    if (head != NULL)
        cleanableNewList.push_back(*head);

    // new Log head + active Segments + cleaner pending Segments
    size_t segmentCount = 1;
    segmentCount += cleanableList.size() + cleanableNewList.size();
    segmentCount += cleanablePendingDigestList.size();

    size_t digestBytes = LogDigest::getBytesFromCount(segmentCount);
    char temp[digestBytes];
    LogDigest digest(segmentCount, temp, digestBytes);

    while (!cleanablePendingDigestList.empty()) {
        Segment& s = cleanablePendingDigestList.front();
        cleanablePendingDigestList.pop_front();
        cleanableNewList.push_back(s);
    }

    foreach (Segment& s, cleanableList)
        digest.addSegment(downCast<LogDigest::SegmentId>(s.getId()));

    foreach (Segment& s, cleanableNewList)
        digest.addSegment(downCast<LogDigest::SegmentId>(s.getId()));

    digest.addSegment(newHeadId);

    while (!freePendingDigestAndReferenceList.empty()) {
        Segment& s = freePendingDigestAndReferenceList.front();
        freePendingDigestAndReferenceList.pop_front();
        freePendingReferenceList.push_back(s);
    }

    Segment* nextHead = new Segment(this, true, newHeadId, baseAddress,
        segmentCapacity, replicaManager, LOG_ENTRY_TYPE_LOGDIGEST, temp,
        downCast<uint32_t>(digestBytes));

    activeIdMap[nextHead->getId()] = nextHead;
    activeBaseAddressMap[nextHead->getBaseAddress()] = nextHead;

    // only close the old head _after_ we've opened up the new head!
    if (head) {
        head->close(nextHead, false); // an exception here would be problematic.
    }

    head = nextHead;
}

/**
 * Print various Segment list counts to the debug log.
 */
void
Log::dumpListStats()
{
    LogLevel level = DEBUG;

    double total = static_cast<double>(getNumberOfSegments());
    LOG(level, "============ LOG LIST OCCUPANCY ============");
    LOG(level, "  freeList:                           %6Zd  (%.2f%%)",
        freeList.size(),
        100.0 * static_cast<double>(freeList.size()) / total);
    LOG(level, "  emergencyCleanerList:               %6Zd  (%.2f%%)",
        emergencyCleanerList.size(),
        100.0 * static_cast<double>(emergencyCleanerList.size()) / total);
    LOG(level, "  cleanableNewList:                   %6Zd  (%.2f%%)",
        cleanableNewList.size(),
        100.0 * static_cast<double>(cleanableNewList.size()) / total);
    LOG(level, "  cleanableList:                      %6Zd  (%.2f%%)",
        cleanableList.size(),
        100.0 * static_cast<double>(cleanableList.size()) / total);
    LOG(level, "  cleaningIntoList:                   %6Zd  (%.2f%%)",
        cleaningIntoList.size(),
        100.0 * static_cast<double>(cleaningIntoList.size()) / total);
    LOG(level, "  cleanablePendingDigestList:         %6Zd  (%.2f%%)",
        cleanablePendingDigestList.size(),
        100.0 * static_cast<double>(cleanablePendingDigestList.size()) / total);
    LOG(level, "  freePendingDigestAndReferenceList:  %6Zd  (%.2f%%)",
        freePendingDigestAndReferenceList.size(),
        100.0 * static_cast<double>(
            freePendingDigestAndReferenceList.size()) / total);
    LOG(level, "  freePendingReferenceList:           %6Zd  (%.2f%%)",
        freePendingReferenceList.size(),
        1.00 * static_cast<double>(freePendingReferenceList.size()) / total);
    LOG(level, "----- Total: %Zd (Segments in Log incl. head: %Zd)",
        freeList.size() +
        emergencyCleanerList.size() +
        cleanableNewList.size() +
        cleanableList.size() +
        cleaningIntoList.size() +
        cleanablePendingDigestList.size() +
        freePendingDigestAndReferenceList.size() +
        freePendingReferenceList.size(),
        getNumberOfSegments());
}

/**
 * Add a single contiguous piece of backing Segment memory to the Log's
 * free list. The memory provided must of at least as large as
 * #segmentCapacity.
 *
 * Note that the Log stashes extra segments for the cleaner to use during low
 * memory situations. If this list is under a high watermark, the memory
 * provided may be added to that list instead of the free list.
 *
 * \param[in] p
 *      Memory to be added to the Log for use as segments.
 */
void
Log::locklessAddToFreeList(void *p)
{
    if (freeList.size() == 0 ||
      emergencyCleanerList.size() == EMERGENCY_CLEAN_SEGMENTS) {
        freeList.push_back(p);
    } else {
        emergencyCleanerList.push_back(p);
    }
}

/**
 * Obtain Segment backing memory from the free list. This method ensures that
 * the last remaining free segment is not allocated unless explicitly asked
 * for. The last Segment is important because it's needed to make forward
 * progress with respect to the cleaner (i.e. a new log digest must be
 * written out before more segments can be freed).
 *
 * Note that this method is only intended to be used by the LogCleaner and
 * within #allocateHead.
 *
 * \param mayUseLastSegment
 *      This parameter only affects allocation if there is just one free
 *      block of segment memory and the cleaner is enabled. The following
 *      explanation assumes this scenario.
 *
 *      If set to false, an exception is always thrown. Only code allocating
 *      a new log head should set this to true.
 *
 *      If true, then only return the last segment if allocating it for the
 *      new head would eventually free sufficient additional segments to make
 *      forward progress. What constitutes sufficient depends on the state of
 *      the lists. A new head cannot be allocated if doing so will not make
 *      at least one more segment cleanable (thus allowing another segment to
 *      be cleaned in order to free up a new head). Furthermore, if the cleaner
 *      is operating under severe memory pressure and is using the emergency
 *      reserve of segments, a new head must not be allocated unless doing so
 *      would replenish the emergency reserve back to its full capacity. The
 *      cleaner ensures that it will only take from the emergency pool if it
 *      can clean enough segments to both replenish it and provide at least
 *      one more clean segment to the log to use for new appends. 
 *
 * \return
 *      On success, a pointer to Segment backing memory of #segmentCapacity
 *      bytes, as provided in the #addSegmentMemory method.
 *
 * \throw LogOutOfMemoryException
 *      If memory is exhausted.
 */
void *
Log::getFromFreeList(bool mayUseLastSegment)
{
    Lock lock(listLock);
    return getFromFreeList(lock, mayUseLastSegment);
}

void *
Log::getFromFreeList(Lock& lock, bool mayUseLastSegment)
{
    if (freeList.empty())
        throw LogOutOfMemoryException(HERE, "Log is out of space");

    if (freeList.size() == 1 && cleanerOption != CLEANER_DISABLED) {
        if (!mayUseLastSegment) {
            throw LogOutOfMemoryException(HERE, "Log is out of space "
                "(last one is reserved for the next head)");
        }

        // The next check essentially ensures that an emergency cleaning
        // pass has completed before we permit the last segment to be used.
        // The cleaner will guarantee that it generates enough segments to
        // both recoup the emergency segments used and to produce at least
        // one extra for the log to make forward progress.

        if (emergencyCleanerList.size() +
          freePendingDigestAndReferenceList.size() <
          (EMERGENCY_CLEAN_SEGMENTS + 1)) {
            throw LogOutOfMemoryException(HERE, "Log is out of space "
                "(cannot allocate last segment because doing so would not "
                "replenish the emergency pool)");
        }
    }

    void *p = freeList.back();
    freeList.pop_back();

    return p;
}

/**
 * Given a pointer into the backing memory of some Segment, return
 * the Segment object associated with it.
 *
 * \throw LogException 
 *      An exception is thrown if no corresponding Segment could be
 *      found (i.e. the pointer is bogus).
 */
Segment*
Log::getSegmentFromAddress(const void* address)
{
    const void *base = Segment::getSegmentBaseAddress(address, segmentCapacity);

    std::lock_guard<SpinLock> lock(listLock);

    if (head != NULL && base == head->getBaseAddress())
        return head;

    BaseAddressMap::const_iterator it = activeBaseAddressMap.find(base);
    if (it == activeBaseAddressMap.end())
        throw LogException(HERE, "getSegmentId on invalid pointer");

    return it->second;
}

/**
 * Obtain the 64-bit identifier assigned to this Log.
 */
uint64_t
Log::getId() const
{
    return logId.getId();
}

/**
 * Obtain the capacity of this Log in bytes.
 */
uint64_t
Log::getCapacity() const
{
    return logCapacity;
}

/**
 * Obtain the capacity of the Log Segments in bytes.
 */
uint32_t
Log::getSegmentCapacity() const
{
    return segmentCapacity;
}

/**
 * Obtain the maximum number of Segments in the Log.
 */
size_t
Log::getNumberOfSegments() const
{
    return logCapacity / segmentCapacity;
}

////////////////////////////////////
// LogStats subclass
////////////////////////////////////

Log::LogStats::LogStats()
    : totalBytesAppended(0),
      totalAppends(0),
      totalBytesFreed(0),
      totalFrees(0)
{
}

uint64_t
Log::LogStats::getBytesAppended() const
{
    return totalBytesAppended;
}

uint64_t
Log::LogStats::getAppends() const
{
    return totalAppends;
}

uint64_t
Log::LogStats::getBytesFreed() const
{
    return totalBytesFreed;
}

uint64_t
Log::LogStats::getFrees() const
{
    return totalFrees;
}

} // namespace
