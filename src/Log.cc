/* Copyright (c) 2009, 2010 Stanford University
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
 * \param[in] backup
 *      The BackupManager that will be used to make each of this Log's
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
Log::Log(const Tub<uint64_t>& logId,
         uint64_t logCapacity,
         uint32_t segmentCapacity,
         BackupManager *backup,
         CleanerOption cleanerOption)
    : stats(),
      logId(logId),
      logCapacity((logCapacity / segmentCapacity) * segmentCapacity),
      segmentCapacity(segmentCapacity),
      segmentMemory(this->logCapacity),
      nextSegmentId(0),
      maximumAppendableBytes(0),
      head(NULL),
      freeList(),
      cleanableNewList(),
      cleanableList(),
      cleaningIntoList(),
      cleanablePendingDigestList(),
      freePendingDigestAndReferenceList(),
      freePendingReferenceList(),
      activeIdMap(),
      activeBaseAddressMap(),
      callbackMap(),
      listLock(),
      backup(backup),
      cleanerOption(cleanerOption),
      cleaner(this,
              (backup == NULL) ? NULL : new BackupManager(backup),
              cleanerOption == CONCURRENT_CLEANER)
{
    if (logCapacity == 0) {
        throw LogException(HERE,
                           "insufficient Log memory for even one segment!");
    }
    for (uint64_t i = 0; i < logCapacity / segmentCapacity; i++) {
        addSegmentMemory(static_cast<char*>(segmentMemory.get()) +
                         i * segmentCapacity);
    }
    transportManager.registerMemory(segmentMemory.get(), segmentMemory.length);
}

/**
 * Clean up after the Log.
 */
Log::~Log()
{
    cleaner.halt();

    foreach (CallbackMap::value_type& typeCallbackPair, callbackMap)
        delete typeCallbackPair.second;

    cleanableNewList.clear_and_dispose(SegmentDisposer());
    cleanableList.clear_and_dispose(SegmentDisposer());
    cleanablePendingDigestList.clear_and_dispose(SegmentDisposer());
    freePendingDigestAndReferenceList.clear_and_dispose(SegmentDisposer());
    freePendingReferenceList.clear_and_dispose(SegmentDisposer());

    if (head) {
        head->close();
        delete head;
    }
}

/**
 * Allocate a new head Segment and write the LogDigest before returning.
 * The current head is closed and replaced with the new one, though closure
 * does not occur until after the new head has been opened on backups.
 *
 * \throw LogException
 *      If no Segments are free.
 */
void
Log::allocateHead()
{
    // these currently also take listLock, so rather than have
    // unlocked versions of those methods or duplicating code,
    // just do them before taking the big lock for this method.
    LogDigest::SegmentId newHeadId = allocateSegmentId();
    void* baseAddress = getFromFreeList();

    boost::lock_guard<SpinLock> lock(listLock);

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

    Segment* nextHead = new Segment(this, newHeadId, baseAddress,
        segmentCapacity, backup, LOG_ENTRY_TYPE_LOGDIGEST, temp,
        downCast<uint32_t>(digestBytes));

    activeIdMap[nextHead->getId()] = nextHead;
    activeBaseAddressMap[nextHead->getBaseAddress()] = nextHead;

    // only close the old head _after_ we've opened up the new head!
    if (head) {
        head->close(false); // an exception here would be problematic...
#ifdef PERF_DEBUG_RECOVERY_SYNC_BACKUP
        head->sync();
#endif
    }

    head = nextHead;
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
    boost::lock_guard<SpinLock> lock(listLock);
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
 *      An exception is thrown if the append exceeds the maximum permitted
 *      append length, as returned by #getMaximumAppendableBytes, or the log
 *      ran out of space.
 */
LogEntryHandle
Log::append(LogEntryType type, const void *buffer, const uint64_t length,
    bool sync, Tub<SegmentChecksum::ResultType> expectedChecksum)
{
    if (length > maximumAppendableBytes)
        throw LogException(HERE, "append exceeded maximum possible length");

    SegmentEntryHandle seh = NULL;

    do {
        if (head != NULL) {
            seh = head->append(type, buffer, downCast<uint32_t>(length),
                sync, expectedChecksum);
        }

        // if either the head Segment is full, or we've never allocated one,
        // get a new head.
        if (seh == NULL)
            allocateHead();
    } while (seh == NULL);

    stats.totalAppends++;
    stats.totalBytesAppended += seh->totalLength();

    if (cleanerOption == INLINED_CLEANER)
        cleaner.clean();

    return seh;
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
 * \param[in] livenessCB
 *      The liveness callback to be registered with the provided type.
 * \param[in] livenessArg
 *      A void* argument to be passed to the liveness callback.
 * \param[in] relocationCB
 *      The relocation callback to be registered with the provided type.
 * \param[in] relocationArg
 *      A void* argument to be passed to the relocation callback.
 * \param[in] timestampCB
 *      The callback to determine the modification time of objects of this
 *      type in RAMCloud seconds (see #secondsTimestamp).
 * \param[in] scanCB
 *      A callback that is invoked on entries in log order. The callback is
 *      fired on all entries at least once. If an object has been relocated
 *      by the cleaner, it is subject to another callback.
 * \param[in] scanArg
 *      A void* argument to be passed to the scan callback.
 * \throw LogException
 *      An exception is thrown if the type has already been registered.
 */
void
Log::registerType(LogEntryType type,
                  log_liveness_cb_t livenessCB,
                  void *livenessArg,
                  log_relocation_cb_t relocationCB,
                  void *relocationArg,
                  log_timestamp_cb_t timestampCB,
                  log_scan_cb_t scanCB,
                  void *scanArg)
{
    if (contains(callbackMap, type))
        throw LogException(HERE, "type already registered with the Log");

    callbackMap[type] = new LogTypeCallback(type,
                                            livenessCB,
                                            livenessArg,
                                            relocationCB,
                                            relocationArg,
                                            timestampCB,
                                            scanCB,
                                            scanArg);
}

/**
 * Return the callbacks associated with a particular type.
 *
 * \param[in] type
 *      The type registered with the log.
 * \return
 *      NULL if 'type' was not registered, else a pointer to the
 *      associated LogTypeCallback.
 */
const LogTypeCallback*
Log::getCallbacks(LogEntryType type)
{
    if (contains(callbackMap, type))
        return callbackMap[type];

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
 * Obtain the maximum number of bytes that can ever be appended to the
 * Log at once. Appends that exceed this maximum will throw an exception.
 */
uint64_t
Log::getMaximumAppendableBytes() const
{
    return maximumAppendableBytes;
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
 * Obtain Segment backing memory from the free list.
 * \return
 *      On success, a pointer to Segment backing memory of #segmentCapacity
 *      bytes, as provided in the #addSegmentMemory method.
 * \throw LogException
 *      If memory is exhausted.
 */
void *
Log::getFromFreeList()
{
    boost::lock_guard<SpinLock> lock(listLock);

    if (freeList.empty())
        throw LogException(HERE, "Log is out of space");

    void *p = freeList.back();
    freeList.pop_back();

    return p;
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
    boost::lock_guard<SpinLock> lock(listLock);

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
    boost::lock_guard<SpinLock> lock(listLock);

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
 */
void
Log::cleaningComplete(SegmentVector& clean)
{
    boost::lock_guard<SpinLock> lock(listLock);

    debugDumpLists();

    // New Segments we've added during cleaning need to wait
    // until the next head is written before they become part
    // of the Log.
    while (!cleaningIntoList.empty()) {
        Segment& s = cleaningIntoList.front();
        cleaningIntoList.pop_front();
        cleanablePendingDigestList.push_back(s);
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
        freeList.push_back(const_cast<void*>(s->getBaseAddress()));
        delete s;
    }
}

/**
 * Allocate a unique Segment identifier. This is used to generate identifiers
 * for new Segments of the Log.
 * \returns
 *      The next valid Segment identifier.
 */
uint64_t
Log::allocateSegmentId()
{
    // XXX- could just be an atomic op
    boost::lock_guard<SpinLock> lock(listLock);
    return nextSegmentId++;
}

////////////////////////////////////
/// Private Methods
////////////////////////////////////

/**
 * Print various Segment list counts to the debug log.
 */
void
Log::debugDumpLists()
{
    LOG(DEBUG, "============ LOG LIST OCCUPANCY ============");
    LOG(DEBUG, "  freeList:                           %Zd", freeList.size());
    LOG(DEBUG, "  cleanableNewList:                   %Zd",
        cleanableNewList.size());
    LOG(DEBUG, "  cleanableList:                      %Zd",
        cleanableList.size());
    LOG(DEBUG, "  cleaningIntoList:                   %Zd",
        cleaningIntoList.size());
    LOG(DEBUG, "  cleanablePendingDigestList:         %Zd",
        cleanablePendingDigestList.size());
    LOG(DEBUG, "  freePendingDigestAndReferenceList:  %Zd",
        freePendingDigestAndReferenceList.size());
    LOG(DEBUG, "  freePendingReferenceList:           %Zd",
        freePendingReferenceList.size());
    LOG(DEBUG, "----- Total: %Zd (Segments in Log (incl. head): %Zd)",
        freeList.size() +
        cleanableNewList.size() +
        cleanableList.size() +
        cleaningIntoList.size() +
        cleanablePendingDigestList.size() +
        freePendingDigestAndReferenceList.size() +
        freePendingReferenceList.size(),
        getNumberOfSegments());
}

/**
 * Provide the Log with a single contiguous piece of backing Segment memory.
 * The memory provided must of at least as large as #segmentCapacity. 
 * This function must be called once for each Segment.
 * \param[in] p
 *      Memory to be added to the Log for use as segments.
 */
void
Log::addSegmentMemory(void *p)
{
    boost::lock_guard<SpinLock> lock(listLock);

    freeList.push_back(p);

    if (maximumAppendableBytes == 0) {
        Segment s((uint64_t)0, 0, p, segmentCapacity);
        maximumAppendableBytes = s.appendableBytes(
            LogDigest::getBytesFromCount(getNumberOfSegments()));
    }
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

    boost::lock_guard<SpinLock> lock(listLock);

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
    return *logId;
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
