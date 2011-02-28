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
 * \throw LogException
 *      An exception is thrown if #logCapacity is not sufficient for
 *      a single segment's worth of log.
 */
Log::Log(const Tub<uint64_t>& logId,
         uint64_t logCapacity,
         uint64_t segmentCapacity,
         BackupManager *backup)
    : stats(),
      logId(logId),
      logCapacity((logCapacity / segmentCapacity) * segmentCapacity),
      segmentCapacity(segmentCapacity),
      segmentMemory(this->logCapacity),
      segmentFreeList(),
      nextSegmentId(0),
      maximumAppendableBytes(0),
      useCleaner(true),
      cleaner(this),
      head(NULL),
      callbackMap(),
      activeIdMap(),
      activeBaseAddressMap(),
      backup(backup)
{
    if (logCapacity == 0) {
        throw LogException(HERE,
                           "insufficient Log memory for even one segment!");
    }
    for (uint64_t i = 0; i < logCapacity / segmentCapacity; i++) {
        addSegmentMemory(static_cast<char*>(segmentMemory.get()) +
                         i * segmentCapacity);
    }
}

/**
 * Clean up after the Log.
 */
Log::~Log()
{
    foreach (ActiveIdMap::value_type& idSegmentPair, activeIdMap) {
        Segment* segment = idSegmentPair.second;
        if (segment == head)
            segment->close(true);
        delete segment;
    }

    foreach (CallbackMap::value_type& typeCallbackPair, callbackMap)
        delete typeCallbackPair.second;
}

/**
 * Allocate a new head Segment and write the LogDigest before returning.
 * The current head is not replaced; that is up to the caller.
 *
 * \throw LogException
 *      If no Segments are free.
 */
Segment*
Log::allocateHead()
{
    uint32_t segmentCount = downCast<uint32_t>(activeIdMap.size() + 1);
    uint32_t digestBytes = LogDigest::getBytesFromCount(segmentCount);
    char temp[digestBytes];
    LogDigest ld(segmentCount, temp, digestBytes);

    foreach (ActiveIdMap::value_type& idSegmentPair, activeIdMap) {
        Segment* segment = idSegmentPair.second;
        ld.addSegment(segment->getId());
    }

    uint64_t newHeadId = allocateSegmentId();
    ld.addSegment(newHeadId);

    return new Segment(this, newHeadId, getFromFreeList(),
        downCast<uint32_t>(segmentCapacity), backup,
        LOG_ENTRY_TYPE_LOGDIGEST, temp, digestBytes);
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
Log::isSegmentLive(uint64_t segmentId) const
{
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
    const void *base = getSegmentBaseAddress(p);

    BaseAddressMap::const_iterator it = activeBaseAddressMap.find(base);
    if (it == activeBaseAddressMap.end())
        throw LogException(HERE, "getSegmentId on invalid pointer");
    Segment *s = it->second;
    return s->getId();
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
 * \param[out] lengthInLog
 *      If non-NULL, the actual number of bytes consumed by this append to
 *      the Log is stored to this address. Note that this size includes all
 *      Log and Segment overheads, so it will be greater than the ``length''
 *      parameter.
 * \param[out] logTime
 *      If non-NULL, return the LogTime of this append operation. This is
 *      simply a (segmentId, segmentOffset) tuple that describes a logical
 *      time for this append. All subsequent appends will have a later
 *      LogTime.
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
    uint64_t *lengthInLog, LogTime *logTime, bool sync,
    Tub<SegmentChecksum::ResultType> expectedChecksum)
{
    if (length > maximumAppendableBytes)
        throw LogException(HERE, "append exceeded maximum possible length");

    SegmentEntryHandle seh;
    uint64_t segmentOffset;

    if (head != NULL) {
        seh = head->append(type, buffer,
                           downCast<uint32_t>(length), lengthInLog,
                           &segmentOffset, sync, expectedChecksum);
        if (seh != NULL) {
            // entry was appended to head segment
            if (logTime != NULL)
                *logTime = LogTime(head->getId(), segmentOffset);
            stats.totalAppends++;
            return seh;
        }
        // head segment is full
        // allocate the next segment, /then/ close this one
        Segment* nextHead = allocateHead();

        head->close(false); // an exception here would be problematic...
#ifdef PERF_DEBUG_RECOVERY_SYNC_BACKUP
        head->sync();
#endif
        head = nextHead;
    } else {
        // allocate the first segment
        head = allocateHead();
    }
    addToActiveMaps(head);

    // append the entry
    seh = head->append(type, buffer,
                       downCast<uint32_t>(length), lengthInLog,
                       &segmentOffset, sync, expectedChecksum);
    assert(seh != NULL);
    if (logTime != NULL)
        *logTime = LogTime(head->getId(), segmentOffset);

    if (useCleaner)
        cleaner.clean(1);
    stats.totalAppends++;
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
    const void *base = getSegmentBaseAddress(
        reinterpret_cast<const void*>(entry));

    BaseAddressMap::const_iterator it = activeBaseAddressMap.find(base);
    if (it == activeBaseAddressMap.end())
        throw LogException(HERE, "free on invalid pointer");
    Segment *s = it->second;
    s->free(entry);
    stats.totalFrees++;
}

/**
 * Register a type with the Log. Types are used to differentiate data written
 * to the Log. When Segments are cleaned, all entries are scanned and the
 * eviction callback for each is fired to notify the owner that the data
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
 * \param[in] evictionCB
 *      The eviction callback to be registered with the provided type.
 * \param[in] evictionArg
 *      A void* argument to be passed to the eviction callback.
 * \throw LogException
 *      An exception is thrown if the type has already been registered.
 */
void
Log::registerType(LogEntryType type,
                  log_eviction_cb_t evictionCB, void *evictionArg)
{
    if (contains(callbackMap, type))
        throw LogException(HERE, "type already registered with the Log");

    callbackMap[type] = new LogTypeCallback(type, evictionCB, evictionArg);
}

/// Wait for all segments to be fully replicated.
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

////////////////////////////////////
/// Private Methods
////////////////////////////////////

/**
 * Provide the Log with a single contiguous piece of backing Segment memory.
 * The memory provided must of at least as large as the #segmentCapacity
 * parameter provided to the Log constructor. This function must be called
 * once for each Segment.
 * \param[in] p
 *      Memory to be added to the Log for use as segments.
 */
void
Log::addSegmentMemory(void *p)
{
    addToFreeList(p);

    if (maximumAppendableBytes == 0) {
        Segment s((uint64_t)0, 0, p, downCast<uint32_t>(segmentCapacity));
        maximumAppendableBytes = s.appendableBytes();
    }
}

/**
 * Add a Segment to various structures tracking live Segments in the Log.
 * \param[in] s
 *      The new Segment to be added.
 */
void
Log::addToActiveMaps(Segment *s)
{
    activeIdMap[s->getId()] = s;
    activeBaseAddressMap[s->getBaseAddress()] = s;
}

/**
 * Remove a Segment from various structures tracing live Segments in the Log.
 * \param[in] s
 *      The Segment to be removed.
 */
void
Log::eraseFromActiveMaps(Segment *s)
{
    activeIdMap.erase(s->getId());
    activeBaseAddressMap.erase(s->getBaseAddress());
}

/**
 * Add Segment backing memory to the free list.
 * \param[in] p
 *      Pointer to the memory to be added. The allocated memory must be
 *      at least #segmentCapacity bytes in length.
 */
void
Log::addToFreeList(void *p)
{
    segmentFreeList.push_back(p);
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
    if (segmentFreeList.empty())
        throw LogException(HERE, "Log is out of space");

    void *p = segmentFreeList.back();
    segmentFreeList.pop_back();

    return p;
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
    return nextSegmentId++;
}

/**
 * Given a pointer anywhere into a Segment's backing memeory that's part of
 * this Log, obtain the base address of that memory. This function returns
 * a pointer to an element that either is, or was on the segmentFreeList.
 */
const void *
Log::getSegmentBaseAddress(const void *p)
{
    const char* logStart = reinterpret_cast<const char*>(segmentMemory.get());
    const char* base = reinterpret_cast<const char*>(p);
    return base - ((base - logStart) % segmentCapacity);
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

////////////////////////////////////
// LogStats subclass
////////////////////////////////////

Log::LogStats::LogStats()
    : totalBytesAppended(0),
      totalAppends(0),
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
Log::LogStats::getFrees() const
{
    return totalFrees;
}

} // namespace
