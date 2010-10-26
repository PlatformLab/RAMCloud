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

// RAMCloud pragma [GCCWARN=5]
// RAMCloud pragma [CPPLINT=0]

#include <assert.h>
#include <stdint.h>
#include <exception>

#include <Log.h>
#include <LogCleaner.h>

namespace RAMCloud {

/**
 * Constructor for Log.
 * \param[in] logId
 *      A unique numerical identifier for this Log. This should be globally
 *      unique in the RAMCloud system.
 * \param[in] segmentSize
 *      Size of the Segments that will be used in this Log in bytes.
 * \return
 *      The newly constructed Log object. The caller must first add backing
 *      Segment memory to the Log with #addSegmentMemory before any appends
 *      will succeed.
 */
Log::Log(uint64_t logId, uint64_t segmentSize)
    : logId(logId), segmentSize(segmentSize), segmentFreeList(),
      nextSegmentId(0), maximumAppendableBytes(0), cleaner(NULL), head(NULL),
      callbackMap(), activeIdMap(), activeBaseAddressMap()
{
}

/**
 * Clean up after the Log.
 */
Log::~Log()
{
    /// XXX:
    //      free Segment *s
    //      free LogTypeCallback *s
}

/**
 * Associate a LogCleaner to with this Log.
 * \param[in] cleaner
 *      The LogCleaner object to be associated with this Log.
 * \throw 0
 *      An exception is thrown if a LogCleaner has already been associated.
 */
void
Log::setCleaner(LogCleaner *cleaner)
{
    if (this->cleaner != NULL)
        throw 0;
    this->cleaner = cleaner;
}

/**
 * Provide the Log with a single contiguous piece of backing Segment memory.
 * The memory provided must of at least as large as the #segmentSize parameter
 * provided to the Log constructor. This function must be called once for each
 * Segment.
 * \param[in] p
 *      Memory to be added to the Log for use as segments.
 */
void
Log::addSegmentMemory(void *p)
{
    addToFreeList(p);

    if (maximumAppendableBytes == 0) {
        Segment s(0, 0, p, segmentSize);
        maximumAppendableBytes = s.appendableBytes();
    }
}

/**
 * Determine whether or not the provided Segment identifier is currently
 * live. A live Segment is one that is still being used by the Log for
 * storage. This method can be used to determine if data once written to the
 * Log is no longer present in the RAMCloud system and hence will not appear
 * again during either normal operation or recovery.
 * \param[in] segmentId
 *      The Segment identifier to check for validity.
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
 */
uint64_t
Log::getSegmentId(const void *p)
{
    Segment *s = activeBaseAddressMap[Segment::getBaseAddress(p, segmentSize)];
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
 * \return
 *      On success, a const pointer into the Log's backing memory with
 *      the same contents as `buffer'. On failure, NULL.
 */
const void *
Log::append(LogEntryType type, const void *buffer, const uint64_t length)
{
    const void *p = NULL;

    if (length > maximumAppendableBytes)
        return NULL;

    /* 
     * try to append.
     * if we fail, try to allocate a new head.
     * if we run out of space entirely, creating the new head will throw an
     * exception.
     */
    do {
        if (head != NULL)
            p = head->append(type, buffer, length);

        if (p == NULL) {
            if (head != NULL) {
                head->close();
                head = NULL;
            }

            void *s = getFromFreeList();
            if (s == NULL)
                return NULL;

            head = new Segment(logId, allocateSegmentId(), s, segmentSize);
            addToActiveMaps(head);

            if (cleaner != NULL)
                cleaner->clean(1);
        }
    } while (p == NULL);

    return p;
}

/**
 * Mark bytes in Log as freed. This simply maintains a per-Segment tally that
 * can be used to compute utilisation of individual Log Segments.
 * \param[in] buffer
 *      A pointer to anywhere within a live Segment of the Log, as provided
 *      by #append.
 * \param[in] length
 *      The number of bytes to mark as freed.
 */
void
Log::free(const void *buffer, const uint64_t length)
{
    Segment *s = activeBaseAddressMap[
        Segment::getBaseAddress(buffer, segmentSize)];
    s->free(length);
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
 */
void
Log::registerType(LogEntryType type,
                  log_eviction_cb_t evictionCB, void *evictionArg)
{
    if (callbackMap.find(type) != callbackMap.end())
        throw 0;

    callbackMap[type] = new LogTypeCallback(type, evictionCB, evictionArg);
}

/**
 * Iterate over live Segments and pass them to the callback provided.
 * The number of Segments iterated over may be artificially limited by the
 * #limit parameter.
 * \param[in] cb
 *      The callback to issue each live Segment to.
 * \param[in] limit
 *      The maximum number of Segments to iterate over.
 * \param[in] cookie
 *      A void* argument to be passed to the specified callback.
 */
void
Log::forEachSegment(LogSegmentCallback cb, uint64_t limit, void *cookie) const
{
    uint64_t i = 0;
    unordered_map<uint64_t, Segment *>::const_iterator it = activeIdMap.begin();

    while (it != activeIdMap.end() && i < limit) {
        cb(it->second, cookie);
        i++, it++;
    }
} 

////////////////////////////////////
/// Private Methods
////////////////////////////////////

/**
 * Add a Segment to various structures tracking live Segments in the Log.
 * \param[in] s
 *      The new Segment to be added.
 */
void
Log::addToActiveMaps(Segment *s)
{
    activeIdMap[s->getId()] = s;
    activeBaseAddressMap[(uintptr_t)s->getBaseAddress()] = s;
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
    activeBaseAddressMap.erase((uintptr_t)s->getBaseAddress());
}

/**
 * Add Segment backing memory to the free list.
 * \param[in] p
 *      Pointer to the memory to be added. The allocated memory must be
 *      at least #segmentSize bytes in length.
 */
void
Log::addToFreeList(void *p)
{
    segmentFreeList.push_back(p);
}

/**
 * Obtain Segment backing memory from the free list.
 * \return
 *      On success, a pointer to Segment backing memory of #segmentSize bytes,
 *      as provided in the #addSegmentMemory method. If memory is exhausted,
 *      NULL is returned.
 */
void *
Log::getFromFreeList()
{
    if (segmentFreeList.empty())
        return NULL;

    void *p = segmentFreeList[segmentFreeList.size() - 1];
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

} // namespace
