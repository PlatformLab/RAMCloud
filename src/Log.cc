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

namespace RAMCloud {

Log::Log(uint64_t logId, Pool *segmentAllocator)
    : logId(logId), segmentAllocator(segmentAllocator), nextSegmentId(0)
{
    head = new Segment(segmentAllocator, logId, allocateSegmentId());

    addToActiveMaps(head);

    maximumAppendableBytes = head->appendableBytes();
}

bool
Log::isSegmentLive(uint64_t segmentId) const
{
    return (activeIdMap.find(segmentId) != activeIdMap.end());
}

uint64_t
Log::getSegmentId(const void *p)
{
    Segment *s = activeBaseAddressMap[
        Segment::getBaseAddress(p, segmentAllocator->getBlockSize())];
    return s->getId();
}

const void *
Log::append(LogEntryType type, const void *buffer, const uint64_t length)
{
    const void *p;

    if (length > maximumAppendableBytes)
        return NULL;

    /* 
     * try to append.
     * if we fail, try to allocate a new head.
     * if we run out of space entirely, creating the new head will throw an
     * exception.
     */
    do {
        p = head->append(type, buffer, length);
        if (p == NULL) {
            head->close();
            head = NULL;

            try {
                head = new Segment(segmentAllocator,
                                   logId, allocateSegmentId());
                addToActiveMaps(head);
            } catch (int e) {
                return NULL;
            }
        }
    } while (p == NULL);

    return p;
}

void
Log::free(const void *buffer, const uint64_t length)
{
    Segment *s = activeBaseAddressMap[
        Segment::getBaseAddress(buffer, segmentAllocator->getBlockSize())];
    s->free(length);
}

void
Log::registerType(LogEntryType type,
                  log_eviction_cb_t evictionCB, void *evictionArg)
{
    if (callbackMap.find(type) != callbackMap.end())
        throw 0;

    callbackMap[type] = new LogTypeCallback(type, evictionCB, evictionArg);
}

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

void
Log::addToActiveMaps(Segment *s)
{
    activeIdMap[s->getId()] = s;
    activeBaseAddressMap[(uintptr_t)s->getBaseAddress()] = s;
}

void
Log::eraseFromActiveMaps(Segment *s)
{
    activeIdMap.erase(s->getId()); 
    activeBaseAddressMap.erase((uintptr_t)s->getBaseAddress());
}

uint64_t
Log::allocateSegmentId()
{
    return nextSegmentId++;
}

} // namespace
