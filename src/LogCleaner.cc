/* Copyright (c) 2010 Stanford University
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

#include "Common.h"
#include "Log.h"
#include "LogCleaner.h"
#include "Segment.h"
#include "SegmentIterator.h"

namespace RAMCloud {

LogCleaner::LogCleaner(Log *log) : log(log)
{
}

static int
compare(const void *a, const void *b)
{
    const Segment *segA = reinterpret_cast<const Segment *>(a);
    const Segment *segB = reinterpret_cast<const Segment *>(b);

    if (segA->getUtilisation() < segB->getUtilisation())
        return -1;
    if (segA->getUtilisation() > segB->getUtilisation())
        return 1;
    return 0;
}

uint64_t
LogCleaner::clean(uint64_t numSegments)
{
    uint64_t i;

    if (!needsCleaning())
        return 0;

    uint64_t maxCleanableSegs = log->activeIdMap.size() - 1;  // ignore head
    Segment *segments[maxCleanableSegs];

    Log::ActiveIdMap::iterator it = log->activeIdMap.begin();
    for (i = 0; it != log->activeIdMap.end(); it++) {
        if (it->second != log->head) {
            segments[i] = it->second;
            i++;
        }
    }

    qsort(segments, maxCleanableSegs, sizeof(segments[0]), compare);

    for (i = 0; i < std::min(maxCleanableSegs, numSegments); i++)
        cleanSegment(segments[i]);

    return i;
}

////////////////////////////////////////
/// Private Methods
////////////////////////////////////////

bool
LogCleaner::needsCleaning()
{
    uint64_t freeSegs  = log->segmentFreeList.size();
    uint64_t totalSegs = freeSegs + log->activeIdMap.size();
    uint64_t pctFree   = (100 * freeSegs) / totalSegs;
    return (pctFree <= FREELIST_LOW_WATERMARK_PCT);
}

void
LogCleaner::cleanSegment(Segment *segment)
{
    for (SegmentIterator i(segment); !i.isDone(); i.next()) {
        LogEntryType type = i.getType();

        if (log->callbackMap.find(type) == log->callbackMap.end())
            continue;

        uint32_t length   = i.getLength();
        const void *p     = i.getPointer();

        LogTypeCallback *logCB = log->callbackMap[type];
        logCB->evictionCB(type, p, length, logCB->evictionArg);
    }

    log->eraseFromActiveMaps(segment);
    log->addToFreeList(const_cast<void *>(segment->getBaseAddress()));
    delete segment;
}

} // namespace
