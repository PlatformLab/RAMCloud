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

#ifndef RAMCLOUD_LOG_H
#define RAMCLOUD_LOG_H

#include <stdint.h>
#include <LogTypes.h>
#include <Segment.h>
#include <Pool.h>
#include <boost/unordered_map.hpp>

using boost::unordered_map;

namespace RAMCloud {

typedef void (*LogSegmentCallback)(Segment *, void *);

class Log {
  public:
    Log(uint64_t logId, Pool *segmentAllocator);
    ~Log() {}
    const void *append(LogEntryType type,
                       const void *buffer, uint64_t length);
    void        free(const void *buffer, const uint64_t length);
    void        registerType(LogEntryType type,
                             log_eviction_cb_t evictionCB, void *evictionArg);
    uint64_t    getSegmentId(const void *p);
    bool        isSegmentLive(uint64_t segmentId) const;
    void        forEachSegment(LogSegmentCallback cb, uint64_t limit,
                               void *cookie) const;

  private:
    void        addToActiveMaps(Segment *s);
    void        eraseFromActiveMaps(Segment *s);
    uint64_t    allocateSegmentId();

    Pool       *segmentAllocator;
    uint64_t    nextSegmentId;
    uint64_t    logId;
    uint64_t    maximumAppendableBytes;

    /// Current head of the log
    Segment *head;

    /// Eviction and liveness callbacks
    unordered_map<LogEntryType, LogTypeCallback *> callbackMap;

    /// Segment Id -> Segment * lookup within the active list
    unordered_map<uint64_t, Segment *> activeIdMap;

    /// Segment base address -> Segment * lookup within the active list
    unordered_map<uintptr_t, Segment *> activeBaseAddressMap;

    DISALLOW_COPY_AND_ASSIGN(Log);

    friend class LogCleaner;
};

} // namespace

#endif // !RAMCLOUD_LOG_H
