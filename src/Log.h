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
#include <boost/unordered_map.hpp>
#include <vector>

using boost::unordered_map;
using std::vector;

namespace RAMCloud {

// forward decl around the circular Log/LogCleaner dependency
class LogCleaner;

typedef void (*LogSegmentCallback)(Segment *, void *);
typedef unordered_map<LogEntryType, LogTypeCallback *> CallbackMap;
typedef unordered_map<uint64_t, Segment *> ActiveIdMap;
typedef unordered_map<const void *, Segment *> BaseAddressMap;

/**
 * An exception that is thrown when the Log class is provided invalid
 * method arguments.
 */
struct LogException : public Exception {
    LogException() : Exception() {}
    explicit LogException(std::string msg) : Exception(msg) {}
    explicit LogException(int errNo) : Exception(errNo) {}
};

class Log {
  public:
    Log(uint64_t logId, uint64_t logCapacity, uint64_t segmentCapacity);
    ~Log();
    const void *append(LogEntryType type,
                       const void *buffer, uint64_t length);
    void        free(const void *p);
    void        registerType(LogEntryType type,
                             log_eviction_cb_t evictionCB, void *evictionArg);
    uint64_t    getSegmentId(const void *p);
    bool        isSegmentLive(uint64_t segmentId) const;
    void        forEachSegment(LogSegmentCallback cb, uint64_t limit,
                               void *cookie) const;
    uint64_t    getMaximumAppendableBytes() const;

  private:
    void        addSegmentMemory(void *p);
    void        addToActiveMaps(Segment *s);
    void        eraseFromActiveMaps(Segment *s);
    void        addToFreeList(void *p);
    void       *getFromFreeList();
    uint64_t    allocateSegmentId();
    const void *getSegmentBaseAddress(const void *p);

    uint64_t       logId;
    uint64_t       logCapacity;
    uint64_t       segmentCapacity;
    vector<void *> segmentFreeList;
    uint64_t       nextSegmentId;
    uint64_t       maximumAppendableBytes;
    LogCleaner    *cleaner;

    /// Current head of the log
    Segment *head;

    /// Per-LogEntryType callbacks (e.g. for eviction)
    CallbackMap callbackMap;

    /// Segment Id -> Segment * lookup within the active list
    ActiveIdMap activeIdMap;

    /// Segment base address -> Segment * lookup within the active list
    BaseAddressMap activeBaseAddressMap;

    DISALLOW_COPY_AND_ASSIGN(Log);

    friend class LogTest;
    friend class LogCleaner;
};

} // namespace

#endif // !RAMCLOUD_LOG_H
