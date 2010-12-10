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
#include <boost/unordered_map.hpp>
#include <vector>

#include "LogCleaner.h"
#include "LogTypes.h"
#include "Segment.h"
#include "BackupManager.h"

namespace RAMCloud {

/**
 * LogTime is a (Segment #, Segment Offset) tuple that represents the logical
 * time at which something was appended to the Log. It is currently only used
 * for computing table partitions.
 */
typedef std::pair<uint64_t, uint64_t> LogTime;

/**
 * An exception that is thrown when the Log class is provided invalid
 * method arguments.
 */
struct LogException : public Exception {
    explicit LogException(const CodeLocation& where)
        : Exception(where) {}
    LogException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
    LogException(const CodeLocation& where, int errNo)
        : Exception(where, errNo) {}
    LogException(const CodeLocation& where, string msg, int errNo)
        : Exception(where, msg, errNo) {}
};

class Log {
  public:
    Log(uint64_t logId, uint64_t logCapacity, uint64_t segmentCapacity,
            BackupManager *backup = NULL);
    ~Log();
    const void *append(LogEntryType type,
                       const void *buffer, uint64_t length, bool sync = true);
    void        free(const void *p);
    void        registerType(LogEntryType type,
                             log_eviction_cb_t evictionCB, void *evictionArg);
    uint64_t    getSegmentId(const void *p);
    bool        isSegmentLive(uint64_t segmentId) const;
    uint64_t    getMaximumAppendableBytes() const;
    uint64_t    getBytesAppended() const;

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
    LogCleaner     cleaner;

    /// Current head of the log
    Segment *head;

    typedef boost::unordered_map<LogEntryType, LogTypeCallback *> CallbackMap;
    /// Per-LogEntryType callbacks (e.g. for eviction)
    CallbackMap callbackMap;

    typedef boost::unordered_map<uint64_t, Segment *> ActiveIdMap;
    /// Segment Id -> Segment * lookup within the active list
    ActiveIdMap activeIdMap;

    typedef boost::unordered_map<const void *, Segment *> BaseAddressMap;
    /// Segment base address -> Segment * lookup within the active list
    BaseAddressMap activeBaseAddressMap;

    /// Given to Segments to make them durable
    BackupManager *backup;

    /// The total number of bytes appended to this log during its lifetime.
    uint64_t bytesAppended;

    friend class LogTest;
    friend class LogCleaner;

    DISALLOW_COPY_AND_ASSIGN(Log);
};

} // namespace

#endif // !RAMCLOUD_LOG_H
