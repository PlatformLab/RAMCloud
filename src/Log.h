/* Copyright (c) 2009 Stanford University
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

#include "Common.h"

#include "Object.h"
#include "LogTypes.h"
#include "Segment.h"
#include "BackupClient.h"

namespace RAMCloud {

struct LogEntry {
    uint32_t  type;
    uint32_t  length;
};

struct SegmentHeader {
    uint64_t id;
};

struct SegmentChecksum {
    uint64_t checksum;
};

class LogEntryIterator {
  public:
    explicit LogEntryIterator(const Segment *s);
    bool getNextAndOffset(const struct LogEntry **le,
                          const void **p,
                          uint64_t *offset);
    bool getNext(const struct LogEntry **le, const void **p);
  private:
    const Segment *segment;
    const struct LogEntry *next;
    DISALLOW_COPY_AND_ASSIGN(LogEntryIterator);
};

typedef void (*LogEvictionCallback)(LogEntryType, const void *,
                                    const uint64_t, void *);
typedef void (*LogEntryCallback)(LogEntryType,
                                 const void *, const uint64_t, void *);

class Log {
  public:
    Log(const uint64_t, void *, const uint64_t, BackupClient *);
    ~Log() {}
    const void *append(LogEntryType, const void *, uint64_t);
    void        free(LogEntryType, const void *, uint64_t);
    void        registerType(LogEntryType, LogEvictionCallback, void *);
    void        printStats();
    uint64_t    getMaximumAppend();
    void        init();
    bool        isSegmentLive(uint64_t) const;
    void        getSegmentIdOffset(const void *, uint64_t *, uint32_t *) const;

  private:
    void        clean(void);
    bool        newHead();
    void        checksumHead();
    void        retireHead();
    const void *appendAnyType(LogEntryType, const void *, uint64_t);
    uint64_t    allocateSegmentId();
    LogEvictionCallback getEvictionCallback(LogEntryType, void **);
    Segment    *getSegment(const void *, uint64_t) const;

    struct {
        LogEvictionCallback cb;
        LogEntryType  type;
        void *cookie;
    } callbacks[10];
    int      numCallbacks;

    uint64_t nextSegmentId;  // next segment Id
    uint64_t maxAppend;      // max bytes append() can ever take
    uint64_t segmentSize;    // size of each segment in bytes
    void    *base;           // base of all segments
    Segment **segments;      // array of all segments
    Segment *head;           // head of the log
    Segment *freeList;       // free (utilization == 0) segments
    uint64_t nsegments;      // total number of segments in the system
    uint64_t nFreeList;      // number of segments in free list
    uint64_t bytesStored;    // bytes stored in the log (non-metadata only)
    bool     cleaning;       // presently cleaning the log
    BackupClient *backup;

    friend class LogTest;
    DISALLOW_COPY_AND_ASSIGN(Log);
};

} // namespace

#endif // !_LOG_H_
