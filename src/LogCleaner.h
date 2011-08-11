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

#ifndef RAMCLOUD_LOGCLEANER_H
#define RAMCLOUD_LOGCLEANER_H

#include <boost/thread.hpp>
#include <vector>

#include "Common.h"
#include "BackupManager.h"
#include "LogTypes.h"
#include "Log.h"
#include "Segment.h"

namespace RAMCloud {

// forward decl around the circular Log/LogCleaner dependency
class Log;

class LogCleaner {
  public:
    explicit LogCleaner(Log* log, BackupManager* backup, bool startThread);
    ~LogCleaner();
    void clean();
    void halt();

  PRIVATE:
    typedef std::vector<SegmentEntryHandle> SegmentEntryHandleVector;

    // cleaner thread entry point
    static void cleanerThreadEntry(LogCleaner* logCleaner);

    void scanNewCleanableSegments();
    void scanSegment(Segment* segment);
    void getSegmentsToClean(SegmentVector&);
    void getSortedLiveEntries(SegmentVector& segments,
                              SegmentEntryHandleVector& liveEntries);
    void moveLiveData(SegmentEntryHandleVector& data);

    /// After cleaning, wake the cleaner again after this many microseconds.
    static const size_t CLEANER_POLL_USEC = 50000;

    /// Don't bother cleaning unless so many bytes have been freed in the Log
    /// since the last cleaning operation.
    static const size_t MIN_CLEANING_DELTA = 2 * Segment::SEGMENT_SIZE;

    /// Number of Segments to clean per pass.
    static const size_t SEGMENTS_PER_CLEANING_PASS = 10;

    /// Maximum write cost we'll permit. Anything higher and we won't clean.
    static const double MAXIMUM_CLEANABLE_WRITE_COST = 3.0;

    /// The number of bytes that have been freed in the Log since the last
    /// cleaning operation completed. This is used to avoid invoking the
    /// cleaner if there isn't likely any work to be done.
    uint64_t        bytesFreedBeforeLastCleaning;

    /// Segments that the Log considers cleanable, but which haven't been
    /// scanned yet (i.e. the scan callback has not been called on each
    /// entry. This list originally existed for asynchronous updates to
    /// TabletProfiler structures, but the general callback may serve
    /// arbitrary purposes for whoever registered a log type.
    SegmentVector   scanList;

    /// Segments are scanned in precise order of SegmentId. This integer
    /// tracks the next SegmentId to be scanned. The assumption is that
    /// the Log begins at ID 0.
    uint64_t        nextScannedSegmentId;

    /// Closed segments that are part of the Log - these may be cleaned
    /// at any time. Only Segments that have been scanned (i.e. previously
    /// were on the #scanList can be added here.
    SegmentVector   cleanableSegments;

    /// The Log we're cleaning.
    Log*            log;

    /// Our own private BackupManager (not the Log's). BackupManager isn't
    /// reentrant, and there's little reason for it to be, so use this one
    // to manage the Segments we create while cleaning.
    BackupManager*  backup;

    // Tub containing our cleaning thread, if we're told to instantiate one
    // by whoever constructs this object.
    Tub<boost::thread> thread;

    friend class LogCleanerTest;

    DISALLOW_COPY_AND_ASSIGN(LogCleaner);
};

} // namespace

#endif // !RAMCLOUD_LOGCLEANER_H
