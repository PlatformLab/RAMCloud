/* Copyright (c) 2015 Stanford University
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

#ifndef RAMCLOUD_MEMORYMONITOR_H
#define RAMCLOUD_MEMORYMONITOR_H

#include "WorkerTimer.h"

namespace RAMCloud {

/**
 * This class implements a WorkerTimer that wakes up at regular intervals
 * to check the amount of physical memory (resident set size) used by the
 * application. It prints log messages when it changes significantly.
 */
class MemoryMonitor : public WorkerTimer {
  PUBLIC:
    MemoryMonitor(Dispatch* dispatch, double intervalSeconds = 1.0,
            int threshold = 1024);
    ~MemoryMonitor();
    static int currentUsage();

  PRIVATE:
    void handleTimerEvent();

    // The time period in Cycles::rdtsc() ticks between successive checks
    // of memory size.
    uint64_t intervalTicks;

    // The memory usage (in MB) the last time we printed a message.
    int lastPrintUsage;

    // How much usage must change before we print a message, in MB.
    int threshold;

    // If non-null, this value will be used instead of reading
    // /proc/self/statm; used for unit testing.
    static const char* statmFileContents;
};

} // namespace RAMCloud

#endif // RAMCLOUD_MEMORYMONITOR_H

