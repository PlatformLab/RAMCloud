/* Copyright (c) 2015 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "Cycles.h"
#include "Logger.h"
#include "MemoryMonitor.h"
#include "Minimal.h"

namespace RAMCloud {
const char* MemoryMonitor::statmFileContents = NULL;

/**
 * Construct a MemoryMonitor.
 *
 * \param dispatch
 *      The dispatcher that will be used to schedule execution of this
 *      object.
 * \param intervalSeconds
 *      This object will check memory usage at regular intervals of
 *      this length.
 * \param threshold
 *      Messages get printed when memory usage moves either up or down by
 *      this much, in MB.
 */
MemoryMonitor::MemoryMonitor(Dispatch* dispatch, double intervalSeconds,
        int threshold)
    : WorkerTimer(dispatch)
    , intervalTicks(Cycles::fromSeconds(intervalSeconds))
    , lastPrintUsage(0)
    , threshold(threshold)
{
    start(0);
}

/**
 * Destructor for MemoryMonitor.
 */
MemoryMonitor::~MemoryMonitor()
{
}

void
MemoryMonitor::handleTimerEvent()
{
    // Check for changes greater than 1GB in either direction.
    int current = currentUsage();
    int delta = current - lastPrintUsage;
    if (delta >= threshold) {
        RAMCLOUD_LOG(NOTICE, "Memory usage now %d MB (increased %d MB)",
                current, delta);
        lastPrintUsage = current;
    } else if (-delta >= threshold) {
        RAMCLOUD_LOG(NOTICE, "Memory usage now %d MB (decreased %d MB)",
                current, -delta);
        lastPrintUsage = current;
    }
    start(Cycles::rdtsc() + intervalTicks);
}

/**
 * This method returns the current process' current usage of physical
 * memory (resident set size), measured in MB (1024*1024).
 */
int
MemoryMonitor::currentUsage()
{
    char buffer[1000];
    const char* data;
    if (statmFileContents != NULL) {
        data = statmFileContents;
    } else {
        FILE* f = fopen("/proc/self/statm", "r");
        if (f == NULL) {
            RAMCLOUD_DIE("Couldn't open /proc/self/statm: %s",
                    strerror(errno));
        }
        size_t count = fread(buffer, 1, sizeof(buffer) - 1, f);
        fclose(f);
        buffer[count] = 0;
        data = buffer;
    }

    // Expected format of data in /proc/self/statm (see "man proc 5"):
    // totalSize residentSize ...
    // Contrary to man page, units are apparently 4KB pages.
    char* end;
    uint64_t resident;
    const char* p = strchr(data, ' ');
    if (p == NULL) {
        goto error;
    }
    resident = strtoull(p+1, &end, 10);
    if ((*end == ' ') && (resident != 0)) {
        return downCast<int>((resident+255)/256);
    }

  error:
    RAMCLOUD_DIE("Couldn't parse contents of /proc/self/statm: %s", data);
}

} // namespace RAMCloud
