/* Copyright (c) 2011-2012 Stanford University
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

#include "Common.h"
#include "Cycles.h"
#include "WallTime.h"

/**
 * \file
 * Routines for obtaining wall clock time relative to the RAMCloud
 * epoch and converting to Unix time_t values.
 */

namespace RAMCloud {

// Static member declarations and initial values.
time_t WallTime::baseTime = 0;
uint64_t WallTime::baseTsc = 0;
#if TESTING
uint32_t WallTime::mockWallTimeValue = 0;
#endif

/**
 * Obtain a fast timestamp with seconds granularity. This stamp is an offset
 * from Jan 1 00:00:00 2011 UTC. A syscall is used only on the first invocation.
 * All subsequent calls use CPUs timestamp counter.
 */
uint32_t
WallTime::secondsTimestamp()
{
#if TESTING
    if (mockWallTimeValue)
        return mockWallTimeValue;

    // In testing, it is possible that some other Unit test sets mockTscValue
    // which can cause Cycles::rdtsc() to return very different value from
    // baseTsc. This can cause assertion failure from downCast<uint32_t> below.
    uint64_t cycle = Cycles::rdtsc_ignoreMockTsc();
#else
    uint64_t cycle = Cycles::rdtsc();
#endif

    if (baseTime == 0) {
        baseTime = time(NULL);
        baseTsc = cycle;

        if (baseTime == -1) {
            fprintf(stderr, "ERROR: The time(3) syscall failed!!");
            exit(1);
        }

        if (baseTime < RAMCLOUD_UNIX_OFFSET) {
            fprintf(stderr, "ERROR: Your clock is too far behind. "
                "Please fix the date on your system!");
            exit(1);
        }
    }

    // calculate seconds offset. be careful to round up.
    uint32_t tscSecondsOffset = downCast<uint32_t>(Cycles::toNanoseconds(
        cycle - baseTsc + 500000000) / 1000000000U);

    return downCast<uint32_t>(baseTime) - RAMCLOUD_UNIX_OFFSET +
        tscSecondsOffset;
}

/**
 * Convert a timestamp returned via #secondsTimestamp to a Unix time_t
 * value, with the appropriate offset from the Unix epoch.
 *
 * \param[in] timestamp
 *      A RAMCloud epoch timestamp, as returned via #secondsTimestamp.
 */
time_t
WallTime::secondsTimestampToUnix(uint32_t timestamp)
{
    return (time_t)timestamp + RAMCLOUD_UNIX_OFFSET;
}

} // namespace
