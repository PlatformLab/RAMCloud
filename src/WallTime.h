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

/**
 * \file
 * Prototypes for RAMCloud wall clock time functions.
 */

#ifndef RAMCLOUD_WALLTIME_H
#define RAMCLOUD_WALLTIME_H

namespace RAMCloud {

/**
 * The WallTime class provides a fast, seconds-granularity real time clock.
 * It works by initially querying the OS for the initial time value, and then
 * increments that count based on the CPU's timestamp counter in subsequent
 * calls.
 *
 * WallTime clocks are 32-bit, like Unix's time_t, though it's unsigned and the
 * "RAMCloud" epoch begins much more recently. This should last us until about
 * the year 2150. 
 */
class WallTime {
  public:
    static uint32_t secondsTimestamp(void);
    static time_t secondsTimestampToUnix(uint32_t timestamp);

    /**
     * The RAMCloud epoch began Jan 1 00:00:00 2011 UTC.
     * The Unix epoch began 41(!) years prior. May ours
     * last just as long!
     */
    static const uint32_t RAMCLOUD_UNIX_OFFSET = (41 * 365 * 86400);

    /**
     * The base Unix time, used by the timestamp methods below.
     * This exists so that we need only call the kernel once to
     * get the time. In the future, we'll use rdtsc().
     */
    static time_t baseTime;

    /**
     * CPU timestamp counter value at the first call to
     * #secondsTimestamp. Used to avoid subsequent syscalls
     * to obtain current time in conjunction with #baseTime.
     */
    static uint64_t baseTsc;

#if TESTING
    /**
     * For testing only. Used to force the time this class returns.
     */
    static uint32_t mockWallTimeValue;
#endif
};

}

#endif // RAMCLOUD_WALLTIME_H

