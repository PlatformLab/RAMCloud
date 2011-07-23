/* Copyright (c) 2011 Stanford University
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

#include <Common.h>
#include <BenchUtil.h>

/**
 * \file
 * Routines for obtaining wall clock time relative to the RAMCloud
 * epoch and converting to Unix time_t values.
 */

namespace RAMCloud {

/**
 * The RAMCloud epoch began Jan 1 00:00:00 2011 UTC.
 * The Unix epoch began 41(!) years prior. May ours
 * last just as long!
 */
#define RAMCLOUD_UNIX_OFFSET (41 * 365 * 86400)

/**
 * The base Unix time, used by the timestamp methods below.
 * This exists so that we need only call the kernel once to
 * get the time. In the future, we'll use rdtsc().
 */
static time_t baseTime = 0;

/**
 * CPU timestamp counter value at the first call to
 * #secondsTimestamp. Used to avoid subsequent syscalls
 * to obtain current time in conjunction with #baseTime.
 */
static uint64_t baseTsc = 0;

/**
 * Obtain a fast timestamp with seconds granularity. This stamp is an offset
 * from Jan 1 00:00:00 2011 UTC. A syscall is used only on the first invocation.
 * All subsequent calls use CPUs timestamp counter.
 */
uint32_t
secondsTimestamp()
{
	if (baseTime == 0) {
		baseTime = time(NULL);
		baseTsc = rdtsc();

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
    uint32_t tscSecondsOffset = downCast<uint32_t>(cyclesToNanoseconds(
        rdtsc() - baseTsc + 500000000) / 1000000000U);

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
secondsTimestampToUnix(uint32_t timestamp)
{
	return (time_t)timestamp + RAMCLOUD_UNIX_OFFSET;
}

} // namespace
