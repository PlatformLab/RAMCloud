/* Copyright (c) 2010 Stanford University
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
 * Implementation for utilities useful in benchmarking.
 */

#include "BenchUtil.h"
#include <errno.h>
#include <sys/time.h>

namespace RAMCloud {

/**
 * Return the approximate number of cycles per second for this CPU.
 * The value is computed once during the first call, then reused in
 * subsequent calls.
 * \return
 *      See above.
 */
uint64_t
getCyclesPerSecond()
{
    static uint64_t cycles = 0;
    if (cycles)
        return cycles;

    // Overall strategy: take parallel time readings using both rdtsc
    // and gettimeofday. After 10ms have elapsed, take the ratio between
    // these readings.

    struct timeval startTime, stopTime;
    uint64_t startCycles, stopCycles, micros;
    uint64_t oldCycles;

    // There is one tricky aspect, which is that we could get interrupted
    // between calling gettimeofday and reading the cycle counter, in which
    // case we won't have corresponding readings.  To handle this (unlikely)
    // case, compute the overall result repeatedly, and wait until we get
    // two successive calculations that are within 0.1% of each other.
    oldCycles = 0;
    while (1) {
        if (gettimeofday(&startTime, NULL) != 0) {
            DIE("BenchUtil::getCyclesPerSecond couldn't read clock: %s",
                    strerror(errno));
        }
        startCycles = rdtsc();
        while (1) {
            if (gettimeofday(&stopTime, NULL) != 0) {
                DIE("BenchUtil::getCyclesPerSecond couldn't read clock: %s",
                        strerror(errno));
            }
            stopCycles = rdtsc();
            micros = (stopTime.tv_usec - startTime.tv_usec) +
                    (stopTime.tv_sec - startTime.tv_sec)*1000000;
            if (micros > 10000) {
                cycles = 1000000*(stopCycles - startCycles) / micros;
                break;
            }
        }
        uint64_t delta = cycles/1000;
        if ((oldCycles > (cycles - delta)) && (oldCycles < (cycles + delta))) {
            return cycles;
        }
        oldCycles = cycles;
    }
}

/**
 * Given an elapsed number of cycles, return an approximate number of
 * nanoseconds elapsed.
 * \param cycles
 *      An elapsed number of cycles.
 * \return
 *      The approximate number of nanoseconds elapsed.
 */
uint64_t
cyclesToNanoseconds(uint64_t cycles)
{
    return (cycles * 1000 * 1000 * 1000 / getCyclesPerSecond());
}

/**
 * Given an elapsed number of cycles, return a floating-point number giving
 * the corresponding time in seconds.
 * \param cycles
 *      An elapsed number of cycles.
 * \return
 *      The time in seconds corresponding to cycles.
 */
double
cyclesToSeconds(uint64_t cycles)
{
    double result = cycles;
    return result / getCyclesPerSecond();
}

/**
 * Given a number of nanoseconds, return an approximate number of
 * cycles for an equivalent time length.
 * \param ns
 *      Number of nanoseconds.
 * \return
 *      The approximate number of cycles for the same time length.
 */
uint64_t
nanosecondsToCycles(uint64_t ns)
{
    return (ns * getCyclesPerSecond()) / (1000 * 1000 * 1000);
}

} // end RAMCloud
