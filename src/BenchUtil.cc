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

#include <BenchUtil.h>

namespace RAMCloud {

/**
 * Return the approximate number of cycles per second for this CPU.
 * The first time this function executes, it measures the elapsed cycles of a
 * usleep (3) call to determine the frequency of the CPU.
 * \return
 *      See above.
 */
uint64_t
getCyclesPerSecond()
{
    static uint64_t cycles = 0;
    if (cycles)
        return cycles;
    uint64_t start = rdtsc();
    usleep(500 * 1000);
    uint64_t end = rdtsc();
    cycles = (end - start) * 2;
    return cycles;
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
    return (cycles * 1000 * 1000 / getCyclesPerSecond());
}

} // end RAMCloud
