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
 * Implementation for CycleCounter.
 */

#include <CycleCounter.h>

namespace RAMCloud {

/**
 * Construct a CycleCounter, starting the timer.
 */
CycleCounter::CycleCounter()
    : total(NULL), startTime(rdtsc()) {
}

/**
 * Construct a CycleCounter, starting the timer.
 * \param total
 *      Where the elapsed time should be added once #stop() is called or the
 *      object is destructed. If you change your mind on this, use #cancel().
 */
CycleCounter::CycleCounter(uint64_t* total)
    : total(total), startTime(rdtsc()) {
}

/**
 * Destructor for CycleCounter, see #stop().
 */
CycleCounter::~CycleCounter() {
    stop();
}

/**
 * Stop the timer and discard the elapsed time.
 */
void
CycleCounter::cancel() {
    total = NULL;
    startTime = ~0UL;
}

/**
 * Stop the timer if it is running, and add the elapsed time to \a total as given
 * to the constructor.
 * \return
 *      The elapsed number of cycles if the timer was running (not previously
 *      stopped or canceled). Otherwise, 0 is returned.
 */
uint64_t
CycleCounter::stop() {
    if (startTime == ~0UL)
        return 0;
    uint64_t elapsed = rdtsc() - startTime;
    if (total != NULL)
        *total += elapsed;
    startTime = ~0UL;
    return elapsed;
}


} // end RAMCloud
