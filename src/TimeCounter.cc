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

#include "TimeCounter.h"

namespace RAMCloud {

/**
 * Construct a TimeCounter, starting the timer.
 */
TimeCounter::TimeCounter()
    : total(NULL), startTime(getCurrentTime()) {
}

/**
 * Construct a TimeCounter, starting the timer.
 * \param total
 *      Where the elapsed time should be added once #stop() is called or the
 *      object is destructed. If you change your mind on this, use #cancel().
 */
TimeCounter::TimeCounter(uint64_t* total)
    : total(total), startTime(getCurrentTime()) {
}

/**
 * Return the current system time in nanoseconds using the clock_gettime()
 * system call and the CLOCK_REALTIME source.
 */
uint64_t
TimeCounter::getCurrentTime()
{
    struct timespec ts;
    int ret = clock_gettime(CLOCK_REALTIME, &ts);
    if (ret != 0) {
        RAMCLOUD_DIE("clock_gettime failed: %s", strerror(errno));
    }

    return (uint64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

/**
 * Destructor for TimeCounter, see #stop().
 */
TimeCounter::~TimeCounter() {
    stop();
}

/**
 * Stop the timer and discard the elapsed time.
 */
void
TimeCounter::cancel() {
    total = NULL;
    startTime = ~0UL;
}

/**
 * Stop the timer if it is running, and add the elapsed time to \a total as
 * given to the constructor.
 * \return
 *      The elapsed number of nanoseconds if the timer was running (not
 *      previously stopped or canceled). Otherwise, 0 is returned.
 */
uint64_t
TimeCounter::stop() {
    if (startTime == ~0UL)
        return 0;
    uint64_t elapsed = getCurrentTime() - startTime;
    if (total != NULL)
        *total += elapsed;
    startTime = ~0UL;
    return elapsed;
}

} // end RAMCloud
