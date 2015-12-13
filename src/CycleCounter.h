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

#ifndef RAMCLOUD_CYCLECOUNTER_H
#define RAMCLOUD_CYCLECOUNTER_H

#include "Common.h"
#include "Cycles.h"

namespace RAMCloud {

/**
 * An object that keeps track of the elapsed number of cycles since its
 * declaration.
 * \tparam T
 *      Something like a uint64_t that supports += with a uint64_t.
 *      If T is empty (see gcc __is_empty trait docs), this counter will have
 *      no effect; this is useful for compiling out the overhead of
 *      CycleCounter.
 */
template<typename T = uint64_t>
class CycleCounter {
  public:
    /**
     * Construct a CycleCounter, starting the timer.
     */
    CycleCounter()
        : total(NULL), startTime(__is_empty(T) ? 0 : Cycles::rdtsc()) {}

    /**
     * Construct a CycleCounter, starting the timer.
     * \param total
     *      Where the elapsed time should be added once #stop() is called or
     *      the object is destructed. If you change your mind on this, use
     *      #cancel(). May be NULL.
     */
    explicit CycleCounter(T* total)
        : total(total), startTime(__is_empty(T) ? 0 : Cycles::rdtsc()) {}

    /**
     * Destructor for CycleCounter, see #stop().
     */
    ~CycleCounter() {
        stop();
    }

    /**
     * Stop the timer and discard the elapsed time.
     */
    void cancel() {
        total = NULL;
        startTime = ~0UL;
    }

    /**
     * Stop the timer if it is running, and add the elapsed time to \a total as
     * given to the constructor.
     * \return
     *      The elapsed number of cycles if the timer was running (not previously
     *      stopped or canceled). Otherwise, 0 is returned.
     */
    uint64_t stop() {
        if (startTime == ~0UL)
            return 0;
        // using 1 avoids most div by zero errors
        uint64_t stopTime = (__is_empty(T) ? 0 : Cycles::rdtsc());
        uint64_t elapsed = stopTime - this->startTime;
        if (total != NULL)
            *total += elapsed;
        startTime = ~0UL;
        return elapsed;
    }

  private:
    T* total;
    uint64_t startTime;
    DISALLOW_COPY_AND_ASSIGN(CycleCounter);
};

} // namespace RAMCloud

#endif
