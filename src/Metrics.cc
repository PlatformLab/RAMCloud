/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for
 * any purpose with or without fee is hereby granted, provided that
 * the above copyright notice and this permission notice appear in all
 * copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL
 * WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL
 * AUTHORS BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR
 * CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
 * OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
 * NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file
 * Implementation file for the RAMCloud::Metrics class.
 */

#include <Metrics.h>

namespace RAMCloud {

uint64_t Metrics::beginCount = 0;
uint64_t Metrics::count = 0;
PerfCounterType Metrics::counterType = PERF_COUNTER_TSC;
Mark Metrics::activeMark = MARK_NONE;
Mark Metrics::beginMark = MARK_NONE;
Mark Metrics::endMark = MARK_NONE;
Metrics::State Metrics::state = Metrics::NO_DATA;

/**
 * Returns the value of the selected metric.
 *
 * \return
 *      See rc_read_perf_counter().
 */
uint64_t
Metrics::read()
{
    if (counterType != PERF_COUNTER_INC) {
        if (state != VALID)
            return 0lu;
    }
    return count - beginCount;
}

/**
 * Selects a particular metric to collect, canceling any previously
 * selected metric.
 *
 * \param counterType
 *      See rc_select_perf_counter().
 * \param beginMark
 *      See rc_select_perf_counter().
 * \param endMark
 *      See rc_select_perf_counter().
 */
void
Metrics::setup(PerfCounterType counterType, Mark beginMark, Mark endMark)
{
    Metrics::counterType = counterType;
    Metrics::beginMark = beginMark;
    Metrics::endMark = endMark;
    activeMark = beginMark;
    beginCount = 0;
    count = 0;
    state = NO_DATA;
}

/**
 * Given the field from an incoming RPC specifying a performance
 * counter that the client would like measured, arranges to measure the
 * corresponding metric.
 *
 * \param which
 *      The perf_counter field from an incoming RPC.  See
 *      rcrpc_perf_counter() for details.
 */
void
Metrics::setup(rcrpc_perf_counter which)
{
    setup(static_cast<PerfCounterType>(which.counterType),
          static_cast<Mark>(which.beginMark),
          static_cast<Mark>(which.endMark));
}

/**
 * Mark a point of interest for performance in the code.
 *
 * If mark is related to the current metric (as specified in the most
 * recent call to setup()) then performance information is gathered.
 *
 * \param mark
 *      Identifies a particular event has just occurred.
 */

void
Metrics::mark(Mark mark)
{
    if (activeMark != mark)
        return;

    if (counterType == PERF_COUNTER_INC) {
        count++;
        return;
    }

    // INC handled above, it must be either TSC or PMC
    uint64_t count = counterType == PERF_COUNTER_TSC ? rdtsc() : rdpmc(0);
    switch (state) {
    case NO_DATA:
        // haven't seen anything yet so record this as the beginMark
        beginCount = count;
        activeMark = endMark;
        state = STARTED;
        break;
    case STARTED:
        // have seen start so record this as the endMark
        // fall through
    case VALID:
        // have seen both already so record this as the
        // replacement endMark so we have the first start to the
        // last stop mark
        count = count;
        state = VALID;
        break;
    default:
        // not reached
        break;
    }
}

}  // namespace RAMCloud
