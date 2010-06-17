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
 * Header file for the RAMCloud::PerfCounterType enum.
 */

#ifndef RAMCLOUD_PERFCOUNTERTYPE_H
#define RAMCLOUD_PERFCOUNTERTYPE_H

namespace RAMCloud {

/**
 * Used to select which performance counter is of interest for
 * profiling (see rc_select_perf_counter()) and Mark).
 */
enum PerfCounterType {
    /** CPU timestamp counter as returned by rdtsc(). */
    PERF_COUNTER_TSC = 0,
    /**
     * CPU performance counter 0 as returned by rdpmc(0).
     * For the moment one must use the external ramcloud Linux kernel
     * module to control which CPU performance counter is active.
     * The ramcloud kernel module must be loaded on the server before
     * selecting this counter or the Server will crash.
     */
    PERF_COUNTER_PMC,
    /**
     * Count the number of times a particular #Mark is encountered
     * during a trace.
     */
    PERF_COUNTER_INC,
};

} // namespace RAMCloud

#endif
