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
 * Header file for the RAMCloud::Metrics class.
 */

#ifndef RAMCLOUD_METRICS_H
#define RAMCLOUD_METRICS_H

#include <Common.h>

#include <PerfCounterType.h>
#include <Mark.h>

namespace RAMCloud {

/**
 * Captures the change in a performance counter or occurrences of some
 * event during execution of a piece of code.
 *
 * Marks are points on a particular trace of the codepath followed on
 * a RPC.  Metrics is used by calling setup() before a trace through
 * the codepath begins to select a #PerfCounterType and two Marks.
 * After the trace occurs the difference of the perf counter between
 * the marks (or the number of times the mark has been seen on the
 * entire trace for increment mode) can be collected via read().
 * #PerfCounterType allows operation in three modes: timestamp counter
 * delta mode (to get cycles between marks), performance counter delta
 * mode (to get performance counter events between marks), and
 * increment mode (to count the number of times a particular mark
 * occurs).
 *
 * Most users of this class will be interested in placing marks in
 * their own code for clients to measure and need not worry about
 * setup() and read().
 *
 * Marks are defined in by creating a new #Mark and placed into the
 * codepath with
 *
 * Metrics::mark(MARK_MY_INTERESTING_POINT);
 *
 * All methods are static allowing Metrics to be used anywhere.
 */
class Metrics {
  public:
    static uint64_t read();
    static void setup(PerfCounterType type, Mark beginMark, Mark endMark);
    static void setup(rcrpc_perf_counter which);
    static void mark(Mark mark);
  private:
    /// Used to track whether a legal pattern of marks has been seen.
    enum State {
        /// Haven't seen a related mark yet since last setup
        NO_DATA,
        /// A beginMark has been encountered but not a endMark.
        /// Only happens on delta style metrics.
        STARTED,
        /// Have seen all the requisite marks such that read()
        /// returns a valid value.
        VALID,
    };

    /// Identifies the single Mark at any given time that is of interest
    /// (such as the beginning or end of an interval).
    static Mark activeMark;

    /// A snapshot of the selected perf counter when beginMark is marked.
    /// Only used on delta type counters.
    static uint64_t beginCount;

    /// Maintains the current value of the performance or event counter.
    static uint64_t count;

    /// Selected via setup, describes which perf counter is used.
    static PerfCounterType counterType;

    /// Selected via setup, describes which Mark is used to trigger the
    /// start of a delta.
    static Mark beginMark;

    /// Selected via setup, describes which Mark is used to trigger the
    /// end of a delta.
    static Mark endMark;

    /// Internally tracks whether beginMark and endMark have been encountered.
    static State state;

    /// Never used
    Metrics() {}
    /// Never used
    ~Metrics() {}

    friend class MetricsTest;
    friend class BlockMetricTest;
};

/**
 * Used in conjuction with Metrics to define start and stop #Mark
 * boundaries that are tied to a scope.
 *
 * Simply construct a BlockMetric on the stack with the appropriate
 * beginMark, which will be marked immediately at that point in the
 * code.  When the BlockMetric instance goes out of scope its
 * destructor will automatically mark the endMark selected when the
 * object was constructed.
 */
class BlockMetric {
  public:
    /**
     * \param beginMark
     *     The #Mark to mark immediately on object construction.
     * \param endMark
     *     The #Mark to mark on destruction (when it goes out of scope
     *     if it is on the stack).
     */
    explicit BlockMetric(Mark beginMark, Mark endMark)
            : stop(endMark) {
        stop = endMark;
        Metrics::mark(beginMark);
    }
    ~BlockMetric() {
        Metrics::mark(stop);
    }
  private:
    /// The #Mark to mark when the object is destroyed.
    Mark stop;
};

}  // namespace RAMCloud

#endif
