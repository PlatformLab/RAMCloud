/* Copyright (c) 2014-2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TimeTrace.h"

namespace RAMCloud {
TimeTrace* TimeTrace::globalTimeTrace;

/**
 * Construct a TimeTrace.
 */
TimeTrace::TimeTrace()
    : mutex("TimeTrace::mutex")
    , events()
    , nextIndex(0)
    , readerActive(false)
    , backgroundLogger()
{
    // Mark all of the events invalid.
    for (int i = 0; i < BUFFER_SIZE; i++) {
        events[i].format = NULL;
    }
    globalTimeTrace = this;
}

/**
 * Destructor for TimeTrace.
 */
TimeTrace::~TimeTrace()
{
}

/**
 * Record an event in the trace.
 *
 * \param timestamp
 *      Identifies the time at which the event occurred.
 * \param format
 *      A format string for snprintf that will be used, along with
 *      arg0..arg3, to generate a human-readable message describing what
 *      happened, when the time trace is printed. The message is generated
 *      by calling snprintf as follows:
 *      snprintf(buffer, size, format, arg0, arg1, arg2, arg3)
 *      where format and arg0..arg3 are the corresponding arguments to this
 *      method. This pointer is stored in the time trace, so the caller must
 *      ensure that its contents will not change over its lifetime in the
 *      trace.
 * \param arg0
 *      Argument to use when printing a message about this event.
 * \param arg1
 *      Argument to use when printing a message about this event.
 * \param arg2
 *      Argument to use when printing a message about this event.
 * \param arg3
 *      Argument to use when printing a message about this event.
 */
void TimeTrace::record(uint64_t timestamp, const char* format,
        uint32_t arg0, uint32_t arg1, uint32_t arg2, uint32_t arg3)
{
    if (readerActive) {
        return;
    }
    int current;

    // This loop allocates a slot for the new event and handles
    // synchronization between threads; if there is a race, it may
    // execute multiple times.
    while (1) {
        current = nextIndex;

        // Don't use % to increment current with wraparound: it's too slow.
        int newNext = current + 1;
        if (newNext == BUFFER_SIZE) {
            newNext = 0;
        }
        if (nextIndex.compareExchange(current, newNext) == current) {
            break;
        }
    }

    Event* event = &events[current];

    // Prefetch the next few events, in order to minimize cache misses on
    // the buffer.
    prefetch(event+1, NUM_PREFETCH*sizeof(Event));

    event->timestamp = timestamp;
    event->format = format;
    event->arg0 = arg0;
    event->arg1 = arg1;
    event->arg2 = arg2;
    event->arg3 = arg3;
}

/**
 * Return a string containing a printout of the records in the trace.
 */
string TimeTrace::getTrace()
{
    string s;
    printInternal(&s);
    return s;
}

/**
 * Print all existing trace records to the system log.
 */
void TimeTrace::printToLog()
{
    printInternal(NULL);
}

/**
 * Print all existing trace records to the system log, but do it
 * in the background and return immediately, before it is done.
 *
 * \param dispatch
 *      Dispatch that can be used to schedule a WorkerTimer to
 *      log the time trace.
 */
void
TimeTrace::printToLogBackground(Dispatch* dispatch)
{
    SpinLock::Guard guard(mutex);
    if (!backgroundLogger) {
        backgroundLogger.construct(dispatch, this);
    }
    backgroundLogger->start(0);
}

/**
 * Discard any existing trace records.
 */
void TimeTrace::reset()
{
    for (int i = 0; i < BUFFER_SIZE; i++) {
        if (events[i].format == NULL) {
            break;
        }
        events[i].format = NULL;
    }
    nextIndex = 0;
}

/**
 * This private method does most of the work for both printToLog and
 * getTrace.
 *
 * \param s
 *      If non-NULL, refers to a string that will hold a printout of the
 *      time trace. If NULL, the trace will be printed on the system log.
 */
void TimeTrace::printInternal(string* s)
{
    {
        SpinLock::Guard guard(mutex);
        if (readerActive) {
            RAMCLOUD_LOG(WARNING,
                    "printInternal already active; skipping this call");
            return;
        }
        readerActive = true;
    }

    // Find the oldest event that we still have (either events[nextIndex],
    // or events[0] if we never completely filled the buffer).
    int i = nextIndex;
    if (events[i].format == NULL) {
        i = 0;
        if (events[0].format == NULL) {
            if (s != NULL) {
                s->append("No time trace events to print");
            } else {
                RAMCLOUD_LOG(NOTICE, "No time trace events to print");
            }
            readerActive = false;
            return;
        }
    }

    // Retrieve a "starting time" so we can print individual event times
    // relative to the starting time.
    uint64_t start = events[i].timestamp;
    double prevTime = 0.0;

    // Each iteration through this loop processes one event from the trace.
    do {
        char buffer[1000];
        double ns = Cycles::toSeconds(events[i].timestamp - start) * 1e09;
        if (s != NULL) {
            if (s->length() != 0) {
                s->append("\n");
            }
            snprintf(buffer, sizeof(buffer), "%8.1f ns (+%6.1f ns): ",
                    ns, ns - prevTime);
            s->append(buffer);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
            snprintf(buffer, sizeof(buffer), events[i].format, events[i].arg0,
                     events[i].arg1, events[i].arg2, events[i].arg3);
#pragma GCC diagnostic pop
            s->append(buffer);
        } else {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
            snprintf(buffer, sizeof(buffer), events[i].format, events[i].arg0,
                     events[i].arg1, events[i].arg2, events[i].arg3);
#pragma GCC diagnostic pop
            RAMCLOUD_LOG(NOTICE, "%8.1f ns (+%6.1f ns): %s", ns, ns - prevTime,
                    buffer);
        }
        i = (i+1)%BUFFER_SIZE;
        prevTime = ns;
        // The NULL test below is only needed for testing.
    } while ((i != nextIndex) && (events[i].format != NULL));
    readerActive = false;
}

} // namespace RAMCloud
