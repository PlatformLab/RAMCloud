/* Copyright (c) 2014-2016 Stanford University
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

#ifndef RAMCLOUD_TIMETRACE_H
#define RAMCLOUD_TIMETRACE_H

#include "Common.h"
#include "Atomic.h"
#include "Cycles.h"
#include "Logger.h"
#include "SpinLock.h"
#include "WorkerTimer.h"

namespace RAMCloud {

/**
 * This class implements a circular buffer of entries, each of which
 * consists of a fine-grain timestamp and a short descriptive string.
 * It's typically used to record times at various points in an operation,
 * in order to find performance bottlenecks. It can record a trace relatively
 * efficiently, and then either return the trace either as a string or
 * print it to the system log.
 *
 * This class is thread-safe.
 */
class TimeTrace {
  PUBLIC:
    TimeTrace();
    ~TimeTrace();
    void record(uint64_t timestamp, const char* format, uint32_t arg0 = 0,
            uint32_t arg1 = 0, uint32_t arg2 = 0, uint32_t arg3 = 0);
    void record(const char* format, uint32_t arg0 = 0, uint32_t arg1 = 0,
            uint32_t arg2 = 0, uint32_t arg3 = 0) {
        record(Cycles::rdtsc(), format, arg0, arg1, arg2, arg3);
    }
    void printToLog();
    void printToLogBackground(Dispatch* dispatch);
    string getTrace();
    void reset();

  PROTECTED:
    void printInternal(string* s);

    /**
     * This class is used to print the time trace to the log in the
     * background (as a WorkerTimer). Printing can take a long time,
     * so doing it in the foreground can potentially make a server
     * appear crashed.
     */

    class TraceLogger : public WorkerTimer {
      public:
        TraceLogger(Dispatch* dispatch, TimeTrace* timeTrace)
                : WorkerTimer(dispatch)
                , timeTrace(timeTrace)
        { }

        virtual void handleTimerEvent() {
            timeTrace->printToLog();
        }

      PRIVATE:
        // The TimeTrace to print to the log.
        TimeTrace* timeTrace;
        DISALLOW_COPY_AND_ASSIGN(TraceLogger);
    };

    /**
     * This structure holds one entry in the TimeTrace.
     */
    struct Event {
      uint64_t timestamp;        // Time when a particular event occurred.
      const char* format;        // Format string describing the event.
                                 // NULL means that this entry is unused.
      uint32_t arg0;             // Argument that may be referenced by format
                                 // when printing out this event.
      uint32_t arg1;             // Argument that may be referenced by format
                                 // when printing out this event.
      uint32_t arg2;             // Argument that may be referenced by format
                                 // when printing out this event.
      uint32_t arg3;             // Argument that may be referenced by format
                                 // when printing out this event.
    };

    // Total number of events that we can retain any given time.
    static const int BUFFER_SIZE = 10000;

    // Number of events to prefetch ahead, in order to minimize cache
    // misses.
    static const int NUM_PREFETCH = 2;

    // Used in a few cases where exclusive access is desirable (e.g. when
    // printing the log).
    SpinLock mutex;

    // Holds information from the most recent calls to the record method.
    // Note: prefetching will cause NUM_PREFETCH extra elements past the
    // end of the buffer, to be accessed (allocating extra space avoids
    // the cost of being cleverer during prefetching).
    Event events[BUFFER_SIZE + NUM_PREFETCH];

    // Index within events of the slot to use for the next call to the
    // record method.
    Atomic<int> nextIndex;

    // True means that printInternal is currently running, so it is not
    // safe to add more records, since that could result in inconsistent
    // output from printInternal.
    volatile bool readerActive;

    // For logging time traces in the background.
    Tub<TraceLogger> backgroundLogger;

  public:
    // Refers to the most recently created time trace; provides a convenient
    // global variable for situations where no other TimeTrace pointer
    // is readily available.
    static TimeTrace* globalTimeTrace;
};

} // namespace RAMCloud

#endif // RAMCLOUD_TIMETRACE_H

