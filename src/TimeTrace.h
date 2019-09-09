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
 * consists of a fine-grain timestamp, a short descriptive string, and
 * a few additional values. It's typically used to record times at
 * various points in an operation, in order to find performance bottlenecks.
 * It can record a trace relatively efficiently (< 10ns as of 6/2016),
 * and then either return the trace either as a string or print it to
 * the system log.
 *
 * This class is thread-safe. By default, trace information is recorded
 * separately for each thread in order to avoid synchronization and cache
 * consistency overheads; the thread-local traces are merged by methods
 * such as printToLog, so the existence of multiple trace buffers is
 * normally invisible.
 *
 * The TimeTrace class should never be constructed; it offers only
 * static methods.
 *
 * If you want to use a single trace buffer rather than per-thread
 * buffers, see the subclass TimeTrace::Buffer below.
 */
class TimeTrace {
  public:
    class Buffer;

    /**
     * Return a thread-local buffer that can be used to record events from the
     * calling thread, creating a new buffer if necessary.
     */
    static inline Buffer* getBuffer() {
        if (expect_false(threadBuffer == NULL)) {
            createThreadBuffer();
        }
        return threadBuffer;
    }

    static string getTrace();
    static void printToLog();
    static void printToLogBackground(Dispatch* dispatch);

    /**
     * Record an event in a thread-local buffer, creating a new buffer
     * if this is the first record for this thread.
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
     * \param args
     *      Arguments to use when printing a message about this event.
     */
    template<typename... Args>
    static inline void record(uint64_t timestamp, const char* format,
            Args... args)
    {
        getBuffer()->record(timestamp, format, args...);
    }

    template<typename... Args>
    static inline void record(const char* format, Args... args) {
        record(Cycles::rdtsc(), format, args...);
    }

    static void reset();

  PROTECTED:
    TimeTrace();
    static void createThreadBuffer();
    static void printInternal(std::vector<TimeTrace::Buffer*>* traces,
            string* s);

    /**
     * This class is used to print the time trace to the log in the
     * background (as a WorkerTimer). Printing can take a long time,
     * so doing it in the foreground can potentially make a server
     * appear crashed. All instances are assumed to be dynamically
     * allocated;they delete themselves automatically.
     */
    class TraceLogger : public WorkerTimer {
      public:
        explicit TraceLogger(Dispatch* dispatch)
                : WorkerTimer(dispatch)
                , isFinished(false)
        {
            start(0);
        }
        virtual void handleTimerEvent();

        // Set to true once it has finished logging.
        bool isFinished;
        DISALLOW_COPY_AND_ASSIGN(TraceLogger);
    };


    // Points to a private per-thread TimeTrace::Buffer object; NULL means
    // no such object has been created yet for the current thread.
    static __thread Buffer* threadBuffer;

    // Holds pointers to all of the thread-private TimeTrace objects created
    // so far. Entries never get deleted from this object.
    static std::vector<Buffer*> threadBuffers;

    // Used for logging TimeTraces in the background as a WorkerTimer.
    // Allocated/reallocated during calls to printToLogBackground.
    // Warning: making backgroundLogger a static variable rather than a pointer
    // causes order-of-deletion problems during static variable destruction.
    static TraceLogger* backgroundLogger;

    // Provides mutual exclusion on threadBuffers and backgroundLogger.
    static SpinLock mutex;

    // Count of number of calls to print* that are currently active;
    // if nonzero, then it isn't safe to log new entries, since this
    // could interfere with readers.
    static Atomic<int> activeReaders;

    /**
     * This structure holds one entry in a TimeTrace::Buffer.
     */
    struct Event {
      uint64_t timestamp;        // Time when a particular event occurred.
      const char* format;        // Format string describing the event.
                                 // NULL means that this entry is unused.
      uint32_t args[4];          // Arguments that may be referenced by format
                                 // when printing out this event.
    };

  public:
    /**
     * Represents a sequence of events, typically consisting of all those
     * generated by one thread.  Has a fixed capacity, so slots are re-used
     * on a circular basis.  This class is not thread-safe.
     */
    class Buffer {
      public:
        Buffer();
        ~Buffer();
        string getTrace();
        void printToLog();

        /**
         * Record an event in the buffer.
         *
         * \param format
         *      A format string for snprintf that will be used, along with
         *      arg0..arg3, to generate a human-readable message describing what
         *      happened, when the time trace is printed. The message is generated
         *      by calling snprintf as follows:
         *      snprintf(buffer, size, format, arg0, arg1, arg2, arg3)
         *      where format and arg0..arg3 are the corresponding arguments to this
         *      method. This pointer is stored in the buffer, so the caller must
         *      ensure that its contents will not change over its lifetime in the
         *      trace.
         * \param args
         *      Arguments to use when printing a message about this event.
         */
        template<typename... Args>
        inline void record(const char* format, Args... args)
        {
            record(Cycles::rdtsc(), format, args...);
        }

        template<typename... Args>
        inline void record(uint64_t timestamp, const char* format, Args... args)
        {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnarrowing"
            uint32_t eventArgs[] = {args...};
#pragma GCC diagnostic pop
            if (expect_true(TimeTrace::activeReaders == 0)) {
                Event* event = &events[nextIndex];
                nextIndex = (nextIndex + 1) & BUFFER_MASK;

                // There used to be code here for prefetching the next few
                // events, in order to minimize cache misses on the array of
                // events. However, performance measurements indicate that this
                // actually slows things down by 2ns per invocation.
                // prefetch(event+1, NUM_PREFETCH*sizeof(Event));

                event->timestamp = timestamp;
                event->format = format;
                for (size_t i = 0; i < sizeof...(args); i++) {
                    event->args[i] = eventArgs[i];
                }
            }
        }

        void reset();

      PROTECTED:
        // Determines the number of events we can retain as an exponent of 2
        static const uint8_t BUFFER_SIZE_EXP = 14;

        // Total number of events that we can retain any given time.
        static const uint32_t BUFFER_SIZE = 1 << BUFFER_SIZE_EXP;

        // Bit mask used to implement a circular event buffer
        static const uint32_t BUFFER_MASK = BUFFER_SIZE - 1;

        // Index within events of the slot to use for the next call to the
        // record method.
        int nextIndex;

        // Holds information from the most recent calls to the record method.
        TimeTrace::Event events[BUFFER_SIZE];

        // Identifier of the thread that owns this buffer.
        int threadId;

        friend class TimeTrace;
        DISALLOW_COPY_AND_ASSIGN(Buffer);
    };
};

} // namespace RAMCloud

#endif // RAMCLOUD_TIMETRACE_H

