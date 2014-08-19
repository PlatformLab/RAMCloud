/* Copyright (c) 2011-2014 Stanford University
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

#ifndef RAMCLOUD_PERFCOUNTER_H
#define RAMCLOUD_PERFCOUNTER_H

#include <condition_variable>
#include <thread>

#include "Common.h"
#include "Cycles.h"
#include "SpinLock.h"
#include "ShortMacros.h"

namespace RAMCloud {
namespace Perf{

/**
 * The number of counters to bulk write at once when dumping to disk.
 */
const size_t WRITES_PER_BATCH =  100000;

/**
 * After recording this many intervals, we begin writing to disk.
 */
const size_t LOW_THRESHOLD   =  100000;

/**
 * After recording this many intervals, we begin dropping counters until
 * the disk-writing thread catches up.
 */
const size_t HIGH_THRESHOLD  = 1000000;

extern std::string serverName;
extern std::string logPath;

void terminationHandler(int signo);
void setNameAndPath(std::string serverName, std::string logPath);

/**
 * A class which handles storing and writing to disk of performance counters,
 * which are raw measurements of how long certain parts of code take to run.
 *
 * Each instance of this class represents one counter (a collection of
 * intervals  all corresponding to the same code being measured).
 *
 * This class is called EnabledCounter because it has a paired class known as
 * DisabledCounter, which has the same interface but does no work.
 * 
 * By typedefing the name PerfCounter to one of these two classes, we cause
 * the compiler to omit code when we do not want performance numbers to be
 * collected.
 */
class EnabledCounter{
   public:
       explicit EnabledCounter(const string& name);

       /**
        * Store the number of CPU ticks required for an operation, and notify
        * the background thread to flush in-memory buffers to file when
        * LOW_THRESHOLD numbers have been recorded.
        *
        * If HIGH_THRESHOLD intervals have been accumulated in memory, it will
        * begin dropping intervals.
        *
        * \param interval
        *      The number of CPU ticks to record.
        */
       void recordTime(const uint64_t& interval) {
           if (!enabled) return;
           std::lock_guard<std::mutex> _(mutex);
           if (ramQueue.size() == HIGH_THRESHOLD) {
               LOG(WARNING, "Counter %s overflowed on server %s, dropping "
                       "an interval.\n", name.c_str(), serverName.c_str());
               return;
           }
           ramQueue.push_back(interval);
           if (ramQueue.size() == LOW_THRESHOLD)
               countersAccumulated.notify_one();
       }

       ~EnabledCounter();
       void terminateBackgroundThread();

       static bool enabled;

   PRIVATE:

       void backgroundWriter();
       void writeCyclesPerSecond(FILE* handle);

       /**
        * Syntactic sugar to constructing filename that this counter writes to.
        */
       std::string getFileName() {
           return logPath + PATH_PREFIX + serverName + "_" + name;
       }

       /**
        * The name under which statistics for the counter will be logged.
        *
        * The name of the counter should match the variable name declared
        * using the DEFINE_COUNTER macro  in PerfCounter.cc.
        */
       string name;


       /**
        * Ordinary threads use recordTime to write counters to ramQueue without
        * any knowledge of diskQueue.  The diskWriterThread swaps the two
        * queues, and then writes the new diskQueue to disk.
        */
       std::vector<uint64_t> ramQueue;
       std::vector<uint64_t> diskQueue;

       /**
        * Prevent simultaneous calls to stop() on the same counter and thereby
        * protect the structure ticks from multithreaded access.
        *
        * It is also used to synchronize between recordTime threads and
        * diskWriterThread.
        */
       std::mutex mutex;

       /**
        * This flag is set to true by the destructor to tell the background
        * thread to exit. It is then set back to false once the background
        * thread has terminated.
        */
       bool terminate;

       /**
        * A CV for the disk writer to wait on, when there's not enough counters
        * to write.
        */
       std::condition_variable countersAccumulated;

       /**
        * Tracks the thread so that we can join it in the destructor to ensure
        * that it is not waiting on a nonexistent condition_variable.
        */
       std::thread diskWriterThread;

       /**
        * True means that calibration information (the number of cycles per
        * second) has been written to the log file.
        */
       bool wroteCyclesPerSecond;

   public:
       static const char* PATH_PREFIX;
       friend class EnabledInterval;
};

/**
  * For any metric set Foo, Foo::PerfCounter gets typedefed to this class iff
  * Foo is disabled.  
  *
  * Then the compiler will omit all function calls to recordTime and
  * constructors because of the empty method bodies.
 */
class DisabledCounter{
   public:
       explicit DisabledCounter(string name) { }
       void recordTime(const uint64_t& interval) { }
};


/**
 * This class provides automatic management for calling Cycles::rdtsc
 * before and after the sequence of statements we wish to measure, as well as
 * recording the difference.
 *
 * It will (optionally) make the first call at construction and stops on
 * destruction or explicit stop call.
 *
 * This class is one of a pair of classes {EnabledInterval, DisabledInterval},
 * which permit us to omit performance-related code at compile-time.
 */
class EnabledInterval {
    public:
        /**
         * Construct and optionally start an interval.
         */
        EnabledInterval(EnabledCounter* counter, bool start = true)
            : counter(counter), startTime(0), stopped(true)
        {
            /**
             * The value of the argument  start should be known at compile
             * time, so we expect the compiler to optimize the branch away.
             */
            if (start)
                this->start();
        }

        EnabledInterval(EnabledCounter* counter, const uint64_t& startTime)
            : counter(counter), startTime(0), stopped(true)
        {
            /**
             * The value of the argument  start should be known at compile
             * time, so we expect the compiler to optimize the branch away.
             */
            this->start(startTime);
        }

        /**
         * Ends an interval on destruction, if the interval is not already
         * ended.
         */
        ~EnabledInterval() {
            stop();
        }

        /**
         * Syntactic sugar for beginning an interval at the current time.
         */
        void start() {
            start(Cycles::rdtsc());
        }

        /**
         * This starts or restarts an interval, allowing reuse of an Interval
         * object.
         *
         * If this method is called twice, the first value will be overwritten
         * unconditionally.
         */
        void start(const uint64_t& startTime) {
            stopped = false;
            this->startTime = startTime;
        }

        /**
         * Syntactic sugar for ending an interval at the current time.
         */
        void stop() {
            stop(Cycles::rdtsc());
        }

        /**
         * Ends an interval, records the difference in time, and logs a message
         * if it is called multiple times in a row.
         *
         * Observe that we differentiate between double calls to stop and the
         * destructor calling stop after stop has already been called manually.
         * The former case is usually a problem, while the latter case happens
         * frequently.
         *
         * \param stopTime
         *      Enable the user to pass in a previously recorded time stamp.
         */
        void stop(const uint64_t& stopTime) {
            if (stopped)
                return;
            stopped = true;
            counter->recordTime(stopTime - startTime);
        }

        /**
         * Convenience method to avoid repeatedly reading expensive counters multiple times.
         * The return value should only be fed into another Interval within the same MetricSet.
         */
        uint64_t getStartTime() {
            return startTime;
        }
    PRIVATE:
        /**
         * The structure we store completed intervals in.
         */
        EnabledCounter* counter;

        /**
         * The time that the this interval began recording.
         */
        uint64_t startTime;

        /**
         * A sanity check to prevent double calls to stop.
         */
        bool stopped;
        DISALLOW_COPY_AND_ASSIGN(EnabledInterval);
};

/**
 * Dummy class for removing the performance impact of intervals when they are
 * not enabled. When a MetricSet is disabled, the compiler will remove the
 * function calls because of the empty method bodies.
 *
 * Justification for having separate disabled and enabled interval classes:
 * Constructors need to take an argument. If they do anything with the argument
 * (such as storing a pointer to it), then that code cannot be removed by the
 * compiler. Thus we need separate classes for disabled and enabled intervals.
 */
class DisabledInterval {
    public:
        DisabledInterval(DisabledCounter* counter, bool start = true) { }
        void start() { }
        void start(const uint64_t& startTime) { }
        void stop() { }
        void stop(const uint64_t& startTime) { }
        uint64_t getStartTime() { return 0; }
};

/**
 * A MetricSet is a set of counters that we can enable and disable together
 * using a compile-time flag.  
 *
 * All code below this line consists of definitions of MetricSets rather than
 * infrastructure code. 
 * 
 * In order to enable a MetricSet, add the following flag to the `make` command
 *     PERF=-DMACRO_NAME
 * where MACRO_NAME corresponds to the name given after the #if in a
 * particular MetricSet, such as COLLECT_READ_PERF.
 *
 * It is possible to enable multiple metric sets by using quotes as follows,
 * but it is not recommended in instances where the Intervals from these
 * MetricSets overlap.
 *     PERF='-DMACRO_NAME1 -DMACRO_NAME2'
 *
 * When a MetricSet is enabled, the output is available in a directory called
 * perfcounters under the log directory.
 *
 * ADDING A NEW MetricSet:
 *    1) Copy and paste one of the metric sets below, 
 *    2) Give a new meaningful name and corresponding macro name to the
 *       MetricSet.
 *    3) Delete all the declarations, and add PerfCounter declarations for
 *       every block of execution you want to measure.
 *    4) At the bottom of PerfCounter.cc, use the DEFINE_COUNTER macro to
 *       allocate storage and construct your counters. The first argument is
 *       the name of the MetricSet. The second argument is the name of the
 *       counter.
 */

/**
 * This MetricSet measures the performance cost of MasterService::read().
 */
struct ReadRPC_MetricSet {
#if COLLECT_READ_PERF
    typedef EnabledCounter PerfCounter;
    typedef EnabledInterval Interval;
#else
    typedef DisabledCounter PerfCounter;
    typedef DisabledInterval Interval;
#endif

    /* This counter measures the top of MasterService::read() to the bottom of
     * MasterService::read() */
    static PerfCounter readRpcTime;
};

/**
 * This MetricSet measures the path from when a request is received to when
 * it is returned.
 * */
struct ReadRequestHandle_MetricSet {
#if COLLECT_READ_REQUEST_PERF
    typedef EnabledCounter PerfCounter;
    typedef EnabledInterval Interval;
#else
    typedef DisabledCounter PerfCounter;
    typedef DisabledInterval Interval;
#endif

    /* This counter marks the time from when the request is received to the
     * call to handleRPC. */
    static PerfCounter requestToHandleRpc;
    /* This counter marks the time from when handleRpc is called to when
     * control returns to the transport. */
    static PerfCounter rpcServiceTime;
    /* This counter marks the time from when control returns to the transport
     * to postSend. */
    static PerfCounter serviceReturnToPostSend;

};

/**
 * This MetricSet measures the time it takes to do each thread handoff during a
 * Read RPC.
 * */
struct ReadThreadingCost_MetricSet {
#if COLLECT_READ_THREAD_PERF
    typedef EnabledCounter PerfCounter;
    typedef EnabledInterval Interval;
#else
    typedef DisabledCounter PerfCounter;
    typedef DisabledInterval Interval;
#endif

    /* This counter marks the time from right before we do the thread handoff
     * to when the work starts being done. */
    static PerfCounter enqueueThreadToStartWork;
    /* This counter marks the time to perform the actual RPC work until just
     * before the thread handoff back to the service thread. */
    static PerfCounter threadWork;
    /* This counter marks the time it takes to get from completion of work back
     * to the service thread. */
    static PerfCounter returnToTransport;

    /* This counter measures the same code as  threadWork above, except the
     * work is done directly in the service thread without a thread handoff.  .
     */
    static PerfCounter noThreadWork;
};

}  // End Perf
}; // End RAMCloud

#endif // RAMCLOUD_PERFCOUNTER_H
