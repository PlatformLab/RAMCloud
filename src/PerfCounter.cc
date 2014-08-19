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

#include "PerfCounter.h"

#include <pthread.h>
#include <signal.h>
#include <mutex>
#include <sstream>
#include <string>


/**
 * This macro is used for allocating the storage for counters declared inside a
 * MetricSet in PerfCounter.h
 */
#define DEFINE_COUNTER(MetricSet, CounterName) \
    MetricSet::PerfCounter MetricSet::CounterName(#CounterName)

namespace RAMCloud { namespace Perf {


/**
 * serverName and logPath are used together to set the directory for dumping
 * performance counters. In general, the performance counters for a given
 * run will live in logPath/perfcounters.
 *
 * The serverName is an unique identifier for a server, used to write counters
 * for each server to different files.
 *
 * The logPath has a terminating '/', while the serverName does not have a
 * preceding '/'. 
 */
std::string serverName;
std::string logPath;

/**
 * Prevent serverName and logPath from being set (by setNameAndPath) and read
 * (by getFileName() as called by backgroundWriter) simultaneously.
 */
static std::mutex fileNameLock;

/**
 * Allow client to  to globally disable counter writing at run time.
 * This is primarily useful for performance measurements which have a
 * long warm-up.
 *
 */
bool EnabledCounter::enabled = false;

/**
 * Counters explicitly register themselves by adding themselves to this queue
 * in the constructor.  They are then cleaned up in the SIGTERM handler in
 * ServerMain.cc.
 */
std::vector<EnabledCounter*>& enabledCounters() {
    static std::vector<EnabledCounter*> _;
    return _;
}


/**
 * This is the relative file path to the directory  where all
 * measurement data files are written out.  It does not have a preceding
 * '/' but it does have a terminating '/'.
 */
const char* EnabledCounter::PATH_PREFIX = "perfcounters/";

/**
 * Explicitly call terminateBackgroundThread on all global counters when
 * SIGTERM is used to terminate.
 */
void terminatePerfCounters() {
    // We make a copy instead of directly using the global structure because we
    // will deadlock if we try to hold the lock while terminating threads.
    vector<EnabledCounter*> enabledCounters;
    {
        std::unique_lock<std::mutex> ul(Perf::fileNameLock);
        enabledCounters = Perf::enabledCounters();
    }
    for (size_t i = 0; i < enabledCounters.size(); i++)
        enabledCounters[i]->terminateBackgroundThread();
    signal(SIGTERM, SIG_DFL);
    kill(getpid(), SIGTERM);
}

/**
  * Start a new thread to do the actual work, so the signal handler can return
  * and not block an arbitrary thread, which may cause a deadlock.
 */
void terminationHandler(int signo) {
    std::thread(terminatePerfCounters).detach();
}

/**
 * Stores the name of the server and the log path prefix in the variables
 * serverName and logPath for use in dumping counters to file.
 * 
 * This function should only ever be called once per process.
 */
void setNameAndPath(std::string serverName, std::string logPath) {
    std::unique_lock<std::mutex> ul(fileNameLock);
    Perf::serverName = serverName;
    Perf::logPath = logPath;
}

/**
 * Write the number of cycles per second to the counter file, so that we
 * can convert the raw counter values to real time units in the future.
 * \param handle
 *      The file handle to write the cycles to.
 */
void EnabledCounter::writeCyclesPerSecond(FILE* handle) {
    if (!wroteCyclesPerSecond) {
        double cyclesPerSececond = Cycles::perSecond();
        fwrite(&cyclesPerSececond, sizeof(cyclesPerSececond), 1, handle);
        wroteCyclesPerSecond = true;
    }
}

/**
 * The main method for the background thread that writes counters to
 * disk. It is functionally a Consumer in the Producer-Consumer model.
 */
void EnabledCounter::backgroundWriter() {
    FILE* handle = NULL;
    for (;;) {
        {
            std::unique_lock<std::mutex> ul(mutex);
            while (ramQueue.size() < LOW_THRESHOLD && !terminate) {
                RAMCLOUD_TEST_LOG("Not enough counters, going to sleep!");
                countersAccumulated.wait(ul);
                RAMCLOUD_TEST_LOG("backgroundWriter awakening!");
            }

            if (terminate && ramQueue.empty())
                return;
            std::swap(ramQueue, diskQueue);
        }

        {
            std::unique_lock<std::mutex> ul(fileNameLock);
            if (logPath.empty() || serverName.empty()) {
                diskQueue.clear();
                RAMCLOUD_TEST_LOG("No serverName, clearing the diskQueue!");
                continue;
            }

            handle = fopen(getFileName().c_str(), "ab");
        }

        // Write the cycles per second here instead of constructor, because
        // we do not know the logPath at constructor time.
        writeCyclesPerSecond(handle);

        // Batch write the first WRITES_PER_BATCH
        // We put WRITES_PER_BATCH on the left to avoid the unassigned
        // subtraction if WRITES_PER_BATCH > diskQueue.size();
        size_t i;
        for (i = 0; i + WRITES_PER_BATCH < diskQueue.size();
                i+=WRITES_PER_BATCH)
            fwrite(&diskQueue[i], sizeof(uint64_t), WRITES_PER_BATCH,
                    handle);

        // Write the remaining counters
        if (i < diskQueue.size())
            fwrite(&diskQueue[i], sizeof(uint64_t), diskQueue.size() - i,
                    handle);

        diskQueue.clear();
        if (handle)
            fclose(handle);
    }
}

/**
 * Utility method (for ease of testing) to ensure that the diskWriterThread
 * cleanly terminates before we permit the object to be fully destroyed.
 *
 * This method is generally called by the destructor, but for global static
 * counters, it is called by terminatePerfCounters() when a SIGTERM is
 * received.
 */
void EnabledCounter::terminateBackgroundThread() {
    {
        std::unique_lock<std::mutex> ul(mutex);
        if (terminate) {
            return;
        }
        terminate = true;
    }

    countersAccumulated.notify_one();
    diskWriterThread.join();
}

EnabledCounter::EnabledCounter(const string& name)
    : name(name)
    , ramQueue()
    , diskQueue()
    , mutex()
    , terminate(false)
    , countersAccumulated()
    , diskWriterThread(&EnabledCounter::backgroundWriter, this)
    , wroteCyclesPerSecond(false)
{
    std::unique_lock<std::mutex> ul(fileNameLock);
    enabledCounters().push_back(this);
}
/**
 * See documentation for terminateBackgroundThread().
 */
EnabledCounter::~EnabledCounter() {
    terminateBackgroundThread();
}

/**
 * Documentation for these counters is available in PerfCounter.h
 */
DEFINE_COUNTER(ReadRPC_MetricSet, readRpcTime);

DEFINE_COUNTER(ReadRequestHandle_MetricSet, requestToHandleRpc);
DEFINE_COUNTER(ReadRequestHandle_MetricSet, rpcServiceTime);
DEFINE_COUNTER(ReadRequestHandle_MetricSet, serviceReturnToPostSend);

DEFINE_COUNTER(ReadThreadingCost_MetricSet, enqueueThreadToStartWork);
DEFINE_COUNTER(ReadThreadingCost_MetricSet, threadWork);
DEFINE_COUNTER(ReadThreadingCost_MetricSet, returnToTransport);

DEFINE_COUNTER(ReadThreadingCost_MetricSet, noThreadWork);

}} // namespace Perf namespace RAMCloud
#undef DEFINE_COUNTER
