/* Copyright (c) 2011-2016 Stanford University
 * Copyright (c) 2011 Facebook
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

// This program contains a collection of low-level performance measurements
// for RAMCloud, which can be run either individually or altogether.  These
// tests measure performance in a single stand-alone process, not in a cluster
// with multiple servers.  Invoke the program like this:
//
//     Perf [options] test1 test2 ...
//
// If no test names are provided then all of the performance tests are run.
// Otherwise, test1 and test2 are substrings that are matched against the
// names of individual performance measurements (ignoring case); a test is
// run if its name contains any of the substrings.
//
// To add a new test:
// * Write a function that implements the test.  Use existing test functions
//   as a guideline, and be sure to generate output in the same form as
//   other tests.
// * Create a new entry for the test in the #tests table.

#include <sched.h>

#if (__GNUC__ == 4 && __GNUC_MINOR__ >= 5) || (__GNUC__ > 4)
#include <atomic>
#else
#include <cstdatomic>
#endif
#include <sys/time.h>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Weffc++"
#include <boost/program_options.hpp>
#pragma GCC diagnostic pop

#if __cplusplus >= 201402L
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Weffc++"
#include "flat_hash_map.h"
#pragma GCC diagnostic pop
#endif

#include "Common.h"
#include "Atomic.h"
#include "Cycles.h"
#include "CycleCounter.h"
#include "Dispatch.h"
#include "Fence.h"
#include "LockTable.h"
#include "Logger.h"
#include "Memory.h"
#include "MurmurHash3.h"
#include "Object.h"
#include "ObjectPool.h"
#include "QueueEstimator.h"
#include "Segment.h"
#include "SegmentIterator.h"
#include "SpinLock.h"
#include "ClientException.h"
#include "PerfHelper.h"
#include "TimeTrace.h"
#include "Util.h"

using namespace RAMCloud;

// Uncomment the following line (or specify -D PERFTT on the make command line)
// to enable a bunch of time tracing in this module.
// #define PERFTT 1

// Provides a shorthand way of invoking TimeTrace::record, compiled in or out
// by the SMTT #ifdef.
void
timeTrace(const char* format, uint32_t arg0 = 0,
        uint32_t arg1 = 0, uint32_t arg2 = 0, uint32_t arg3 = 0)
{
#ifdef PERFTT
    TimeTrace::record(format, arg0, arg1, arg2, arg3);
#endif
}
void
timeTrace(uint64_t timestamp, const char* format, uint32_t arg0 = 0,
        uint32_t arg1 = 0, uint32_t arg2 = 0, uint32_t arg3 = 0)
{
#ifdef PERFTT
    TimeTrace::record(timestamp, format, arg0, arg1, arg2, arg3);
#endif
}

// For tests involving interactions between cores, these variables
// specify the two cores to use. Core2 can be controlled with the
// "--secondCore" command-line option.
int core1 = 2;
int core2 = 3;

// Holds CPU affinities when application starts.
cpu_set_t savedAffinities;

/**
 * Ask the operating system to pin the current thread to a given CPU.
 *
 * \param cpu
 *      Indicates the desired CPU and hyperthread; low order 2 bits
 *      specify CPU, next bit specifies hyperthread.
 */
void pinThread(int cpu)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    sched_setaffinity((pid_t)syscall(SYS_gettid), sizeof(set), &set);
    int current = sched_getcpu();
    if (current != cpu) {
        printf("Thread is on wrong CPU: asked for %d, currently %d\n",
                cpu, current);
    }
}

/**
 * Reverse the effects of a previous call to pinThread: restore
 * initial CPU affinity mask.
 */
void unpinThread()
{
    sched_setaffinity(0, sizeof(savedAffinities), &savedAffinities);
}

/*
 * This function just discards its argument. It's used to make it
 * appear that data is used,  so that the compiler won't optimize
 * away the code we're trying to measure.
 *
 * \param value
 *      Pointer to arbitrary value; it's discarded.
 */
void discard(void* value) {
    int x = *reinterpret_cast<int*>(value);
    if (x == 0x43924776) {
        printf("Value was 0x%x\n", x);
    }
}

//----------------------------------------------------------------------
// Test functions start here
//----------------------------------------------------------------------

// Simulates the context of an Arachne thread context.
struct ArachneThreadContext {
    volatile uint64_t wakeupTimeInCycles;
    volatile uint64_t startTime;
    volatile int args[16];
};

// This function runs the second thread for arachneThreadCreate.
void arachneThreadCreateWorker(Atomic<uint64_t>* maskAndCount,
        ArachneThreadContext* threadContext, volatile uint64_t* elapsed)
{
    pinThread(core2);
    int total = 0;
    uint64_t stopTime;
    while (1) {
        while (threadContext->wakeupTimeInCycles != 0) {
            /* Do nothing */
        }
        total += threadContext->args[0] + threadContext->args[1]
                + threadContext->args[2] + threadContext->args[3];
        stopTime = Cycles::rdtscp();
        uint64_t extraRdtsc = Cycles::rdtscp();
        timeTrace(stopTime, "thread created");
        if (*maskAndCount > 100) {
            break;
        }
        threadContext->wakeupTimeInCycles = 1000;
        timeTrace("target reset wakeupTimeInCycles");
        timeTrace("measure time trace cost");
        *maskAndCount = 0;              /* Simulate thread exit. */
        *elapsed = stopTime - threadContext->startTime -
                (extraRdtsc - stopTime);
    }
}

// Simulate the cross-core interactions in Arachne thread creation
// and measure end-to-end time to "create a thread".
double arachneThreadCreate()
{
    int count = 1000000;

    // Allocated cache-line aligned space for the mock maskAndcount
    // and mock context values.
    char memory[2000];
    char* cacheLine = reinterpret_cast<char*>(
            (reinterpret_cast<uint64_t>(memory) + 1023) & ~0x3ff);
    Atomic<uint64_t>* maskAndCount =
            reinterpret_cast<Atomic<uint64_t>*>(cacheLine);
    *maskAndCount = 0;
    ArachneThreadContext* threadContext =
            reinterpret_cast<ArachneThreadContext*>(cacheLine + 512);
    threadContext->wakeupTimeInCycles = 12345;
    volatile uint64_t elapsed = 0;
    uint64_t totalCycles = 0;
    // uint64_t dummy = 0;

    // Get the worker thread running.
    std::thread thread(arachneThreadCreateWorker, maskAndCount, threadContext,
            &elapsed);
    pinThread(core1);

    // Now run the test.
    for (int i = -10; i < count; i++) {
        if (i == 0) {
            // Restart the timing after the test has been running
            // for a while, so everything is warmed up.
            totalCycles = 0;
        }
        elapsed = 0;
        uint64_t startTime = Cycles::rdtscp();
        timeTrace(startTime, "about to read maskAndCount");
        uint64_t maskCopy = *maskAndCount;
        // uint64_t maskCopy = maskAndCount->compareExchange(0, 0);
        if (maskCopy && 8) {
            printf("Bogus maskAndCount: %lu\n", maskAndCount->load());
        }
        // timeTrace("read maskAndCount");
        while (maskAndCount->compareExchange(maskCopy, maskCopy|4)) {
            printf("maskAndCount->compareExchange failed!\n");
        }
        // timeTrace("updated maskAndCount");
        threadContext->args[0] = 1;
        threadContext->args[1] = 2;
        threadContext->args[2] = 3;
        threadContext->args[3] = 4;
        threadContext->startTime = startTime;
        Fence::sfence();
        threadContext->wakeupTimeInCycles = 0;
        // timeTrace("updated wakeupTimeInCycles");
        while (elapsed == 0) {
            /* Wait for "thread creation" */
        }
        totalCycles += elapsed;
    }
    *maskAndCount = 1000;
    threadContext->wakeupTimeInCycles = 0;
    Fence::sfence();
    thread.join();
    unpinThread();
#ifdef PERFTT
    TimeTrace::printToLog();
#endif
    return Cycles::toSeconds(totalCycles)/count;
}

// Measure the cost of Atomic<int>::compareExchange.
double atomicIntCmpX()
{
    int count = 1000000;
    Atomic<int> value(11);
    int test = 11;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
         value.compareExchange(test, test+2);
         test += 2;
    }
    uint64_t stop = Cycles::rdtscp();
    // printf("Final value: %d\n", value.load());
    return Cycles::toSeconds(stop - start)/count;
}
// Measure the cost of Atomic<int>::inc.
double atomicIntInc()
{
    int count = 1000000;
    Atomic<int> value(11);
    int prevValue = 1;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
         prevValue = value.inc(prevValue);
    }
    uint64_t stop = Cycles::rdtscp();
    // printf("Final value: %d\n", prevValue);
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of std::atomic<int>::fetch_add with relaxed memory order.
double cppAtomicIntInc()
{
    int count = 1000000;
    std::atomic<int> value(11);
    int prevValue = 1;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        prevValue = value.fetch_add(prevValue, std::memory_order_relaxed);
    }
    uint64_t stop = Cycles::rdtscp();
    // printf("Final value: %d\n", prevValue);
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of reading an Atomic<int>.
double atomicIntLoad()
{
    int count = 1000000;
    Atomic<int> value(11);
    int total = 0;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
         total += value.load();
    }
    uint64_t stop = Cycles::rdtscp();
    // printf("Total: %d\n", total);
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of Atomic<int>::exchange.
double atomicIntXchg()
{
    int count = 1000000;
    Atomic<int> value(11);
    int total = 0;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
         total += value.exchange(i);
    }
    uint64_t stop = Cycles::rdtscp();
    // printf("Total: %d\n", total);
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of storing a new value in a Atomic<int>.
double atomicIntStore()
{
    int count = 1000000;
    Atomic<int> value(11);
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        value.store(88);
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of acquiring and releasing a std mutex in the
// fast case where the mutex is free.
double bMutexNoBlock()
{
    int count = 1000000;
    std::mutex m;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        m.lock();
        m.unlock();
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

template<int keyLength>
static double bufferAppendCommon()
{
    Buffer b;
    char src[keyLength] = {};
    int count = 1000000;
    uint64_t totalTime = 0;
    for (int i = 0; i < count; i++) {
        uint64_t start = Cycles::rdtscp();
        b.appendCopy(src, keyLength);
        totalTime += Cycles::rdtscp() - start;
        b.reset();
    }
    return Cycles::toSeconds(totalTime)/count;
}

template<int keyLength>
static double bufferAppendExternalCommon()
{
    Buffer b;
    char src[keyLength];
    int count = 1000000;
    uint64_t totalTime = 0;
    for (int i = 0; i < count; i++) {
        uint64_t start = Cycles::rdtscp();
        b.appendExternal(src, keyLength);
        totalTime += Cycles::rdtscp() - start;
        b.reset();
    }
    return Cycles::toSeconds(totalTime)/count;
}

// Measure the cost of appendCopy'ing 1 bytes to a Buffer
double bufferAppendCopy1()
{
    return bufferAppendCommon<1>();
}

// Measure the cost of appendCopy'ing 50 bytes to a Buffer
double bufferAppendCopy50()
{
    return bufferAppendCommon<50>();
}

// Measure the cost of appendCopy'ing 100 bytes to a Buffer
double bufferAppendCopy100()
{
    return bufferAppendCommon<100>();
}

// Measure the cost of appendCopy'ing 250 bytes to a Buffer
double bufferAppendCopy250()
{
    return bufferAppendCommon<250>();
}

// Measure the cost of appendCopy'ing 500 bytes to a Buffer
double bufferAppendCopy500()
{
    return bufferAppendCommon<500>();
}

// Measure the cost of appendExternal'ing 1 bytes to a Buffer
double bufferAppendExternal1()
{
    return bufferAppendExternalCommon<1>();
}

// Measure the cost of appendExternal'ing 50 bytes to a Buffer
double bufferAppendExternal50()
{
    return bufferAppendExternalCommon<50>();
}

// Measure the cost of appendExternal'ing 100 bytes to a Buffer
double bufferAppendExternal100()
{
    return bufferAppendExternalCommon<100>();
}

// Measure the cost of appendExternal'ing 250 bytes to a Buffer
double bufferAppendExternal250()
{
    return bufferAppendExternalCommon<250>();
}

// Measure the cost of appendExternal'ing 500 bytes to a Buffer
double bufferAppendExternal500()
{
    return bufferAppendExternalCommon<500>();
}

// Measure the cost of allocating and deallocating a buffer, plus
// appending (virtually) one block.
double bufferBasic()
{
    int count = 1000000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        Buffer b;
        b.appendExternal("abcdefg", 5);
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

struct dummyBlock {
    int a, b, c, d;
};

// Measure the cost of allocating and deallocating a buffer, plus
// allocating space for one chunk.
double bufferBasicAlloc()
{
    int count = 1000000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        Buffer b;
        b.emplaceAppend<dummyBlock>();
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of allocating and deallocating a buffer, plus
// copying in a small block.
double bufferBasicCopy()
{
    int count = 1000000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        Buffer b;
        b.appendCopy("abcdefg", 6);
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of making a copy of parts of two chunks.
double bufferCopy()
{
    int count = 1000000;
    Buffer b;
    b.appendExternal("abcde", 5);
    b.appendExternal("01234", 5);
    char copy[10];
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        b.copy(2, 6, copy);
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

double bufferConstruct() {
    int count = 1000000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        Buffer b;   // Compiler doesn't seem to optimize this out.
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of allocating new space by extending the
// last chunk.
double bufferExtendChunk()
{
    int count = 100000;
    uint64_t total = 0;
    for (int i = 0; i < count; i++) {
        Buffer b;
        b.emplaceAppend<dummyBlock>();
        uint64_t start = Cycles::rdtscp();
        b.emplaceAppend<dummyBlock>();
        b.emplaceAppend<dummyBlock>();
        b.emplaceAppend<dummyBlock>();
        b.emplaceAppend<dummyBlock>();
        b.emplaceAppend<dummyBlock>();
        b.emplaceAppend<dummyBlock>();
        b.emplaceAppend<dummyBlock>();
        b.emplaceAppend<dummyBlock>();
        b.emplaceAppend<dummyBlock>();
        b.emplaceAppend<dummyBlock>();
        total += Cycles::rdtscp() - start;
        b.reset();
    }
    return Cycles::toSeconds(total)/(count*10);
}
// Measure the cost of reseting an empty Buffer
double bufferReset() {
    Buffer b;
    int count = 1000000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        b.reset();
    }
    uint64_t totalTime = Cycles::rdtscp() - start;
    return Cycles::toSeconds(totalTime)/count;
}

// Measure the cost of retrieving an object from the beginning of a buffer.
double bufferGetStart()
{
    int count = 1000000;
    int value = 11;
    Buffer b;
    b.appendCopy(&value);
    int sum = 0;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        sum += *b.getStart<int>();
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of creating an iterator and iterating over/accessing
// 1 byte in every appendCopy()ed, 100-byte chunk.
template<uint32_t chunks>
double bufferCopyIterator()
{
    Buffer b;
    char data[4096 * chunks];
    for (uint32_t i = 0; i < chunks; i++) {
        b.appendCopy(data + 4096 * i, 100);
    }

    int sum = 0;
    int count = 1000000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        Buffer::Iterator it(&b);
        while (!it.isDone()) {
            for (uint32_t j = 0; j < it.getLength(); j += 100)
                sum += (static_cast<const char*>(it.getData()))[j];
            it.next();
        }
    }
    uint64_t stop = Cycles::rdtscp();
    discard(&sum);

    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of creating an iterator and iterating over/accessing
// 1 byte in every appendExternal()ed, 100-byte chunk.
template<uint32_t chunks>
double bufferExternalIterator()
{
    Buffer b;
    char data[4096 * chunks];
    for (uint32_t i = 0; i < chunks; i++) {
        b.appendExternal(data + 4096 * i, 100);
    }

    int sum = 0;
    int count = 1000000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        Buffer::Iterator it(&b);
        while (!it.isDone()) {
            for (uint32_t j = 0; j < it.getLength(); j += 100)
                sum += (static_cast<const char*>(it.getData()))[j];
            it.next();
        }
    }
    uint64_t stop = Cycles::rdtscp();
    discard(&sum);

    return Cycles::toSeconds(stop - start)/count;
}

double bufferCopyIterator2()
{
    return bufferCopyIterator<2>();
}

double bufferCopyIterator5()
{
    return bufferCopyIterator<5>();
}

double bufferExternalIterator2()
{
    return bufferExternalIterator<2>();
}

double bufferExternalIterator5()
{
    return bufferExternalIterator<5>();
}

// This function runs the second thread for several of the cache
// tests; it just writes a given value to cache it with exclusive
// ownership.
void cacheReadWorker(Atomic<int>* value, volatile int* sync)
{
    pinThread(core2);
    while (1) {
        while (*sync == 0) {
            /* Do nothing */
        }
        if (*sync < 0) {
            break;
        }
        (*value)++;
        Fence::sfence();
        *sync = 0;
    }
}

// Measure the time to first compare-and-swap on the same value twice;
// it is initially owned by a different cache.
double cacheCasThenCas()
{
    int count = 1000000;
    std::vector<uint64_t> samples;
    samples.reserve(count);

    // Make sure the shared value is on a cache line with nothing else.
    int memory[1024];
    Atomic<int>* value = reinterpret_cast<Atomic<int>*>(&memory[512]);
    *value = 0;

    // Used to synchronize writer and reader.
    volatile int sync = 0;

    // Get the worker thread running.
    std::thread thread(cacheReadWorker, value, &sync);
    pinThread(core1);

    // Now run the test.
    int dummy = 0;
    int check = 0;
    for (int i = -10; i < count; i++) {
        sync = 1;
        while (sync != 0) {
            /* Wait for worker thread to update value, which flushes
             * it from our cache. */
        }
        uint64_t start = Cycles::rdtscp();
        if (value->compareExchange(4, 6) != 0) {
            // Put in if statement just to make sure first op completes
            // before second one runs.
            check++;
            dummy += value->compareExchange(1, 2);
        }
        Fence::sfence();
        uint64_t t1 = Cycles::rdtscp();
        uint64_t t2 = Cycles::rdtscp();
        if (i >= 0) {
            samples.push_back((t1 - start) - (t2 - t1));
        }
        // printf("Value %d, cycles %lu\n", *value, t1 - start);
    }
    if (check != count+10) {
        printf("Check is only %d in cacheCasThenCas\n", check);
    }
    sync = -1;
    thread.join();
    unpinThread();
    std::sort(samples.begin(), samples.end());
    return Cycles::toSeconds(samples[count/2]);
}

// Measure the time to read a value cached in a different core
// (the value ends up shared).
double cacheRead()
{
    int count = 1000000;
    std::vector<uint64_t> samples;
    samples.reserve(count);

    // Make sure the shared value is on a cache line with nothing else.
    int memory[1024];
    Atomic<int>* value = reinterpret_cast<Atomic<int>*>(&memory[512]);
    *value = 0;

    // Used to synchronize writer and reader.
    volatile int sync = 0;

    // Get the worker thread running.
    std::thread thread(cacheReadWorker, value, &sync);
    pinThread(core1);

    // Now run the test.
    int dummy;
    for (int i = -10; i < count; i++) {
        sync = 1;
        while (sync != 0) {
            /* Wait for worker thread to update value, which flushes
             * it from our cache. */
        }
        uint64_t start = Cycles::rdtscp();
        dummy += *value;
        Fence::lfence();
        uint64_t t1 = Cycles::rdtscp();
        uint64_t t2 = Cycles::rdtscp();
        if (i >= 0) {
            samples.push_back((t1 - start) - (t2 - t1));
        }
        // printf("Value %d, cycles %lu\n", *value, t1 - start);
    }
    sync = -1;
    thread.join();
    unpinThread();
    std::sort(samples.begin(), samples.end());
    return Cycles::toSeconds(samples[count/2]);
}

// This function modifies a collection of cache lines, forcing each to
// be cached on this core and not another other. Used by cacheReadLines.
void cacheReadLinesWorker(volatile char* firstByte, int numLines, int stride,
        volatile int* sync)
{
    pinThread(core2);
    char value = 1;
    while (1) {
        while (*sync == 0) {
            /* Do nothing */
        }
        if (*sync < 0) {
            break;
        }
        volatile char* p = firstByte;
        for (int i = 0; i < numLines; i++, p += stride) {
            *p = value;
        }
        value++;
        Fence::sfence();
        *sync = 0;
    }
}

// Measure the time to read several cache lines in parallel from data
// cached in another core.
double cacheReadLines(int numLines)
{
    int count = 1000000;
    int stride = 256;
    std::vector<uint64_t> samples;
    samples.reserve(count);

    // Make sure the shared value is on a cache line with nothing else.
    volatile char memory[10000];
    volatile char* firstByte = memory + 100;

    // Used to synchronize writer and reader.
    volatile int sync = 0;

    // Get the worker thread running.
    std::thread thread(cacheReadLinesWorker, firstByte, numLines, stride,
            &sync);
    pinThread(core1);

    // Now run the test.
    int dummy = 0;
    for (int i = -10; i < count; i++) {
        sync = 1;
        while (sync != 0) {
            /* Wait for worker thread to modify all of the cache lines. */
        }
        volatile char* p = firstByte + (numLines-1)*stride;
        uint64_t start = Cycles::rdtscp();
            for ( ; p >= firstByte; p -= stride) {
                dummy += *p;
            }
        Fence::lfence();
        // total += p[0] + p[64] + p[128] + p[192];
        uint64_t t1 = Cycles::rdtscp();
        uint64_t t2 = Cycles::rdtscp();
        if (i >= 0) {
            samples.push_back((t1 - start) - (t2 - t1));
        }
        if ((numLines == 2) && (i < 10)) {
            // printf("Total: %d\n", total);
        }
    }
    sync = -1;
    thread.join();
    unpinThread();
    std::sort(samples.begin(), samples.end());
    return Cycles::toSeconds(samples[count/2]);
}

double cacheRead2Lines() {
    return cacheReadLines(2);
}
double cacheRead4Lines() {
    return cacheReadLines(4);
}
double cacheRead6Lines() {
    return cacheReadLines(6);
}
double cacheRead8Lines() {
    return cacheReadLines(8);
}
double cacheRead10Lines() {
    return cacheReadLines(10);
}
double cacheRead12Lines() {
    return cacheReadLines(12);
}
double cacheRead14Lines() {
    return cacheReadLines(14);
}
double cacheRead16Lines() {
    return cacheReadLines(16);
}
double cacheRead32Lines() {
    return cacheReadLines(32);
}

// Measure the time to read a value cached in a different core using
// compare-and-swap, so the value ends up owned exclusive
// (the value ends up shared).
double cacheReadExcl()
{
    int count = 1000000;
    std::vector<uint64_t> samples;
    samples.reserve(count);

    // Make sure the shared value is on a cache line with nothing else.
    int memory[1024];
    Atomic<int>* value = reinterpret_cast<Atomic<int>*>(&memory[512]);
    *value = 0;

    // Used to synchronize writer and reader.
    volatile int sync = 0;

    // Get the worker thread running.
    std::thread thread(cacheReadWorker, value, &sync);
    pinThread(core1);

    // Now run the test.
    int total;
    for (int i = -10; i < count; i++) {
        sync = 1;
        while (sync != 0) {
            /* Wait for worker thread to update value, which flushes
             * it from our cache. */
        }
        uint64_t start = Cycles::rdtscp();
        total += value->compareExchange(1, 2);
        Fence::lfence();
        uint64_t t1 = Cycles::rdtscp();
        uint64_t t2 = Cycles::rdtscp();
        if (i >= 0) {
            samples.push_back((t1 - start) - (t2 - t1));
        }
        // printf("Value %d, cycles %lu\n", *value, t1 - start);
    }
    sync = -1;
    thread.join();
    unpinThread();
    std::sort(samples.begin(), samples.end());
    return Cycles::toSeconds(samples[count/2]);
}

// Measure the time to first read a value cached in a different core
// (nonexclusive), then execute a compare-and-swap on it.
double cacheReadThenCas()
{
    int count = 1000000;
    std::vector<uint64_t> samples;
    samples.reserve(count);

    // Make sure the shared value is on a cache line with nothing else.
    int memory[1024];
    Atomic<int>* value = reinterpret_cast<Atomic<int>*>(&memory[512]);
    *value = 0;

    // Used to synchronize writer and reader.
    volatile int sync = 0;

    // Get the worker thread running.
    std::thread thread(cacheReadWorker, value, &sync);
    pinThread(core1);

    // Now run the test.
    int dummy = 0;
    int check = 0;
    for (int i = -10; i < count; i++) {
        sync = 1;
        while (sync != 0) {
            /* Wait for worker thread to update value, which flushes
             * it from our cache. */
        }
        uint64_t start = Cycles::rdtscp();
        if (*value >= 0) {
            check++;
            dummy += value->compareExchange(1, 2);
        }
        uint64_t t1 = Cycles::rdtscp();
        uint64_t t2 = Cycles::rdtscp();
        if (i >= 0) {
            samples.push_back((t1 - start) - (t2 - t1));
        }
        // printf("Value %d, cycles %lu\n", *value, t1 - start);
    }
    if (check != count+10) {
        printf("Check is only %d in cacheReadThenCas\n", check);
    }
    sync = -1;
    thread.join();
    unpinThread();
    std::sort(samples.begin(), samples.end());
    return Cycles::toSeconds(samples[count/2]);
}

// This function runs the second thread for cacheTransfer.
void cacheTransferWorker(volatile uint64_t* startTime)
{
    pinThread(core2);
    while (true) {
        while (*startTime > 10) {
            /* The reader is still processing the last update. */
        }
        if (*startTime == 0) {
            return;
        }
        *startTime = Cycles::rdtscp();
    }
}

// Measure the end-to-end latency to transfer a value from one core
// to another using the cache coherency mechanism. Note: as of 4/2017,
// the time for this is more than half the time for pingVariable,
// which doesn't make sense....
double cacheTransfer()
{
    int count = 1000000;
    std::vector<uint64_t> samples;
    samples.reserve(count);

    // Make sure the shared value is on a cache line with nothing else.
    int values[1024];
    volatile uint64_t* startTime =
            reinterpret_cast<uint64_t*>(&values[512]);
    *startTime = 12345;

    std::thread thread(cacheTransferWorker, startTime);
    pinThread(core1);

    // Now run the test.
    for (int i = -10; i < count; i++) {
        *startTime = 1;
        uint64_t start;
        while (1) {
            start = *startTime;
            if (start > 10) {
                break;
            }
            // Retry until the value changes.
        }
        uint64_t t1 = Cycles::rdtscp();
        uint64_t t2 = Cycles::rdtscp();
        if (i >= 0) {
            samples.push_back((t1 - start) - (t2 - t1));
        }
        // printf("Value %d, cycles %lu\n", expected, t1 - startTime);
    }
    *startTime = 0;
    thread.join();
    unpinThread();
    // printf("Average rdtsc time: %.1fns\n",
    //         1e09*Cycles::toSeconds(rdtscTime)/count);
    std::sort(samples.begin(), samples.end());
    return Cycles::toSeconds(samples[count/2]);
}

// This function runs the second thread for cacheTransferAfterMiss.
void cacheTransferAfterMissWorker(volatile uint64_t* startTime,
        volatile int* sync)
{
    pinThread(core2);
    while (1) {
        while (*sync == 0) {
            /* Do nothing */
        }
        if (*sync < 0) {
            break;
        }
        *startTime = Cycles::rdtscp();
        *sync = 0;
    }
}

// Measure the end-to-end latency to transfer a value from one core
// to another using the cache coherency mechanism. This test is different
// from cacheTransfer because the shared value starts off dirty in the
// cache of the receiver, so the initiator must first take a cache miss
// before setting the value.
double cacheTransferAfterMiss()
{
    int count = 1000000;
    std::vector<uint64_t> samples;
    samples.reserve(count);

    // Make sure the shared value is on a cache line with nothing else.
    int values[1024];
    volatile uint64_t* startTime =
            reinterpret_cast<uint64_t*>(&values[512]);
    *startTime = 12345;

    // Used to schedule the execution of the worker.
    volatile int sync = 0;

    std::thread thread(cacheTransferAfterMissWorker, startTime, &sync);
    pinThread(core1);

    // Now run the test.
    for (int i = -10; i < count; i++) {
        *startTime = 1;
        sync = 1;
        uint64_t start;
        while (1) {
            start = *startTime;
            if (start > 10) {
                break;
            }
            // Retry until the value changes.
        }
        uint64_t t1 = Cycles::rdtscp();
        uint64_t t2 = Cycles::rdtscp();
        if (i >= 0) {
            samples.push_back((t1 - start) - (t2 - t1));
        }
        // printf("Value %d, cycles %lu\n", expected, t1 - startTime);
        while (sync != 0) {
            // Wait for worker to get in the correct state.
        }
    }
    sync = -1;
    thread.join();
    unpinThread();
    // printf("Average rdtsc time: %.1fns\n",
    //         1e09*Cycles::toSeconds(rdtscTime)/count);
    std::sort(samples.begin(), samples.end());
    return Cycles::toSeconds(samples[count/2]);
}

// Measure the cost of the exchange method on a C++ atomic_int.
double cppAtomicExchange()
{
    int count = 100000;
    std::atomic_int value(11);
    int other = 22;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
         other = value.exchange(other);
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of the load method on a C++ atomic_int (seems to have
// 2 mfence operations!).
double cppAtomicLoad()
{
    int count = 100000;
    std::atomic_int value(11);
    int total = 0;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        total += value.load();
    }
    uint64_t stop = Cycles::rdtscp();
    // printf("Total: %d\n", total);
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of gcc's  __builtin_clzll, which counts the number of
// leading zeroes in a word.
double clzll()
{
    int count = 1000000;
    uint64_t word = 0xdeaddeadbeef;
    static uint64_t dummy = 11;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        // Without any change to word, compiler throws away instruction under
        // benchmark.
        word <<= 1;
        dummy += __builtin_clzll(word);
    }
    uint64_t stop = Cycles::rdtscp();
    // printf("Final value: %d\n", value.load());
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the minimum cost of Dispatch::poll, when there are no
// Pollers and no Timers.
double dispatchPoll()
{
    int count = 1000000;
    Dispatch dispatch(false);
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        dispatch.poll();
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of a 32-bit divide. Divides don't take a constant
// number of cycles. Values were chosen here semi-randomly to depict a
// fairly expensive scenario. Someone with fancy ALU knowledge could
// probably pick worse values.
double div32()
{
    int count = 1000000;
    Dispatch dispatch(false);
    uint64_t start = Cycles::rdtscp();
    // NB: Expect an x86 processor exception is there's overflow.
    uint32_t numeratorHi = 0xa5a5a5a5U;
    uint32_t numeratorLo = 0x55aa55aaU;
    uint32_t divisor = 0xaa55aa55U;
    uint32_t quotient;
    uint32_t remainder;
    for (int i = 0; i < count; i++) {
        __asm__ __volatile__("div %4" :
                             "=a"(quotient), "=d"(remainder) :
                             "a"(numeratorLo), "d"(numeratorHi), "r"(divisor) :
                             "cc");
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of a 64-bit divide. Divides don't take a constant
// number of cycles. Values were chosen here semi-randomly to depict a
// fairly expensive scenario. Someone with fancy ALU knowledge could
// probably pick worse values.
double div64()
{
    int count = 1000000;
    Dispatch dispatch(false);
    // NB: Expect an x86 processor exception is there's overflow.
    uint64_t start = Cycles::rdtscp();
    uint64_t numeratorHi = 0x5a5a5a5a5a5UL;
    uint64_t numeratorLo = 0x55aa55aa55aa55aaUL;
    uint64_t divisor = 0xaa55aa55aa55aa55UL;
    uint64_t quotient;
    uint64_t remainder;
    for (int i = 0; i < count; i++) {
        __asm__ __volatile__("divq %4" :
                             "=a"(quotient), "=d"(remainder) :
                             "a"(numeratorLo), "d"(numeratorHi), "r"(divisor) :
                             "cc");
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of calling a non-inlined function.
double functionCall()
{
    int count = 1000000;
    uint64_t x = 0;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        x = PerfHelper::plusOne(x);
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of generating a random number between 0 and i
double generateRandomNumber() {
    int count = 100000;
    uint64_t x = 0;

    uint64_t start = Cycles::rdtscp();
    for (int i = 1; i < count + 1; ++i) {
        x += randomNumberGenerator(i);
    }
    uint64_t stop = Cycles::rdtscp();
    discard(&x);

    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of generating a 100-byte random string.
double genRandomString() {
    int count = 100000;
    char buffer[100];
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        Util::genRandomString(buffer, 100);
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of calling ThreadId::get.
double getThreadId()
{
    int count = 1000000;
    int64_t result = 0;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        result += ThreadId::get();
    }
    uint64_t stop = Cycles::rdtscp();
    // printf("Result: %d\n", downCast<int>(result));
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of getting the kernel thread id using a syscall.
double getThreadIdSyscall()
{
    int count = 1000000;
    int64_t result = 0;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        result += syscall(SYS_gettid);
    }
    uint64_t stop = Cycles::rdtscp();
    // printf("Result: %d\n", downCast<int>(result));
    return Cycles::toSeconds(stop - start)/count;
}

double getTimeOfDaySyscall()
{
    int count = 1000000;
    int64_t result = 0;
    uint64_t start = Cycles::rdtscp();

    struct timeval tv;
    for (int i = 0; i < count; i++) {
        gettimeofday(&tv, NULL);
        result += tv.tv_usec;
    }
    uint64_t stop = Cycles::rdtscp();
    // printf("Result: %d\n", downCast<int>(result));
    return Cycles::toSeconds(stop - start)/count;
}

// Measure hash table lookup performance. Prefetching can
// be enabled to measure its effect. This test is a lot
// slower than the others (takes several seconds) due to the
// set up cost, but we really need a large hash table to
// avoid caching.
template<int prefetchBucketAhead = 0>
double hashTableLookup()
{
    uint64_t numBuckets = 16777216;       // 16M * 64 = 1GB
    uint32_t numLookups = 1000000;
    HashTable hashTable(numBuckets);
    HashTable::Candidates candidates;

    // fill with some objects to look up (enough to blow caches)
    for (uint64_t i = 0; i < numLookups; i++) {
        uint64_t* object = new uint64_t(i);
        uint64_t reference = reinterpret_cast<uint64_t>(object);
        Key key(0, object, downCast<uint16_t>(sizeof(*object)));
        hashTable.insert(key.getHash(), reference);
    }

    PerfHelper::flushCache();

    // now look up the objects again
    uint64_t start = Cycles::rdtscp();
    for (uint64_t i = 0; i < numLookups; i++) {
        if (prefetchBucketAhead) {
            if (i + prefetchBucketAhead < numLookups) {
                uint64_t object = i + prefetchBucketAhead;
                Key key(0, &object, downCast<uint16_t>(sizeof(object)));
                hashTable.prefetchBucket(key.getHash());
            }
        }

        Key key(0, &i, downCast<uint16_t>(sizeof(i)));
        hashTable.lookup(key.getHash(), candidates);
        while (!candidates.isDone()) {
            if (*reinterpret_cast<uint64_t*>(candidates.getReference()) == i)
                break;
            candidates.next();
        }
    }
    uint64_t stop = Cycles::rdtscp();

    // clean up
    for (uint64_t i = 0; i < numLookups; i++) {
        Key key(0, &i, downCast<uint16_t>(sizeof(i)));
        hashTable.lookup(key.getHash(), candidates);
        while (!candidates.isDone()) {
            if (*reinterpret_cast<uint64_t*>(candidates.getReference()) == i) {
                delete reinterpret_cast<uint64_t*>(candidates.getReference());
                candidates.remove();
                break;
            }
        }
    }

    return Cycles::toSeconds((stop - start) / numLookups);
}

// Measure the cost of an lfence instruction.
double lfence()
{
    int count = 1000000;
    Dispatch dispatch(false);
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        Fence::lfence();
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of creating and deleting a Dispatch::Lock from within
// the dispatch thread.
double lockInDispThrd()
{
    int count = 1000000;
    Dispatch dispatch(false);
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        Dispatch::Lock lock(&dispatch);
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of creating and deleting a Dispatch::Lock from a thread
// other than the dispatch thread (best case: the dispatch thread has no
// pollers).
void dispatchThread(Dispatch **d, volatile int* flag)
{
    pinThread(core2);
    Dispatch dispatch(true);
    *d = &dispatch;
    dispatch.poll();
    *flag = 1;
    while (*flag == 1)
        dispatch.poll();
}

double lockNonDispThrd()
{
    int count = 100000;
    volatile int flag = 0;
    pinThread(core1);

    // Start a new thread and wait for it to create a dispatcher.
    Dispatch* dispatch;
    std::thread thread(dispatchThread, &dispatch, &flag);
    while (flag == 0) {
        usleep(100);
    }

    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        Dispatch::Lock lock(dispatch);
    }
    uint64_t stop = Cycles::rdtscp();
    flag = 0;
    thread.join();
    unpinThread();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the time to create and delete an entry in a small
// map.
double mapCreate()
{
    // Generate an array of random keys that will be used to lookup
    // entries in the map.
    int numKeys = 20;
    uint64_t keys[numKeys];
    for (int i = 0; i < numKeys; i++) {
        keys[i] = generateRandom();
    }

    int count = 10000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i += 5) {
        std::map<uint64_t, uint64_t> map;
        for (int j = 0; j < numKeys; j++) {
            map[keys[j]] = 1000+j;
        }
        for (int j = 0; j < numKeys; j++) {
            map.erase(keys[j]);
        }
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/(count * numKeys);
}

// Measure the time to lookup a random element in a small map.
double mapLookup()
{
    std::map<uint64_t, uint64_t> map;

    // Generate an array of random keys that will be used to lookup
    // entries in the map.
    int numKeys = 20;
    uint64_t keys[numKeys];
    for (int i = 0; i < numKeys; i++) {
        keys[i] = generateRandom();
        map[keys[i]] = 12345;
    }

    int count = 100000;
    uint64_t sum = 0;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        for (int j = 0; j < numKeys; j++) {
            sum += map[keys[j]];
        }
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/(count*numKeys);
}

// Measure the cost of copying a given number of bytes with memcpy.
double memcpyShared(int cpySize, bool coldSrc = false, bool coldDst = false)
{
    int count = 1000000;
    uint32_t src[count], dst[count];
    int bufSize = 1000000000; // 1GB buffer
    char *buf = static_cast<char*>(malloc(bufSize));

    uint32_t bound = (bufSize - cpySize);
    for (int i = 0; i < count; i++) {
        src[i] = (coldSrc) ? downCast<uint32_t>(generateRandom() % bound) : 0;
        dst[i] = (coldDst) ? downCast<uint32_t>(generateRandom() % bound) : 0;
    }

    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        memcpy((buf + dst[i]),
                (buf + src[i]),
                cpySize);
    }
    uint64_t stop = Cycles::rdtscp();

    free(buf);
    return Cycles::toSeconds(stop - start)/(count);
}

double memcpyCached100()
{
    return memcpyShared(100, false, false);
}

double memcpyCached1000()
{
    return memcpyShared(1000, false, false);
}

double memcpyCached10000()
{
    return memcpyShared(10000, false, false);
}

double memcpyCachedDst100()
{
    return memcpyShared(100, true, false);
}

double memcpyCachedDst1000()
{
    return memcpyShared(1000, true, false);
}

double memcpyCachedDst10000()
{
    return memcpyShared(10000, true, false);
}

double memcpyCachedSrc100()
{
    return memcpyShared(100, false, true);
}

double memcpyCachedSrc1000()
{
    return memcpyShared(1000, false, true);
}

double memcpyCachedSrc10000()
{
    return memcpyShared(10000, false, true);
}

double memcpyCold100()
{
    return memcpyShared(100, true, true);
}

double memcpyCold1000()
{
    return memcpyShared(1000, true, true);
}

double memcpyCold10000()
{
    return memcpyShared(10000, true, true);
}

// Benchmark MurmurHash3 hashing performance on cached data.
// Uses the version generating 128-bit hashes with code
// optimised for 64-bit processors.
template <int keyLength>
double murmur3()
{
    int count = 100000;
    char buf[keyLength];
    uint32_t seed = 11051955;
    uint64_t out[2];

    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++)
        MurmurHash3_x64_128(buf, sizeof(buf), seed, &out);
    uint64_t stop = Cycles::rdtscp();

    return Cycles::toSeconds(stop - start)/count;
}

// Starting with a new ObjectPool, measure the cost of Object
// allocations. The pool may optionally be primed first to
// measure the best-case performance.
template <typename T, bool primeFirst>
double objectPoolAlloc()
{
    int count = 100000;
    T* toDestroy[count];
    ObjectPool<T> pool;

    if (primeFirst) {
        for (int i = 0; i < count; i++) {
            toDestroy[i] = pool.construct();
        }
        for (int i = 0; i < count; i++) {
            pool.destroy(toDestroy[i]);
        }
    }

    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        toDestroy[i] = pool.construct();
    }
    uint64_t stop = Cycles::rdtscp();

    // clean up
    for (int i = 0; i < count; i++) {
        pool.destroy(toDestroy[i]);
    }

    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of the Cylcles::toNanoseconds method.
double perfCyclesToNanoseconds()
{
    int count = 1000000;
    std::atomic_int value(11);
    uint64_t total = 0;
    uint64_t cycles = 994261;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        total += Cycles::toNanoseconds(cycles);
    }
    uint64_t stop = Cycles::rdtscp();
    // printf("Result: %lu\n", total/count);
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of the Cycles::toSeconds method.
double perfCyclesToSeconds()
{
    int count = 1000000;
    std::atomic_int value(11);
    double total = 0;
    uint64_t cycles = 994261;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        total += Cycles::toSeconds(cycles);
    }
    uint64_t stop = Cycles::rdtscp();
    // printf("Result: %.4f\n", total/count);
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of the prefetch instruction.
double perfPrefetch()
{
    uint64_t totalTicks = 0;
    int count = 10;
    char buf[16 * 64];

    for (int i = 0; i < count; i++) {
        PerfHelper::flushCache();
        CycleCounter<uint64_t> ticks(&totalTicks);
        prefetch(&buf[576], 64);
        prefetch(&buf[0],   64);
        prefetch(&buf[512], 64);
        prefetch(&buf[960], 64);
        prefetch(&buf[640], 64);
        prefetch(&buf[896], 64);
        prefetch(&buf[256], 64);
        prefetch(&buf[704], 64);
        prefetch(&buf[320], 64);
        prefetch(&buf[384], 64);
        prefetch(&buf[128], 64);
        prefetch(&buf[448], 64);
        prefetch(&buf[768], 64);
        prefetch(&buf[832], 64);
        prefetch(&buf[64],  64);
        prefetch(&buf[192], 64);
    }
    return Cycles::toSeconds(totalTicks) / count / 16;
}

// This function runs the second thread for pingConditionVar.
void pingConditionVarWorker(std::mutex* mutex,
        std::condition_variable* condition1,
        std::condition_variable* condition2, bool* finished)
{
    pinThread(core2);
    std::unique_lock<std::mutex> guard(*mutex);
    while (!*finished) {
        condition2->notify_one();
        condition1->wait(guard);
    }
}

// Measure the round-trip time for two threads to ping each other using
// a pair of condition variables.
double pingConditionVar()
{
    int count = 100000;
    std::mutex mutex;
    std::condition_variable condition1, condition2;
    bool finished = false;
    Tub<std::unique_lock<std::mutex>> guard;
    guard.construct(mutex);

    // First get the other thread running and warm up the caches.
    std::thread thread(pingConditionVarWorker, &mutex, &condition1,
            &condition2, &finished);
    pinThread(core1);
    condition2.wait(*guard);

    // Now run the test.
    uint64_t start = 0;
    for (int i = -10; i < count; i++) {
        if (i == 0) {
            // Restart the timing after the test has been running
            // for a while, so everything is warmed up.
            start = Cycles::rdtscp();
        }
        condition1.notify_one();
        condition2.wait(*guard);
    }
    uint64_t stop = Cycles::rdtscp();
    finished = true;
    condition1.notify_one();
    guard.destroy();
    thread.join();
    unpinThread();
    return Cycles::toSeconds(stop - start)/count;
}

// This function runs the second thread for pingMutex.
void pingMutexWorker(std::mutex* mutex1, std::mutex* mutex2, bool* finished)
{
    pinThread(core2);
    while (!*finished) {
        mutex1->lock();
        mutex2->unlock();
    }
}

// Measure the round-trip time for two threads to ping each other using
// a pair of mutexes.
double pingMutex()
{
    int count = 100000;
    std::mutex mutex1, mutex2;
    bool finished = false;

    // First get the other thread running and warm up the caches.
    mutex1.lock();
    mutex2.lock();
    std::thread thread(pingMutexWorker, &mutex1, &mutex2, &finished);
    pinThread(core1);

    // Now run the test.
    uint64_t start = 0;
    for (int i = -10; i < count; i++) {
        if (i == 0) {
            // Restart the timing after the test has been running
            // for a while, so everything is warmed up.
            start = Cycles::rdtscp();
        }
        mutex1.unlock();
        mutex2.lock();
    }
    uint64_t stop = Cycles::rdtscp();
    finished = true;
    mutex1.unlock();
    thread.join();
    unpinThread();
    return Cycles::toSeconds(stop - start)/count;
}

// This function runs the second thread for pingVariable.
void pingVariableWorker(volatile int* value)
{
    pinThread(core2);
    int prev = 0;
    while (1) {
        int current = *value;
        if (current != prev) {
            prev = current+1;
            *value = prev;
            if (current < 0) {
                break;
            }
        }
    }
}

// Measure the round-trip time for two threads to ping each other using
// a single variable; each round-trip represents two cache misses (one
// by each thread).
double pingVariable()
{
    int count = 1000000;

    // Arrange for shared value to be on private cache line.
    volatile int values[500];
    volatile int *value = &values[100];
    int prev = 99;

    // First get the other thread running.
    std::thread thread(pingVariableWorker, value);
    pinThread(core1);

    // Now run the test.
    uint64_t start = 0;
    for (int i = -10; i < count; i++) {
        if (i == 0) {
            // Restart the timing after the test has been running
            // for a while, so everything is warmed up.
            start = Cycles::rdtscp();
        }
        while (1) {
            int current = *value;
            if (current != prev) {
                prev = current+1;
                *value = prev;
                break;
            }
        }
    }
    uint64_t stop = Cycles::rdtscp();
    *value = -1;
    Fence::sfence();
    thread.join();
    unpinThread();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of the QueueEstimator::getQueueSize method.
double queueEstimator()
{
    int count = 1000000;
    QueueEstimator estimator(8000);
    uint64_t start = Cycles::rdtscp();
    static uint32_t total = 0;
    for (int i = 0; i < count; i++) {
        estimator.setQueueSize(1000, 100000+i);
        total += estimator.getQueueSize(200000);
    }
    uint64_t stop = Cycles::rdtscp();
    // printf("Result: %u\n", total/count);
    return Cycles::toSeconds(stop - start)/count;
}
/**
  * A random number generator from the Internet that returns 64-bit integers.
  * It is advertised to be fast.
  */
inline uint64_t
randomUint64(void) {
    // This function came from the following site.
    // http://stackoverflow.com/a/1640399/391161
    thread_local uint64_t x = 123456789, y = 362436069, z = 521288629;
    uint64_t t;
    x ^= x << 16;
    x ^= x >> 5;
    x ^= x << 1;

    t = x;
    x = y;
    y = z;
    z = t ^ x ^ y;

    return z;
}

// Measure the cost of generating a random number with the method
// "randomUint64" above.
double randomTest()
{
    int count = 1000000;
    uint64_t start = Cycles::rdtscp();
    uint64_t total = 0;
    for (int i = 0; i < count; i++) {
        total += randomUint64();
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of reading the fine-grain cycle counter using rdtsc.
double rdtscTest()
{
    int count = 1000000;
    uint64_t start = Cycles::rdtscp();
    uint64_t total = 0;
    for (int i = 0; i < count; i++) {
        total += Cycles::rdtsc();
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of reading the fine-grain cycle counter using rdtscp.
double rdtscpTest()
{
    int count = 1000000;
    uint64_t start = Cycles::rdtscp();
    uint64_t total = 0;
    for (int i = 0; i < count; i++) {
        total += Cycles::rdtscp();
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Sorting functor for #segmentEntrySort.
struct SegmentEntryLessThan {
  public:
    bool
    operator()(const std::pair<uint64_t, uint32_t> a,
               const std::pair<uint64_t, uint32_t> b)
    {
        return a.second < b.second;
    }
};

// Measure the time it takes to walk a Segment full of small objects,
// populate a vector with their entries, and sort it by age.
double segmentEntrySort()
{
    Segment s;
    const int avgObjectSize = 100;

    char data[2 * avgObjectSize];
    vector<std::pair<uint64_t, uint32_t>> entries;

    int count;
    for (count = 0; ; count++) {
        uint32_t timestamp = static_cast<uint32_t>(generateRandom());
        uint32_t size = timestamp % (2 * avgObjectSize);

        Key key(0, &count, sizeof(count));

        Buffer dataBuffer;
        Object object(key, data, size, 0, timestamp, dataBuffer);

        Buffer buffer;
        object.assembleForLog(buffer);
        if (!s.append(LOG_ENTRY_TYPE_OBJ, buffer))
            break;
    }

    // doesn't appear to help
    entries.reserve(count);

    uint64_t start = Cycles::rdtscp();

    // appears to take about 1/8th the time
    for (SegmentIterator i(s); !i.isDone(); i.next()) {
        if (i.getType() == LOG_ENTRY_TYPE_OBJ) {
            uint64_t handle = 0;    // fake pointer to object

            Buffer buffer;
            i.appendToBuffer(buffer);
            Object object(buffer);
            uint32_t timestamp = object.getTimestamp();
            entries.push_back(std::pair<uint64_t, uint32_t>(handle, timestamp));
        }
    }

    // the rest is, unsurprisingly, here
    std::sort(entries.begin(), entries.end(), SegmentEntryLessThan());

    uint64_t stop = Cycles::rdtscp();

    return Cycles::toSeconds(stop - start);
}

// Measure the time it takes to iterate over the entries in
// a single Segment.
template<uint32_t minObjectBytes, uint32_t maxObjectBytes>
double segmentIterator()
{
    uint64_t numObjects = 0;
    uint64_t nextKeyVal = 0;

    // build a segment
    Segment segment;
    while (1) {
        uint32_t size = minObjectBytes;
        if (minObjectBytes != maxObjectBytes) {
            uint64_t rnd = generateRandom();
            size += downCast<uint32_t>(rnd % (maxObjectBytes - minObjectBytes));
        }
        string stringKey = format("%lu", nextKeyVal++);
        Key key(0, stringKey.c_str(), downCast<uint16_t>(stringKey.length()));

        char data[size];
        Buffer dataBuffer;
        Object object(key, data, size, 0, 0, dataBuffer);

        Buffer buffer;
        object.assembleForLog(buffer);
        if (!segment.append(LOG_ENTRY_TYPE_OBJ, buffer))
            break;
        numObjects++;
    }
    segment.close();

    // scan through the segment
    uint64_t totalBytes = 0;
    uint64_t totalObjects = 0;
    CycleCounter<uint64_t> counter;
    SegmentIterator si(segment);
    while (!si.isDone()) {
        totalBytes += si.getLength();
        totalObjects++;
        si.next();
    }
    double time = Cycles::toSeconds(counter.stop());

    return time;
}

// Measure the cost of cpuid
double serialize() {
    int count = 1000000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        Util::serialize();
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of incrementing and decrementing the reference count in
// a SessionRef.
double sessionRefCount()
{
    int count = 1000000;
    Transport::SessionRef ref1(new Transport::Session("test"));
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        Transport::SessionRef ref2 = ref1;
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of an sfence instruction.
double sfence()
{
    int count = 1000000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        Fence::sfence();
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of acquiring and releasing a SpinLock (assuming the
// lock is initially free).
double spinLock()
{
    int count = 1000000;
    SpinLock lock("Perf");
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        lock.lock();
        lock.unlock();
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Helper for spawnThread. This is the main function that the thread executes
// (intentionally empty).
void spawnThreadHelper()
{
}

// Measure the cost of start and joining with a thread.
double spawnThread()
{
    int count = 10000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        std::thread thread(&spawnThreadHelper);
        thread.join();
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of starting and stopping a Dispatch::Timer.
double startStopTimer()
{
    int count = 1000000;
    Dispatch dispatch(false);
    Dispatch::Timer timer(&dispatch);
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        timer.start(12345U);
        timer.stop();
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of throwing and catching an int. This uses an integer as
// the value thrown, which is presumably as fast as possible.
double throwInt()
{
    int count = 10000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        try {
            throw 0;
        } catch (int) { // NOLINT
            // pass
        }
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of throwing and catching an int from a function call.
double throwIntNL()
{
    int count = 10000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        try {
            PerfHelper::throwInt();
        } catch (int) { // NOLINT
            // pass
        }
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of throwing and catching an Exception. This uses an actual
// exception as the value thrown, which may be slower than throwInt.
double throwException()
{
    int count = 10000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        try {
            throw ObjectDoesntExistException(HERE);
        } catch (const ObjectDoesntExistException&) {
            // pass
        }
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of throwing and catching an Exception from a function call.
double throwExceptionNL()
{
    int count = 10000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        try {
            PerfHelper::throwObjectDoesntExistException();
        } catch (const ObjectDoesntExistException&) {
            // pass
        }
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of throwing and catching an Exception using
// ClientException::throwException.
double throwSwitch()
{
    int count = 10000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        try {
            ClientException::throwException(HERE, STATUS_OBJECT_DOESNT_EXIST);
        } catch (const ObjectDoesntExistException&) {
            // pass
        }
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of recording a TimeTrace entry.
double timeTrace()
{
    int count = 100000;
    TimeTrace::Buffer* trace = TimeTrace::getBuffer();
    trace->record("warmup record");
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        trace->record("sample TimeTrace record");
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the time to create and delete an entry in a small
// unordered_map.
double unorderedMapCreate()
{
    std::unordered_map<uint64_t, uint64_t> map;

    int count = 100000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i += 5) {
        map[i] = 100;
        map[i+1] = 200;
        map[i+2] = 300;
        map[i+3] = 400;
        map[i+4] = 500;
        map.erase(i);
        map.erase(i+1);
        map.erase(i+2);
        map.erase(i+3);
        map.erase(i+4);
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the time to lookup a random element in a small unordered_map.
double unorderedMapLookup()
{
    std::unordered_map<uint64_t, uint64_t> map;

    // Generate an array of random keys that will be used to lookup
    // entries in the map.
    int numKeys = 10;
    uint64_t keys[numKeys];
    for (int i = 0; i < numKeys; i++) {
        keys[i] = generateRandom();
        map[keys[i]] = 12345;
    }

    int count = 100000;
    uint64_t sum = 0;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        for (int j = 0; j < numKeys; j++) {
            sum += map[keys[j]];
        }
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/(count*numKeys);
}

#if __cplusplus >= 201402L
// Measure the time to create and delete an entry in a small ska::flat_hash_map.
double skaFlatHashMapCreate()
{
    ska::flat_hash_map<uint64_t, uint64_t> map;

    int count = 100000;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i += 5) {
        map[i] = 100;
        map[i+1] = 200;
        map[i+2] = 300;
        map[i+3] = 400;
        map[i+4] = 500;
        map.erase(i);
        map.erase(i+1);
        map.erase(i+2);
        map.erase(i+3);
        map.erase(i+4);
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the time to lookup a random element in a small ska::flat_hash_map.
double skaFlatHashMapLookup()
{
    ska::flat_hash_map<uint64_t, uint64_t> map;

    // Generate an array of random keys that will be used to lookup
    // entries in the map.
    int numKeys = 10;
    uint64_t keys[numKeys];
    for (int i = 0; i < numKeys; i++) {
        keys[i] = generateRandom();
        map[keys[i]] = 12345;
    }

    int count = 100000;
    uint64_t sum = 0;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        for (int j = 0; j < numKeys; j++) {
            sum += map[keys[j]];
        }
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/(count*numKeys);
}
#endif

// Measure the cost of pushing a new element on a std::vector, copying
// from the end to an internal element, and popping the end element.
double vectorPushPop()
{
    int count = 100000;
    std::vector<int> vector;
    vector.push_back(1);
    vector.push_back(2);
    vector.push_back(3);
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        vector.push_back(i);
        vector.push_back(i+1);
        vector.push_back(i+2);
        vector[2] = vector.back();
        vector.pop_back();
        vector[0] = vector.back();
        vector.pop_back();
        vector[1] = vector.back();
        vector.pop_back();
    }
    uint64_t stop = Cycles::rdtscp();
    return Cycles::toSeconds(stop - start)/(count*3);
}

// The following struct and table define each performance test in terms of
// a string name and a function that implements the test.
struct TestInfo {
    const char* name;             // Name of the performance test; this is
                                  // what gets typed on the command line to
                                  // run the test.
    double (*func)();             // Function that implements the test;
                                  // returns the time (in seconds) for each
                                  // iteration of that test.
    const char *description;      // Short description of this test (not more
                                  // than about 40 characters, so the entire
                                  // test output fits on a single line).
};
TestInfo tests[] = {
    {"arachneThreadCreate", arachneThreadCreate,
     "Simulate Arachne thread creation"},
    {"atomicIntCmpX", atomicIntCmpX,
     "Atomic<int>::compareExchange"},
    {"atomicIntInc", atomicIntInc,
     "Atomic<int>::inc"},
    {"atomicIntInc", cppAtomicIntInc,
     "std::atomic<int>::fetch_add"},
    {"atomicIntLoad", atomicIntLoad,
     "Atomic<int>::load"},
    {"atomicIntStore", atomicIntStore,
     "Atomic<int>::store"},
    {"atomicIntXchg", atomicIntXchg,
     "Atomic<int>::exchange"},
    {"bMutexNoBlock", bMutexNoBlock,
     "std::mutex lock/unlock (no blocking)"},
    {"bufferAppendCopy1", bufferAppendCopy1,
     "appendCopy 1 byte to a buffer"},
    {"bufferAppendCopy50", bufferAppendCopy50,
     "appendCopy 50 bytes to a buffer"},
    {"bufferAppendCopy100", bufferAppendCopy100,
     "appendCopy 100 bytes to a buffer"},
    {"bufferAppendCopy250", bufferAppendCopy250,
     "appendCopy 250 bytes to a buffer"},
    {"bufferAppendCopy500", bufferAppendCopy500,
     "appendCopy 500 bytes to a buffer"},
    {"bufferAppendExternal1", bufferAppendExternal1,
     "appendExternal 1 byte to a buffer"},
    {"bufferAppendExternal50", bufferAppendExternal50,
     "appendExternal 50 bytes to a buffer"},
    {"bufferAppendExternal100", bufferAppendExternal100,
     "appendExternal 100 bytes to a buffer"},
    {"bufferAppendExternal250", bufferAppendExternal250,
     "appendExternal 250 bytes to a buffer"},
    {"bufferAppendExternal500", bufferAppendExternal500,
     "appendExternal 500 bytes to a buffer"},
    {"bufferBasic", bufferBasic,
     "buffer create, add one chunk, delete"},
    {"bufferBasicAlloc", bufferBasicAlloc,
     "buffer create, alloc block in chunk, delete"},
    {"bufferBasicCopy", bufferBasicCopy,
     "buffer create, copy small block, delete"},
    {"bufferCopy", bufferCopy,
     "copy out 2 small chunks from buffer"},
    {"bufferExtendChunk", bufferExtendChunk,
     "buffer add onto existing chunk"},
    {"bufferGetStart", bufferGetStart,
     "Buffer::getStart"},
    {"bufferConstruct", bufferConstruct,
     "buffer stack allocation"},
    {"bufferReset", bufferReset,
     "Buffer::reset"},
    {"bufferCopyIterator2", bufferCopyIterator2,
     "buffer iterate over 2 copied chunks, accessing 1 byte each"},
    {"bufferCopyIterator5", bufferCopyIterator5,
     "buffer iterate over 5 copied chunks, accessing 1 byte each"},
    {"bufferExternalIterator2", bufferExternalIterator2,
     "buffer iterate over 2 external chunks, accessing 1 byte each"},
    {"bufferExternalIterator5", bufferExternalIterator5,
     "buffer iterate over 5 external chunks, accessing 1 byte each"},
    {"cacheCasThenCas", cacheCasThenCas,
     "compare-swap twice on value from another cache"},
    {"cacheRead", cacheRead,
     "read value from another core's cache"},
    {"cacheRead2Lines", cacheRead2Lines,
     "read 2 cache lines concurrently from another core's cache"},
    {"cacheRead4Lines", cacheRead4Lines,
     "read 4 cache lines concurrently from another core's cache"},
    {"cacheRead6Lines", cacheRead6Lines,
     "read 6 cache lines concurrently from another core's cache"},
    {"cacheRead8Lines", cacheRead8Lines,
     "read 8 cache lines concurrently from another core's cache"},
    {"cacheRead12Lines", cacheRead12Lines,
     "read 12 cache lines concurrently from another core's cache"},
    {"cacheRead16Lines", cacheRead16Lines,
     "read 16 cache lines concurrently from another core's cache"},
    {"cacheRead32Lines", cacheRead32Lines,
     "read 32 cache lines concurrently from another core's cache"},
    {"cacheReadExcl", cacheReadExcl,
     "read value from another cache, make exclusive"},
    {"cacheReadThenCas", cacheReadThenCas,
     "read value, then make exclusive with compare-swap"},
    {"cacheTransfer", cacheTransfer,
     "pass value from one core to another (initially cached)"},
    {"cacheTransferAfterMiss", cacheTransferAfterMiss,
     "pass value from one core to another (not cached on sender)"},
    {"cppAtomicExchg", cppAtomicExchange,
     "Exchange method on a C++ atomic_int"},
    {"cppAtomicLoad", cppAtomicLoad,
     "Read a C++ atomic_int"},
    {"cyclesToSeconds", perfCyclesToSeconds,
     "Convert a rdtsc result to (double) seconds"},
    {"cyclesToNanos", perfCyclesToNanoseconds,
     "Convert a rdtsc result to (uint64_t) nanoseconds"},
    {"leadingZeroes", clzll,
    "Find the number of leading 0-bits"},
    {"dispatchPoll", dispatchPoll,
     "Dispatch::poll (no timers or pollers)"},
    {"div32", div32,
     "32-bit integer division instruction"},
    {"div64", div64,
     "64-bit integer division instruction"},
    {"functionCall", functionCall,
     "Call a function that has not been inlined"},
    {"generateRandomNumber", generateRandomNumber,
     "Call to randomNumberGenerator(x)"},
    {"genRandomString", genRandomString,
     "Generate a random 100-byte value"},
    {"getThreadId", getThreadId,
     "Retrieve thread id via ThreadId::get"},
    {"getThreadIdSyscall", getThreadIdSyscall,
     "Retrieve kernel thread id using syscall"},
    {"getTimeOfDaySyscall", getTimeOfDaySyscall,
     "Retrieve time of day using syscall"},
    {"hashTableLookup", hashTableLookup,
     "Key lookup in a 1GB HashTable"},
    {"hashTableLookupPf", hashTableLookup<20>,
     "Key lookup in a 1GB HashTable with prefetching"},
    {"lfence", lfence,
     "Lfence instruction"},
    {"lockInDispThrd", lockInDispThrd,
     "Acquire/release Dispatch::Lock (in dispatch thread)"},
    {"lockNonDispThrd", lockNonDispThrd,
     "Acquire/release Dispatch::Lock (non-dispatch thread)"},
    {"mapCreate", mapCreate,
     "Create+delete entry in std::map"},
    {"mapLookup", mapLookup,
     "Lookup in std::map<uint64_t, uint64_t>"},
    {"memcpyCached100", memcpyCached100,
     "memcpy 100 bytes with hot/fixed dst and src"},
    {"memcpyCached1000", memcpyCached1000,
     "memcpy 1000 bytes with hot/fixed dst and src"},
    {"memcpyCached10000", memcpyCached10000,
     "memcpy 10000 bytes with hot/fixed dst and src"},
    {"memcpyCachedDst100", memcpyCachedDst100,
     "memcpy 100 bytes with hot/fixed dst and cold src"},
    {"memcpyCachedDst1000", memcpyCachedDst1000,
     "memcpy 1000 bytes with hot/fixed dst and cold src"},
    {"memcpyCachedDst10000", memcpyCachedDst10000,
     "memcpy 10000 bytes with hot/fixed dst and cold src"},
    {"memcpyCachedSrc100", memcpyCachedSrc100,
     "memcpy 100 bytes with hot/fixed src and cold dst"},
    {"memcpyCachedSrc1000", memcpyCachedSrc1000,
     "memcpy 1000 bytes with hot/fixed src and cold dst"},
    {"memcpyCachedSrc10000", memcpyCachedSrc10000,
     "memcpy 10000 bytes with hot/fixed src and cold dst"},
    {"memcpyCold100", memcpyCold100,
     "memcpy 100 bytes with cold dst and src"},
    {"memcpyCold1000", memcpyCold1000,
     "memcpy 1000 bytes with cold dst and src"},
    {"memcpyCold10000", memcpyCold10000,
     "memcpy 10000 bytes with cold dst and src"},
    {"murmur3", murmur3<1>,
     "128-bit MurmurHash3 (64-bit optimised) on 1 byte of data"},
    {"murmur3", murmur3<256>,
     "128-bit MurmurHash3 hash (64-bit optimised) on 256 bytes of data"},
    {"objectPoolAlloc", objectPoolAlloc<int, false>,
     "Cost of new allocations from an ObjectPool (no destroys)"},
    {"objectPoolRealloc", objectPoolAlloc<int, true>,
     "Cost of ObjectPool allocation after destroying an object"},
    {"pingConditionVar", pingConditionVar,
     "Round-trip ping with std::condition_variable"},
    {"pingMutex", pingMutex,
     "Round-trip ping with 2 std::mutexes"},
    {"pingVariable", pingVariable,
     "Round-trip ping by polling variables"},
    {"prefetch", perfPrefetch,
     "Prefetch instruction"},
    {"queueEstimator", queueEstimator,
     "Recompute # bytes outstanding in queue"},
    {"random", randomTest,
     "Generate 64-bit random number (Arachne version)"},
    {"rdtsc", rdtscTest,
     "Read the fine-grain cycle counter using rdtsc"},
    {"rdtscp", rdtscpTest,
     "Read the fine-grain cycle counter using rdtscp"},
    {"segmentEntrySort", segmentEntrySort,
     "Sort a Segment full of avg. 100-byte Objects by age"},
    {"segmentIterator", segmentIterator<50, 150>,
     "Iterate a Segment full of avg. 100-byte Objects"},
    {"sessionRefCount", sessionRefCount,
     "Create/delete SessionRef"},
    {"serialize", serialize,
     "cpuid instruction for serialize"},
    {"sfence", sfence,
     "Sfence instruction"},
    {"spinLock", spinLock,
     "Acquire/release SpinLock"},
    {"startStopTimer", startStopTimer,
     "Start and stop a Dispatch::Timer"},
    {"spawnThread", spawnThread,
     "Start and stop a thread"},
    {"throwInt", throwInt,
     "Throw an int"},
    {"throwIntNL", throwIntNL,
     "Throw an int in a function call"},
    {"throwException", throwException,
     "Throw an Exception"},
    {"throwExceptionNL", throwExceptionNL,
     "Throw an Exception in a function call"},
    {"throwSwitch", throwSwitch,
     "Throw an Exception using ClientException::throwException"},
    {"timeTrace", timeTrace,
     "Record an event using TimeTrace"},
    {"unorderedMapCreate", unorderedMapCreate,
     "Create+delete entry in unordered_map"},
    {"unorderedMapLookup", unorderedMapLookup,
     "Lookup in std::unordered_map<uint64_t, uint64_t>"},
#if __cplusplus >= 201402L
    {"skaFlatHashMapCreate", skaFlatHashMapCreate,
     "Create+delete entry in ska::flat_hash_map"},
    {"skaFlatHashMapLookup", skaFlatHashMapLookup,
     "Lookup in ska::flat_hash_map<uint64_t, uint64_t>"},
#endif
    {"vectorPushPop", vectorPushPop,
     "Push and pop a std::vector"},
};

/**
 * Runs a particular test and prints a one-line result message.
 *
 * \param info
 *      Describes the test to run.
 */
void runTest(TestInfo& info)
{
    double secs = info.func();
    int width = printf("%-23s ", info.name);
    if (secs < 1.0e-06) {
        width += printf("%8.2fns", 1e09*secs);
    } else if (secs < 1.0e-03) {
        width += printf("%8.2fus", 1e06*secs);
    } else if (secs < 1.0) {
        width += printf("%8.2fms", 1e03*secs);
    } else {
        width += printf("%8.2fs", secs);
    }
    printf("%*s %s\n", 26-width, "", info.description);
}

int
main(int argc, char *argv[])
{
    // Parse command-line options.
    namespace po = boost::program_options;
    vector<string> testNames;
    po::options_description optionsDesc(
            "Usage: Perf [options] testName testName ...\n\n"
            "Runs one or more micro-benchmarks and outputs performance "
            "information.\n\n"
            "Allowed options");
    optionsDesc.add_options()
        ("help", "Print this help message and exit")
        ("secondCore", po::value<int>(&core2)->default_value(3),
                "Second core to use for tests involving two cores (first "
                "core is always 2)")
        ("testName", po::value<vector<string>>(&testNames),
                "A test is run if its name contains any of the testName"
                "arguments as a substring");

    po::positional_options_description posDesc;
    posDesc.add("testName", -1);
    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).
          options(optionsDesc).positional(posDesc).run(), vm);
    po::notify(vm);
    if (vm.count("help")) {
        std::cout << optionsDesc << "\n";
        exit(0);
    }

    Logger::get().setLogLevels("NOTICE");
    CPU_ZERO(&savedAffinities);
    sched_getaffinity(0, sizeof(savedAffinities), &savedAffinities);
    if (argc == 1) {
        // No test names specified; run all tests.
        foreach (TestInfo& info, tests) {
            runTest(info);
        }
    } else {
        // Run only the tests whose names contain at least one of the
        // command-line arguments as a substring.
        bool foundTest = false;
        foreach (TestInfo& info, tests) {
            for (size_t i = 0; i < testNames.size(); i++) {
                if (strstr(info.name, testNames[i].c_str()) !=  NULL) {
                    foundTest = true;
                    runTest(info);
                    break;
                }
            }
        }
        if (!foundTest) {
            printf("No tests matched given arguments\n");
        }
    }
    Logger::get().sync();
}
