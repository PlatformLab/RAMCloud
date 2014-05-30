/* Copyright (c) 2011-2014 Stanford University
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
//     Perf test1 test2 ...
//
// test1 and test2 are the names of individual performance measurements to
// run.  If no test names are provided then all of the performance tests
// are run.
//
// To add a new test:
// * Write a function that implements the test.  Use existing test functions
//   as a guideline, and be sure to generate output in the same form as
//   other tests.
// * Create a new entry for the test in the #tests table.
#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 5
#include <atomic>
#else
#include <cstdatomic>
#endif
#include <vector>

#include "Common.h"
#include "Atomic.h"
#include "Cycles.h"
#include "CycleCounter.h"
#include "Dispatch.h"
#include "Fence.h"
#include "Memory.h"
#include "MurmurHash3.h"
#include "Object.h"
#include "ObjectPool.h"
#include "Segment.h"
#include "SegmentIterator.h"
#include "SpinLock.h"
#include "ClientException.h"
#include "PerfHelper.h"
#include "KeyUtil.h"

using namespace RAMCloud;

/**
 * Ask the operating system to pin the current thread to a given CPU.
 *
 * \param cpu
 *      Indicates the desired CPU and hyperthread; low order 2 bits
 *      specify CPU, next bit specifies hyperthread.
 */
void bindThreadToCpu(int cpu)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    sched_setaffinity((pid_t)syscall(SYS_gettid), sizeof(set), &set);
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

// Measure the cost of Atomic<int>::compareExchange.
double atomicIntCmpX()
{
    int count = 1000000;
    Atomic<int> value(11);
    int test = 11;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
         value.compareExchange(test, test+2);
         test += 2;
    }
    uint64_t stop = Cycles::rdtsc();
    // printf("Final value: %d\n", value.load());
    return Cycles::toSeconds(stop - start)/count;
}
// Measure the cost of Atomic<int>::inc.
double atomicIntInc()
{
    int count = 1000000;
    Atomic<int> value(11);
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
         value.inc();
    }
    uint64_t stop = Cycles::rdtsc();
    // printf("Final value: %d\n", value.load());
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of reading an Atomic<int>.
double atomicIntLoad()
{
    int count = 1000000;
    Atomic<int> value(11);
    int total = 0;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
         total += value.load();
    }
    uint64_t stop = Cycles::rdtsc();
    // printf("Total: %d\n", total);
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of Atomic<int>::exchange.
double atomicIntXchg()
{
    int count = 1000000;
    Atomic<int> value(11);
    int total = 0;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
         total += value.exchange(i);
    }
    uint64_t stop = Cycles::rdtsc();
    // printf("Total: %d\n", total);
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of storing a new value in a Atomic<int>.
double atomicIntStore()
{
    int count = 1000000;
    Atomic<int> value(11);
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        value.store(88);
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of acquiring and releasing a std mutex in the
// fast case where the mutex is free.
double bMutexNoBlock()
{
    int count = 1000000;
    std::mutex m;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        m.lock();
        m.unlock();
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of allocating and deallocating a buffer, plus
// appending (virtually) one block.
double bufferBasic()
{
    int count = 1000000;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        Buffer b;
        b.append("abcdefg", 5);
    }
    uint64_t stop = Cycles::rdtsc();
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
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        Buffer b;
        b.emplaceAppend<dummyBlock>();
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of allocating and deallocating a buffer, plus
// copying in a small block.
double bufferBasicCopy()
{
    int count = 1000000;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        Buffer b;
        b.appendCopy("abcdefg", 6);
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of making a copy of parts of two chunks.
double bufferCopy()
{
    int count = 1000000;
    Buffer b;
    b.append("abcde", 5);
    b.append("01234", 5);
    char copy[10];
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        b.copy(2, 6, copy);
    }
    uint64_t stop = Cycles::rdtsc();
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
        uint64_t start = Cycles::rdtsc();
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
        total += Cycles::rdtsc() - start;
        b.reset();
    }
    return Cycles::toSeconds(total)/(count*10);
}

// Measure the cost of retrieving an object from the beginning of a buffer.
double bufferGetStart()
{
    int count = 1000000;
    int value = 11;
    Buffer b;
    b.appendCopy(&value);
    int sum = 0;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        sum += *b.getStart<int>();
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of creating an iterator and iterating over 10
// chunks in a buffer.
double bufferIterator()
{
    Buffer b;
    const char* p = "abcdefghijklmnopqrstuvwxyz";
    for (int i = 0; i < 5; i++) {
        b.append(p+i, 5);
    }
    int count = 100000;
    int sum = 0;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        Buffer::Iterator it(&b);
        while (!it.isDone()) {
            sum += (static_cast<const char*>(it.getData()))[it.getLength()-1];
            it.next();
        }
    }
    uint64_t stop = Cycles::rdtsc();
    discard(&sum);
    return Cycles::toSeconds(stop - start)/count;
}

// Implements the condPingPong test.
class CondPingPong {
  public:
    CondPingPong()
        : mutex()
        , cond()
        , prod(0)
        , cons(0)
        , count(10000)
    {
    }

    double run() {
        std::thread thread(&CondPingPong::consumer, this);
        uint64_t start = Cycles::rdtsc();
        producer();
        uint64_t stop = Cycles::rdtsc();
        thread.join();
        return Cycles::toSeconds(stop - start)/count;
    }

    void producer() {
        std::unique_lock<std::mutex> lockGuard(mutex);
        while (cons < count) {
            while (cons < prod)
                cond.wait(lockGuard);
            ++prod;
            cond.notify_all();
        }
    }

    void consumer() {
        std::unique_lock<std::mutex> lockGuard(mutex);
        while (cons < count) {
            while (cons == prod)
                cond.wait(lockGuard);
            ++cons;
            cond.notify_all();
        }
    }

  private:
    std::mutex mutex;
    std::condition_variable cond;
    int prod;
    int cons;
    const int count;
    DISALLOW_COPY_AND_ASSIGN(CondPingPong);
};

// Measure the cost of coordinating between threads using a condition variable.
double condPingPong()
{
    return CondPingPong().run();
}

// Measure the cost of the exchange method on a C++ atomic_int.
double cppAtomicExchange()
{
    int count = 100000;
    std::atomic_int value(11);
    int other = 22;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
         other = value.exchange(other);
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of the load method on a C++ atomic_int (seems to have
// 2 mfence operations!).
double cppAtomicLoad()
{
    int count = 100000;
    std::atomic_int value(11);
    int total = 0;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        total += value.load();
    }
    uint64_t stop = Cycles::rdtsc();
    // printf("Total: %d\n", total);
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the minimum cost of Dispatch::poll, when there are no
// Pollers and no Timers.
double dispatchPoll()
{
    int count = 1000000;
    Dispatch dispatch(false);
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        dispatch.poll();
    }
    uint64_t stop = Cycles::rdtsc();
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
    uint64_t start = Cycles::rdtsc();
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
    uint64_t stop = Cycles::rdtsc();
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
    uint64_t start = Cycles::rdtsc();
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
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of calling a non-inlined function.
double functionCall()
{
    int count = 1000000;
    uint64_t x = 0;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        x = PerfHelper::plusOne(x);
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of calling ThreadId::get.
double getThreadId()
{
    int count = 1000000;
    int64_t result = 0;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        result += ThreadId::get();
    }
    uint64_t stop = Cycles::rdtsc();
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
    uint64_t start = Cycles::rdtsc();
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
    uint64_t stop = Cycles::rdtsc();

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
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        Fence::lfence();
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of creating and deleting a Dispatch::Lock from within
// the dispatch thread.
double lockInDispThrd()
{
    int count = 1000000;
    Dispatch dispatch(false);
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        Dispatch::Lock lock(&dispatch);
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of creating and deleting a Dispatch::Lock from a thread
// other than the dispatch thread (best case: the dispatch thread has no
// pollers).
void dispatchThread(Dispatch **d, volatile int* flag)
{
    bindThreadToCpu(2);
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

    // Start a new thread and wait for it to create a dispatcher.
    Dispatch* dispatch;
    std::thread thread(dispatchThread, &dispatch, &flag);
    while (flag == 0) {
        usleep(100);
    }

    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        Dispatch::Lock lock(dispatch);
    }
    uint64_t stop = Cycles::rdtsc();
    flag = 0;
    thread.join();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of copying a given number of bytes with memcpy.
double memcpyShared(size_t size)
{
    int count = 1000000;
    char src[size], dst[size];
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        memcpy(dst, src, size);
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

double memcpy100()
{
    return memcpyShared(100);
}

double memcpy1000()
{
    return memcpyShared(1000);
}

double memcpy10000()
{
    return memcpyShared(10000);
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

    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++)
        MurmurHash3_x64_128(buf, sizeof(buf), seed, &out);
    uint64_t stop = Cycles::rdtsc();

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

    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        toDestroy[i] = pool.construct();
    }
    uint64_t stop = Cycles::rdtsc();

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
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        total += Cycles::toNanoseconds(cycles);
    }
    uint64_t stop = Cycles::rdtsc();
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
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        total += Cycles::toSeconds(cycles);
    }
    uint64_t stop = Cycles::rdtsc();
    // printf("Result: %.4f\n", total/count);
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of the prefetch instruction.
double prefetch()
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

// Measure the cost of reading the fine-grain cycle counter.
double rdtscTest()
{
    int count = 1000000;
    uint64_t start = Cycles::rdtsc();
    uint64_t total = 0;
    for (int i = 0; i < count; i++) {
        total += Cycles::rdtsc();
    }
    uint64_t stop = Cycles::rdtsc();
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

    uint64_t start = Cycles::rdtsc();

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

    uint64_t stop = Cycles::rdtsc();

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

// Measure the cost of incrementing and decrementing the reference count in
// a SessionRef.
double sessionRefCount()
{
    int count = 1000000;
    Transport::Session session;
    Transport::SessionRef ref1(&session);
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        Transport::SessionRef ref2 = ref1;
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of an sfence instruction.
double sfence()
{
    int count = 1000000;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        Fence::sfence();
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of acquiring and releasing a SpinLock (assuming the
// lock is initially free).
double spinLock()
{
    int count = 1000000;
    SpinLock lock;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        lock.lock();
        lock.unlock();
    }
    uint64_t stop = Cycles::rdtsc();
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
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        std::thread thread(&spawnThreadHelper);
        thread.join();
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of starting and stopping a Dispatch::Timer.
double startStopTimer()
{
    int count = 1000000;
    Dispatch dispatch(false);
    Dispatch::Timer timer(&dispatch);
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        timer.start(12345U);
        timer.stop();
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of throwing and catching an int. This uses an integer as
// the value thrown, which is presumably as fast as possible.
double throwInt()
{
    int count = 10000;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        try {
            throw 0;
        } catch (int) { // NOLINT
            // pass
        }
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of throwing and catching an int from a function call.
double throwIntNL()
{
    int count = 10000;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        try {
            PerfHelper::throwInt();
        } catch (int) { // NOLINT
            // pass
        }
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of throwing and catching an Exception. This uses an actual
// exception as the value thrown, which may be slower than throwInt.
double throwException()
{
    int count = 10000;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        try {
            throw ObjectDoesntExistException(HERE);
        } catch (const ObjectDoesntExistException&) {
            // pass
        }
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of throwing and catching an Exception from a function call.
double throwExceptionNL()
{
    int count = 10000;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        try {
            PerfHelper::throwObjectDoesntExistException();
        } catch (const ObjectDoesntExistException&) {
            // pass
        }
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of throwing and catching an Exception using
// ClientException::throwException.
double throwSwitch()
{
    int count = 10000;
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        try {
            ClientException::throwException(HERE, STATUS_OBJECT_DOESNT_EXIST);
        } catch (const ObjectDoesntExistException&) {
            // pass
        }
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

// Measure the cost of pushing a new element on a std::vector, copying
// from the end to an internal element, and popping the end element.
double vectorPushPop()
{
    int count = 100000;
    std::vector<int> vector;
    vector.push_back(1);
    vector.push_back(2);
    vector.push_back(3);
    uint64_t start = Cycles::rdtsc();
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
    uint64_t stop = Cycles::rdtsc();
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
    {"atomicIntCmpX", atomicIntCmpX,
     "Atomic<int>::compareExchange"},
    {"atomicIntInc", atomicIntInc,
     "Atomic<int>::inc"},
    {"atomicIntLoad", atomicIntLoad,
     "Atomic<int>::load"},
    {"atomicIntStore", atomicIntStore,
     "Atomic<int>::store"},
    {"atomicIntXchg", atomicIntXchg,
     "Atomic<int>::exchange"},
    {"bMutexNoBlock", bMutexNoBlock,
     "std::mutex lock/unlock (no blocking)"},
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
    {"bufferIterator", bufferIterator,
     "iterate over buffer with 5 chunks"},
    {"condPingPong", condPingPong,
     "std::condition_variable round-trip"},
    {"cppAtomicExchg", cppAtomicExchange,
     "Exchange method on a C++ atomic_int"},
    {"cppAtomicLoad", cppAtomicLoad,
     "Read a C++ atomic_int"},
    {"cyclesToSeconds", perfCyclesToSeconds,
     "Convert a rdtsc result to (double) seconds"},
    {"cyclesToNanos", perfCyclesToNanoseconds,
     "Convert a rdtsc result to (uint64_t) nanoseconds"},
    {"dispatchPoll", dispatchPoll,
     "Dispatch::poll (no timers or pollers)"},
    {"div32", div32,
     "32-bit integer division instruction"},
    {"div64", div64,
     "64-bit integer division instruction"},
    {"functionCall", functionCall,
     "Call a function that has not been inlined"},
    {"getThreadId", getThreadId,
     "Retrieve thread id via ThreadId::get"},
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
    {"memcpy100", memcpy100,
     "Copy 100 bytes with memcpy"},
    {"memcpy1000", memcpy1000,
     "Copy 1000 bytes with memcpy"},
    {"memcpy10000", memcpy10000,
     "Copy 10000 bytes with memcpy"},
    {"murmur3", murmur3<1>,
     "128-bit MurmurHash3 (64-bit optimised) on 1 byte of data"},
    {"murmur3", murmur3<256>,
     "128-bit MurmurHash3 hash (64-bit optimised) on 256 bytes of data"},
    {"objectPoolAlloc", objectPoolAlloc<int, false>,
     "Cost of new allocations from an ObjectPool (no destroys)"},
    {"objectPoolRealloc", objectPoolAlloc<int, true>,
     "Cost of ObjectPool allocation after destroying an object"},
    {"prefetch", prefetch,
     "Prefetch instruction"},
    {"rdtsc", rdtscTest,
     "Read the fine-grain cycle counter"},
    {"segmentEntrySort", segmentEntrySort,
     "Sort a Segment full of avg. 100-byte Objects by age"},
    {"segmentIterator", segmentIterator<50, 150>,
     "Iterate a Segment full of avg. 100-byte Objects"},
#if 0  // Causes a double free.
    {"sessionRefCount", sessionRefCount,
     "Create/delete SessionRef"},
#endif
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
    int width = printf("%-18s ", info.name);
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
    bindThreadToCpu(3);
    if (argc == 1) {
        // No test names specified; run all tests.
        foreach (TestInfo& info, tests) {
            runTest(info);
        }
    } else {
        // Run only the tests that were specified on the command line.
        for (int i = 1; i < argc; i++) {
            bool foundTest = false;
            foreach (TestInfo& info, tests) {
                if (strcmp(argv[i], info.name) == 0) {
                    foundTest = true;
                    runTest(info);
                    break;
                }
            }
            if (!foundTest) {
                int width = printf("%-18s ??", argv[i]);
                printf("%*s No such test\n", 26-width, "");
            }
        }
    }
}
