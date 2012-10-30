/* Copyright (c) 2010-2011 Stanford University
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
 * Benchmark for memcpy.
 */

#include <math.h>
#include <thread>

#include "Common.h"
#include "Cycles.h"
#include "Memory.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * The size of #scrub.
 */
enum { SCRUB_SIZE = 1024UL * 1024 * 1024 };

/**
 * A memory area used as scratch space to empty out caches.
 */
static uint8_t scrub[SCRUB_SIZE];

/**
 * The most naive implementation of memcpy possible.
 */
void*
dumbMemcpy(void* dst, const void* src, size_t bytes)
{
    for (size_t i = 0; i < bytes; i++) {
        reinterpret_cast<uint8_t*>(dst)[i] =
            reinterpret_cast<const uint8_t*>(src)[i];
    }
    return dst;
}

/**
 * An implementation of memcpy that spawns off threads and gives them a
 * fraction of the work.
 * \tparam numJobs
 *      The total number of threads that will copy a fraction of the memory.
 *      The number of thread spawned is numJobs - 1, since the calling thread
 *      also copies a fraction.
 * \tparam memcpyFn
 *      The implementation of memcpy used by each thread.
 */
template<uint32_t numJobs,
         void* (*memcpyFn)(void*, const void*, size_t)>
void*
parallelMemcpy(void* dst, const void* src, size_t bytes)
{
    uint32_t numThreads = numJobs - 1;
    size_t bytesPerJob = bytes / numJobs;
    Tub<std::thread> threads[numThreads];
    uint32_t i;
    for (i = 0; i < numThreads; i++) {
        threads[i].construct(memcpyFn,
             reinterpret_cast<uint8_t*>(dst) + bytesPerJob * i,
             reinterpret_cast<const uint8_t*>(src) + bytesPerJob * i,
             bytesPerJob);
    }
    memcpyFn(reinterpret_cast<uint8_t*>(dst) + bytesPerJob * i,
             reinterpret_cast<const uint8_t*>(src) + bytesPerJob * i,
             bytes - bytesPerJob * i);
    for (i = 0; i < numThreads; i++)
        threads[i]->join();
    return dst;
}

/**
 * Execute nop instructions.
 * \tparam N
 *      How many nop instructions to execute.
 */
template<uint32_t N>
inline void
nop()
{
    asm volatile("nop");
    nop<N - 1>();
}
template<>
inline void
nop<0>()
{
}

/**
 * Measure memcpy performance for a particular memcpy implementation.
 * \tparam N
 *      The number of trials.
 * \tparam cached
 *      Whether to measure a memcpy with warm caches.
 * \tparam memcpyFn
 *      The memcpy implementation to measure.
 * \param bytes
 *      The number of bytes to copy.
 */
// This is a struct because you can't partially specialize a function.
template<uint32_t N, bool cached,
         void* (*memcpyFn)(void*, const void*, size_t)>
struct timeCopy {
    timeCopy(uint64_t results[N], void* dst, const void* src, size_t bytes) {
        // wreck the cache
        if (!cached) {
            for (uint64_t i = 0; i < SCRUB_SIZE; i++)
                scrub[i]++;
            // reload instruction cache
            memcpyFn(scrub, &scrub[1024 * 1024],
                     std::min(bytes, 1024UL * 1024));
        }

        // Start with a sane pipeline
        nop<64>();

        // now time it
        uint64_t before = Cycles::rdtsc();
        memcpyFn(dst, src, bytes);
        *results = Cycles::rdtsc() - before;

        // compile-time recursion
        timeCopy<N-1, cached, memcpyFn>(results + 1, dst, src, bytes);
    }
};

template<bool cached, void* (*memcpyFn)(void*, const void*, size_t)>
struct timeCopy<0, cached, memcpyFn> {
    timeCopy(uint64_t* results, void* dst, const void* src, size_t bytes) {
    }
};

/**
 * Measure memcpy performance for a particular memcpy implementation.
 * \tparam cached
 *      Whether to measure a memcpy with warm caches.
 * \tparam memcpyFn
 *      The memcpy implementation to measure.
 * \param bytes
 *      The number of bytes to copy.
 */
template<bool cached, void* (*memcpyFn)(void*, const void*, size_t)>
static void
measure(uint64_t bytes)
{
    uint8_t *src = reinterpret_cast<uint8_t *>(Memory::xmalloc(HERE, bytes));
    uint8_t *dst = reinterpret_cast<uint8_t *>(Memory::xmalloc(HERE, bytes));

    // Write to all pages so that the OS is forced to allocate them. Note that
    // it's not sufficient to just read from all pages, as the OS can alias
    // them all to the zero page.
    for (uint64_t i = 0; i < bytes; i++) {
        src[i]++;
        dst[i]++;
    }
    for (uint64_t i = 0; i < SCRUB_SIZE; i++)
        scrub[i]++;

    // warm up the cache
    memcpyFn(dst, src, bytes);

    // Running through nops before an unrolled loop seems to result in a first
    // measurement that is reasonably close to the rest.
    const int runs = 10;
    uint64_t results[runs];
    timeCopy<runs, cached, memcpyFn>(results, dst, src, bytes);

    uint64_t min = ~0UL;
    uint64_t max = 0;
    uint64_t total = 0;
    uint64_t sumSquares = 0;
    for (int j = 0; j < runs; j++) {
        min = std::min(min, results[j]);
        max = std::max(max, results[j]);
        total += results[j];
        sumSquares += results[j] * results[j];
    }
    double avg = downCast<double>(total) * 1.0 / runs;
    double stddev = sqrt(downCast<double>(sumSquares) * 1.0 / runs -
                         downCast<double>(total) *
                         downCast<double>(total) * 1.0 / runs / runs);

    printf("%10lu bytes: "
           "%10lu avg    "
           "%10lu stddev "
           "%10lu min    "
           "%10lu max\n",
           bytes,
           Cycles::toNanoseconds(uint64_t(avg)),
           Cycles::toNanoseconds(uint64_t(stddev)),
           Cycles::toNanoseconds(min),
           Cycles::toNanoseconds(max));

    free(src);
    free(dst);
}

} // namespace RAMCloud

int
main()
{
    using namespace RAMCloud;
    uint64_t maxSize = 1024UL * 1024 * 1024;

    printf("=== Cached-libc Memcpy ===\n");
    for (uint64_t i = 1; i <= maxSize; i *= 2)
        measure<true, memcpy>(i);

    printf("\n=== Uncached-libc Memcpy ===\n");
    for (uint64_t i = 1; i <= maxSize; i *= 2)
        measure<false, memcpy>(i);

    printf("\n=== Cached-Dumb Memcpy ===\n");
    for (uint64_t i = 1; i <= maxSize; i *= 2)
        measure<true, dumbMemcpy>(i);

    printf("\n=== Uncached-Dumb Memcpy ===\n");
    for (uint64_t i = 1; i <= maxSize; i *= 2)
        measure<false, dumbMemcpy>(i);

    printf("\n=== Cached-2xlibc Memcpy ===\n");
    for (uint64_t i = 1; i <= maxSize; i *= 2)
        measure<true, parallelMemcpy<2, memcpy>>(i);

    printf("\n=== Uncached-2xlibc Memcpy ===\n");
    for (uint64_t i = 1; i <= maxSize; i *= 2)
        measure<false, parallelMemcpy<2, memcpy>>(i);

    return 0;
}
