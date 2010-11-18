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
 * Benchmark for dm's rabinpoly code.
 * on.
 */

// RAMCloud pragma [CPPLINT=0]

#include <Common.h>
#include <BenchUtil.h>
#include <rabinpoly.h>

static const uint64_t  RABIN_POLYNOMIAL = 0x92d42091a28158a5ull;

static void
measure(int bytes)
{
    uint8_t *array = reinterpret_cast<uint8_t *>(xmalloc(bytes)); 
    rabinpoly rp(RABIN_POLYNOMIAL);
    uint64_t checksum = 0;

    // randomize input
    for (int i = 0; i < bytes; i++)
        array[i] = generateRandom();

    // warm up
    for (int i = 0; i < bytes; i++)
        checksum = rp.append8(checksum, array[i]);

    // do more runs for smaller inputs
    int runs = 1;
    if (bytes < 4096)
        runs = 100;

    // run the test. be sure method call isn't removed by the compiler.
    uint64_t total = 0;
    for (int i = 0; i < runs; i++) { 
        uint64_t before = rdtsc();
        for (int i = 0; i < bytes; i++)
            checksum = rp.append8(checksum, array[i]);
        total += (rdtsc() - before);
    }
    total /= runs;

    uint64_t nsec = RAMCloud::cyclesToNanoseconds(total);
    printf("%10d bytes: %10llu ticks    %10llu nsec    %3llu nsec/byte   "
        "%7llu MB/sec    cksum 0x%16lx\n",
        bytes,
        total,
        nsec,
        nsec / bytes,
        (uint64_t)(1.0e9 / ((float)nsec / bytes) / (1024*1024)),
        checksum);

    free(array);
}

int
main()
{
    for (int i = 1; i < 128; i++)
        measure(i);
    for (int i = 128; i <= (16 * 1024 * 1024); i *= 2)
        measure(i);

    return 0;
}
