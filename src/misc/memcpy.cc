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
 * Benchmark for memcpy.
 */

// RAMCloud pragma [CPPLINT=0]

#include <Common.h>
#include <BenchUtil.h>

static void
measure(int bytes, bool cached)
{
    uint8_t *src = reinterpret_cast<uint8_t *>(xmalloc(bytes)); 
    uint8_t *dst = reinterpret_cast<uint8_t *>(xmalloc(bytes));
    uint8_t *scrub = reinterpret_cast<uint8_t *>(xmalloc(64 * 1024 * 1024));

    // the very first memcpy seems to have a big overhead, so burn though
    // it before timing
    memcpy(dst, src, bytes);

    // do more runs for smaller inputs
    int runs = 10;
    if (bytes < 4096)
        runs = 100;

    uint64_t total = 0;
    for (int i = 0; i < runs; i++ ) {
        // wreck the cache
        if (!cached) {
            for (int i = 0; i < 64 * 1024 * 1024; i++)
                scrub[i]++;
        }

        // now time it
        uint64_t before = rdtsc();
        memcpy(dst, src, bytes);
        total += (rdtsc() - before);
    }
    total /= runs;

    printf("%10d bytes: %10lu ticks    %10lu nsec    %.2f nsec/byte\n",
        bytes,
        total,
        RAMCloud::cyclesToNanoseconds(total),
        (double)RAMCloud::cyclesToNanoseconds(total) / bytes);

    free(src);
    free(dst);
    free(scrub);
}

int
main()
{
    printf("=== Cached Memcpy ===\n");
    for (int i = 1; i <= (16 * 1024 * 1024); i *= 2)
        measure(i, true);

    printf("\n=== Uncached Memcpy ===\n");
    for (int i = 1; i <= (16 * 1024 * 1024); i *= 2)
        measure(i, false);

    return 0;
}
