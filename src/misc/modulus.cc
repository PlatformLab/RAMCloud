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
 * Benchmark for modulus vs masking a power of 2.
 */

// RAMCloud pragma [CPPLINT=0]

#include <Common.h>

#define ITERATIONS 10000000

static void
measure(const uint64_t r)
{
    const uint64_t mask = r - 1;
    uint64_t foo = 0;
    uint64_t i;

    printf("r: 0x%lx\n", r);

    { // modulus
        uint64_t start = rdtsc();
        for (i = 1; i < ITERATIONS; i++) {
            asm("");
            foo += i % r;
            asm("");
        }
        uint64_t elapsed = rdtsc() - start;
        printf("modulus cycles: %f\n", (double) elapsed / ITERATIONS);
    }

    { // divide
        uint64_t start = rdtsc();
        for (i = 1; i < ITERATIONS; i++) {
            asm("");
            foo += i / r;
            asm("");
        }
        uint64_t elapsed = rdtsc() - start;
        printf("divide cycles: %f\n", (double) elapsed / ITERATIONS);
    }

    { // masking
        if ((r & (r-1)) != 0)
            printf("masking is bogus because not a power of 2\n");
        uint64_t start = rdtsc();
        for (i = 1; i < ITERATIONS; i++) {
            asm("");
            foo += i & mask;
            asm("");
        }
        uint64_t elapsed = rdtsc() - start;
        printf("masking cycles: %f\n", (double) elapsed / ITERATIONS);
    }

    { // if (power of 2) masking, else modulus
        uint64_t start = rdtsc();
        for (i = 1; i < ITERATIONS; i++) {
            asm("");
            if ((r & mask) == 0)
                foo += i & mask;
            else
                foo += i % r;
            asm("");
        }
        uint64_t elapsed = rdtsc() - start;
        printf("dynamic masking or modulus cycles: %f\n", (double) elapsed / ITERATIONS);
    }

    printf("\n");
}

int
main()
{
    measure(1804289383);
    measure(8469308867);
    measure(3258872058);
    measure(749008893);
    measure(3878847148);
    measure(2881896609);
    measure(2358955510);
    measure(1752224878);
    measure(3534577336);
    measure(3298511653);
    measure(1 << 30);
    measure(2426134194);
    measure(3255735297);
    measure(2819431787);
    measure(2416732117);
    measure(2581038180);
    measure(3594843307);
    measure(2035999013);
    measure(1868770349);
    measure(2636396647);
    measure(1764383048);
    measure(2809055677);
    measure(241819598);
    return 0;
}
