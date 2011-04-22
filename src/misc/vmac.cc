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
 * Benchmark for vmac as implemented in crytpo++ (cryptopp.com). 
 * Link with -lcryptopp to use this.
 */

// RAMCloud pragma [CPPLINT=0]

#include <Common.h>
#include <BenchUtil.h>
#include <cryptopp/cryptlib.h>
#include <cryptopp/aes.h>
#include <cryptopp/vmac.h>

template<int BITS>
static void
measure(int bytes, bool print = true)
{
    uint8_t *array = reinterpret_cast<uint8_t *>(xmalloc(bytes)); 
    CryptoPP::VMAC<CryptoPP::AES, BITS> vmac;
    byte digest[vmac.DigestSize()];
    byte key[16] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    byte iv[1] = { 0 };

    // randomize input
    for (int i = 0; i < bytes; i++)
        array[i] = generateRandom();

    vmac.SetKeyWithIV(key, sizeof(key), iv, sizeof(iv));

    // do more runs for smaller inputs
    int runs = 1;
    if (bytes < 4096)
        runs = 100;

    uint64_t total = 0;
    for (int i = 0; i < runs; i++) {
        // run the test. be sure method call isn't removed by the compiler.
        uint64_t before = rdtsc();
        vmac.CalculateDigest(digest, array, bytes);
        total += (rdtsc() - before);
    }
    total /= runs;

    if (print) {
        uint64_t nsec = RAMCloud::cyclesToNanoseconds(total);
        printf("%10d bytes: %10llu ticks    %10llu nsec    %3llu nsec/byte   "
            "%7llu MB/sec    digest 0x",
            bytes,
            total,
            nsec,
            nsec / bytes,
            (uint64_t)(1.0e9 / ((float)nsec / bytes) / (1024*1024)));

        for (uint32_t i = 0; i < vmac.DigestSize(); i++)
            printf("%x", digest[i]);
        printf("\n");
    }

    free(array);
}

int
main()
{
    printf("=== 64-bit digest ===\n");
    measure<64>(4096, false);   // warm up
    for (int i = 1; i < 128; i++)
        measure<64>(i);
    for (int i = 128; i <= (16 * 1024 * 1024); i *= 2)
        measure<64>(i);

    printf("\n");

    printf("=== 128-bit digest ===\n");
    measure<128>(4096, false);   // warm up
    for (int i = 1; i < 128; i++)
        measure<128>(i);
    for (int i = 128; i <= (16 * 1024 * 1024); i *= 2)
        measure<128>(i);

    return 0;
}
