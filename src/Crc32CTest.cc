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

#include "TestUtil.h"

#include "Crc32C.h"

namespace RAMCloud {

/*
 * A randomly-generated array of bytes to run checksums over.
 * The length of the array was chosen to exercise combinations
 * of 64-byte, 8-byte, and 1-byte block checksums.
 */
static const uint8_t input[81] = {
    0x67, 0xc6, 0x69, 0x73, 0x51, 0xff, 0x4a, 0xec, 0x29, 0xcd, 0xba, 0xab,
    0xf2, 0xfb, 0xe3, 0x46, 0x7c, 0xc2, 0x54, 0xf8, 0x1b, 0xe8, 0xe7, 0x8d,
    0x76, 0x5a, 0x2e, 0x63, 0x33, 0x9f, 0xc9, 0x9a, 0x66, 0x32, 0x0d, 0xb7,
    0x31, 0x58, 0xa3, 0x5a, 0x25, 0x5d, 0x05, 0x17, 0x58, 0xe9, 0x5e, 0xd4,
    0xab, 0xb2, 0xcd, 0xc6, 0x9b, 0xb4, 0x54, 0x11, 0x0e, 0x82, 0x74, 0x41,
    0x21, 0x3d, 0xdc, 0x87, 0x70, 0xe9, 0x3e, 0xa1, 0x41, 0xe1, 0xfc, 0x67,
    0x3e, 0x01, 0x7e, 0x97, 0xea, 0xdc, 0x6b, 0x96, 0x8f
};

/*
 * Checksums corresponding to the above random array. Each i'th offset is
 * the checksum starting at input[0] and spanning i bytes. That is,
 * crcByLength[0] is a checksum over nothing, and crcByLength[5] is a 
 * checksum from input[0] to input[4], inclusive.
 */ 
static const uint32_t crcByLength[sizeof(input) + 1] = {
    0x00000000, 0xb50cf789, 0x1fdd2d6f, 0x26be3ac5, 0x94922b1b, 0x2a0da7af,
    0x510fd774, 0x9ccaf941, 0x1dff338c, 0x97a75e89, 0x86762dfd, 0x95370f7a,
    0x212d55a7, 0x64f5e39a, 0x93c9e582, 0x933ed984, 0x222568ea, 0xb42b3e97,
    0x6460e5f1, 0x97dec15f, 0x76160f8c, 0x46148ef3, 0x899178da, 0x8f42941a,
    0x46edda68, 0xbc609592, 0x49e81e9e, 0xb25c9ad7, 0x753db94c, 0x2ffe5590,
    0x5e3cc271, 0x389216c4, 0xfd65f7e2, 0x4591c990, 0x43355498, 0xa71add5f,
    0x694eea08, 0x4838dc97, 0x9dd6223c, 0xcc261611, 0xd83e9091, 0x7557db46,
    0x89a23b8f, 0xe99db17b, 0x2c6cc103, 0xc8801d29, 0xc345a6d9, 0x56ff1a35,
    0x110dd403, 0x2883e0bd, 0x5e3bbfc4, 0x78ece073, 0x879c8a35, 0x0eb4960b,
    0xec08fa7b, 0x7e42ba00, 0xe24b06d6, 0x59e8f262, 0xe36940e8, 0xdf085a87,
    0xe5f3c976, 0x850a4df1, 0x8e4be359, 0xb7896487, 0x00b78964, 0xd7c4e7f9,
    0x10890388, 0x94a41c22, 0x91326c90, 0x212950c4, 0x156db392, 0xcdab41f6,
    0x600ed455, 0xf82f368d, 0xcf4ddb9e, 0xe3ffe5c1, 0x7767c2de, 0xf70eb96c,
    0xb2e27c70, 0x89658e28, 0x1ee4a860, 0xbd3d7096
};

/**
 * Unit tests for Crc32C.
 */
class Crc32CTest : public CppUnit::TestFixture {

    DISALLOW_COPY_AND_ASSIGN(Crc32CTest); // NOLINT

    CPPUNIT_TEST_SUITE(Crc32CTest);
    CPPUNIT_TEST(test_single_Crc32C);
    CPPUNIT_TEST(test_accumulated_Crc32C);
    CPPUNIT_TEST(test_accumulatedVaried_Crc32C);
    CPPUNIT_TEST_SUITE_END();

  public:
    Crc32CTest() {}

    void
    test_single_Crc32C()
    {
        for (unsigned int i = 0; i < sizeof(input); i++) {
            CPPUNIT_ASSERT_EQUAL(crcByLength[i], Crc32C(0, input, i));
        }
    }

    void
    test_accumulated_Crc32C()
    {
        uint32_t crc = 0;
        for (unsigned int i = 0; i < sizeof(input); i++) {
            crc = Crc32C(crc, &input[i], 1);
            CPPUNIT_ASSERT_EQUAL(crcByLength[i + 1], crc);
        }
    }

    void
    test_accumulatedVaried_Crc32C()
    {
        /*
         * All of these add up to 81 (sizeof(input)). The goal is to exercise
         * different block's worth for each invocation.
         */
        int spans[8][5] = {
            { 1, 7, 8, 64, 1 },
            { 1, 64, 8, 1, 7 },
            { 7, 1, 8, 64, 1 },
            { 7, 1, 1, 8, 64 },
            { 8, 1, 64, 1, 7 },
            { 8, 7, 1, 64, 1 },
            { 64, 7, 8, 1, 1 },
            { 64, 7, 1, 1, 8 }
        };

        for (int i = 0; i < 8; i++) {
            uint32_t crc = 0;
            int offset = 0;
            for (int j = 0; j < 5; j++) {
                crc = Crc32C(crc, &input[offset], spans[i][j]);
                offset += spans[i][j];
                CPPUNIT_ASSERT_EQUAL(crcByLength[offset], crc);
            }
        }
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(Crc32CTest);

} // namespace RAMCloud
