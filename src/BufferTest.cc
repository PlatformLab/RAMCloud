/* Copyright (c) 2010 Stanford University
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

/**
 * \file
 * Unit tests for Buffer.
 */

#include <string.h>
#include <strings.h>

#include <Buffer.h>

#include <cppunit/extensions/HelperMacros.h>

namespace RAMCloud {

class BufferTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(BufferTest);

    CPPUNIT_TEST(test_prependZero);
    CPPUNIT_TEST(test_prependAllocateMoreChunks);
    CPPUNIT_TEST(test_prependNormal);

    CPPUNIT_TEST(test_appendZero);
    CPPUNIT_TEST(test_appendAllocateMoreChunks);
    CPPUNIT_TEST(test_appendNormal);

    CPPUNIT_TEST(test_peekLengthZero);
    CPPUNIT_TEST(test_peekNormal);
    CPPUNIT_TEST(test_peekSpanningChunks);
    CPPUNIT_TEST(test_peekOffsetGreaterThanTotalLength);

    CPPUNIT_TEST(test_copyLengthZero);
    CPPUNIT_TEST(test_copySpanningChunks);
    CPPUNIT_TEST(test_copyNormal);
    CPPUNIT_TEST(test_copyOffsetGreaterThanTotalLength);

    CPPUNIT_TEST(test_findChunkNormal);
    CPPUNIT_TEST(test_findChunkOffsetGreaterThanTotalLength);

    CPPUNIT_TEST(test_offsetOfChunkNormal);
    CPPUNIT_TEST(test_offsetOfChunkEdgeCases);

    CPPUNIT_TEST(test_allocateMoreChunks);

    CPPUNIT_TEST(test_totalLength);

    CPPUNIT_TEST_SUITE_END();

    char* testString;

  public:
    BufferTest() { }

    void setUp() { testString = "0123456789"; }

    void tearDown() { }

    void test_prependZero() {
        Buffer b;

        // Since size == 0 is checked before asserting src, this should not
        // fail/crash the program.
        b.prepend(NULL, 0);

        // TODO(aravindn): How do we test assert(NULL)? Perform same test in
        // test_appendZero().

        CPPUNIT_ASSERT_EQUAL(0, b.chunksUsed);
    }

    void test_prependAllocateMoreChunks() {
        Buffer b;
        char buf[150];
        int i;

        for (i = 0; i < 15; ++i) {
            CPPUNIT_ASSERT(memcpy(buf + (i*10), testString, 10));
            b.prepend(buf + (i*10), 10);
        }

        CPPUNIT_ASSERT_EQUAL(15, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL(20, b.chunksAvail);

        for (i = 0; i < 15; ++i)
            CPPUNIT_ASSERT(!memcmp(b.chunks[i].ptr, buf + (i*10), 10));
    }

    void test_prependNormal() {
        Buffer b;
        char buf[10];
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));
        b.prepend(buf, 10);
        CPPUNIT_ASSERT_EQUAL(1, b.chunksUsed);
        CPPUNIT_ASSERT(!memcmp(b.chunks[0].ptr, buf, 10));

        b.prepend(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL(2, b.chunksUsed);
        CPPUNIT_ASSERT(!memcmp(b.chunks[0].ptr, buf+2, 5));
    }

    void test_appendZero() {
        Buffer b;

        // Since size == 0 is checked before asserting src, this should not
        // fail/crash the proram.
        b.append(NULL, 0);
        CPPUNIT_ASSERT_EQUAL(0, b.chunksUsed);
    }

    void test_appendAllocateMoreChunks() {
        Buffer b;
        char buf[150];
        int i;

        for (i = 0; i < 15; ++i) {
            CPPUNIT_ASSERT(memcpy(buf + (i*10), testString, 10));
            b.prepend(buf + (i*10), 10);
        }

        CPPUNIT_ASSERT_EQUAL(15, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL(20, b.chunksAvail);

        for (i = 0; i < 15; ++i)
            CPPUNIT_ASSERT(!memcmp(b.chunks[i].ptr, buf + (i*10), 10));
    }

    void test_appendNormal() {
        Buffer b;
        char buf[10];
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));
        b.prepend(buf, 10);
        CPPUNIT_ASSERT_EQUAL(1, b.chunksUsed);
        CPPUNIT_ASSERT(!memcmp(b.chunks[0].ptr, buf, 10));

        b.prepend(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL(2, b.chunksUsed);
        CPPUNIT_ASSERT(!memcmp(b.chunks[0].ptr, buf+2, 5));
    }

    void test_peekLengthZero() {
        void *ret_val;
        Buffer b;
        char buf[10];
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        CPPUNIT_ASSERT_EQUAL((size_t) 0, b.peek(0, 0, &ret_val));
    }

    void test_peekNormal() {
        void *ret_val;
        Buffer b;
        char buf[10];

        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        CPPUNIT_ASSERT_EQUAL(1, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL((size_t) 10, b.peek(0, 10, &ret_val));
        CPPUNIT_ASSERT(!memcmp(buf, ret_val, 10));

        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL(2, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL((size_t) 5, b.peek(10, 5, &ret_val));
        CPPUNIT_ASSERT(!memcmp(buf+2, ret_val, 5));

        CPPUNIT_ASSERT_EQUAL((size_t) 8, b.peek(2, 8, &ret_val));
        CPPUNIT_ASSERT(!memcmp(buf+2, ret_val, 8));
    }

    void test_peekSpanningChunks() {
        void *ret_val;
        Buffer b;
        char buf[10];
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL(2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((size_t) 10, b.peek(0, 100, &ret_val));
        CPPUNIT_ASSERT(!memcmp(buf, ret_val, 10));

        CPPUNIT_ASSERT_EQUAL((size_t) 5, b.peek(10, 50, &ret_val));
        CPPUNIT_ASSERT(!memcmp(buf+2, ret_val, 5));
    }

    void test_peekOffsetGreaterThanTotalLength() {
        void *ret_val;
        Buffer b;
        char buf[10];
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL(2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((size_t) 0, b.peek(20, 10, &ret_val));
    }

    void test_copyLengthZero() {
        char dest[15];
        char buf[10];
        Buffer b;
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL(2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((size_t) 0, b.copy(0, 0, dest));
        CPPUNIT_ASSERT_EQUAL((size_t) 0, b.copy(10, 0, dest));
    }

    void test_copyNormal() {
        char dest[15];
        char buf[10];
        Buffer b;
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL(2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((size_t) 10, b.copy(0, 10, dest));
        CPPUNIT_ASSERT(!memcmp(buf, dest, 10));
        CPPUNIT_ASSERT_EQUAL((size_t) 5, b.copy(10, 5, dest+10));
        CPPUNIT_ASSERT(!memcmp(buf+2, dest+10, 5));
    } 

    void test_copySpanningChunks() {
        char dest[15];
        char buf[10];
        Buffer b;
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL(2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((size_t) 15, b.copy(0, 15, dest));
        CPPUNIT_ASSERT(!memcmp(dest, buf, 10));
        CPPUNIT_ASSERT(!memcmp(dest+10, buf+2, 5));

        CPPUNIT_ASSERT_EQUAL((size_t) 11, b.copy(4, 15, dest));
        CPPUNIT_ASSERT(!memcmp(dest, buf+4, 6));
        CPPUNIT_ASSERT(!memcmp(dest+6, buf+2, 5));

        CPPUNIT_ASSERT_EQUAL((size_t) 10, b.copy(3, 10, dest));
        CPPUNIT_ASSERT(!memcmp(dest, buf+3, 7));
        CPPUNIT_ASSERT(!memcmp(dest+7, buf+2, 3));
    }

    void test_copyOffsetGreaterThanTotalLength() {
        char dest[15];
        char buf[10];
        Buffer b;
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL(2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((size_t) 0, b.copy(20, 10, dest));
    }

    void test_findChunkNormal() {
        char dest[15];
        char buf[10];
        Buffer b;
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL(2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL(0, b.findChunk(6));
        CPPUNIT_ASSERT_EQUAL(1, b.findChunk(12));
    }

    void test_findChunkOffsetGreaterThanTotalLength() {
        char dest[15];
        char buf[10];
        Buffer b;
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL(2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL(-1, b.findChunk(17));
    }

    void test_offsetOfChunkNormal() {
        char dest[15];
        char buf[10];
        Buffer b;
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL(2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((off_t) 0, b.offsetOfChunk(0));
        CPPUNIT_ASSERT_EQUAL((off_t) 10, b.offsetOfChunk(1));

        CPPUNIT_ASSERT_EQUAL((off_t) 0, b.offsetOfChunk(b.findChunk(4)));
    }

    void test_offsetOfChunkEdgeCases() {
        char dest[15];
        char buf[10];
        Buffer b;
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL(2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((off_t) -1, b.offsetOfChunk(-1));
        CPPUNIT_ASSERT_EQUAL((off_t) -1, b.offsetOfChunk(6));
    }

    void test_allocateMoreChunks() {
        Buffer b;

        CPPUNIT_ASSERT_EQUAL(0, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL(10, b.chunksAvail);
        b.allocateMoreChunks();
        CPPUNIT_ASSERT_EQUAL(20, b.chunksAvail);
        b.allocateMoreChunks();
        CPPUNIT_ASSERT_EQUAL(40, b.chunksAvail);
    }
    
    void test_totalLength() {
        Buffer b;
        uint8_t buf1[100];
        bzero(buf1, 100);

        CPPUNIT_ASSERT_EQUAL(0, (int) b.totalLength());
        b.prepend(buf1, 100);
        b.append(buf1, 10);
        CPPUNIT_ASSERT_EQUAL(110, (int) b.totalLength());
    }
};


CPPUNIT_TEST_SUITE_REGISTRATION(BufferTest);

}  // namespace RAMCloud
