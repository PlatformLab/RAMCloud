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

    CPPUNIT_TEST(test_prepend_zero);
    CPPUNIT_TEST(test_prepend_allocateMoreChunks);
    CPPUNIT_TEST(test_prepend_normal);

    CPPUNIT_TEST(test_append_zero);
    CPPUNIT_TEST(test_append_allocateMoreChunks);
    CPPUNIT_TEST(test_append_normal);

    CPPUNIT_TEST(test_peek_lengthZero);
    CPPUNIT_TEST(test_peek_normal);
    CPPUNIT_TEST(test_peek_spanningChunks);
    CPPUNIT_TEST(test_peek_offsetGreaterThanTotalLength);

    CPPUNIT_TEST(test_read_inputEdgeCases);
    CPPUNIT_TEST(test_read_peek);
    CPPUNIT_TEST(test_read_copy);    

    CPPUNIT_TEST(test_copy_lengthZero);
    CPPUNIT_TEST(test_copy_spanningChunks);
    CPPUNIT_TEST(test_copy_normal);
    CPPUNIT_TEST(test_copy_offsetGreaterThanTotalLength);

    CPPUNIT_TEST(test_findChunk_normal);
    CPPUNIT_TEST(test_findChunk_offsetGreaterThanTotalLength);

    CPPUNIT_TEST(test_offsetOfChunk_normal);
    CPPUNIT_TEST(test_offsetOfChunk_edgeCases);

    CPPUNIT_TEST(test_allocateMoreChunks);

    CPPUNIT_TEST(test_totalLength);

    CPPUNIT_TEST_SUITE_END();

    char testString[10];

  public:
    BufferTest() { }

    void setUp() { CPPUNIT_ASSERT(memcpy(testString, "0123456789", 10)); }

    void tearDown() { }

    void test_prepend_zero() {
        Buffer b;
        char buf[10];
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.prepend(buf, 0);
        b.prepend(buf, 10);

        // TODO(aravindn): How do we test assert(NULL)? Perform same test in
        // test_appendZero().

        CPPUNIT_ASSERT_EQUAL((uint32_t) 2, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.chunks[1].size);
    }

    void test_prepend_allocateMoreChunks() {
        Buffer b;
        char buf[150];
        int i;

        for (i = 0; i < 15; ++i) {
            CPPUNIT_ASSERT(memcpy(buf + (i*10), testString, 10));
            b.prepend(buf + (i*10), 10);
        }

        CPPUNIT_ASSERT_EQUAL((uint32_t) 15, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 20, b.chunksAvail);

        for (i = 0; i < 15; ++i)
            CPPUNIT_ASSERT(!memcmp(b.chunks[i].ptr, buf + (i*10), 10));
    }

    void test_prepend_normal() {
        Buffer b;
        char buf[10];
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));
        b.prepend(buf, 10);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 1, b.chunksUsed);
        CPPUNIT_ASSERT(!memcmp(b.chunks[0].ptr, buf, 10));

        b.prepend(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 2, b.chunksUsed);
        CPPUNIT_ASSERT(!memcmp(b.chunks[0].ptr, buf+2, 5));
    }

    void test_append_zero() {
        Buffer b;
        char buf[10];
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 0);
        b.append(buf, 10);
        
        CPPUNIT_ASSERT_EQUAL((uint32_t) 2, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.chunks[0].size);
    }

    void test_append_allocateMoreChunks() {
        Buffer b;
        char buf[150];
        int i;

        for (i = 0; i < 15; ++i) {
            CPPUNIT_ASSERT(memcpy(buf + (i*10), testString, 10));
            b.prepend(buf + (i*10), 10);
        }

        CPPUNIT_ASSERT_EQUAL((uint32_t) 15, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 20, b.chunksAvail);

        for (i = 0; i < 15; ++i)
            CPPUNIT_ASSERT(!memcmp(b.chunks[i].ptr, buf + (i*10), 10));
    }

    void test_append_normal() {
        Buffer b;
        char buf[10];
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));
        b.prepend(buf, 10);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 1, b.chunksUsed);
        CPPUNIT_ASSERT(!memcmp(b.chunks[0].ptr, buf, 10));

        b.prepend(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 2, b.chunksUsed);
        CPPUNIT_ASSERT(!memcmp(b.chunks[0].ptr, buf+2, 5));
    }

    void test_peek_lengthZero() {
        void *ret_val;
        Buffer b;
        char buf[10];
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.peek(0, 0, &ret_val));
    }

    void test_peek_normal() {
        void *ret_val;
        Buffer b;
        char buf[10];

        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 1, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 10, b.peek(0, 10, &ret_val));
        CPPUNIT_ASSERT(!memcmp(buf, ret_val, 10));

        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 2, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 5, b.peek(10, 5, &ret_val));
        CPPUNIT_ASSERT(!memcmp(buf+2, ret_val, 5));

        CPPUNIT_ASSERT_EQUAL((uint32_t) 8, b.peek(2, 8, &ret_val));
        CPPUNIT_ASSERT(!memcmp(buf+2, ret_val, 8));
    }

    void test_peek_spanningChunks() {
        void *ret_val;
        Buffer b;
        char buf[10];
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 10, b.peek(0, 100, &ret_val));
        CPPUNIT_ASSERT(!memcmp(buf, ret_val, 10));

        CPPUNIT_ASSERT_EQUAL((uint32_t) 5, b.peek(10, 50, &ret_val));
        CPPUNIT_ASSERT(!memcmp(buf+2, ret_val, 5));
    }

    void test_peek_offsetGreaterThanTotalLength() {
        void *ret_val;
        Buffer b;
        char buf[10];
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.peek(20, 10, &ret_val));
    }

    void test_read_inputEdgeCases() {
        void *ret_val;
        Buffer b;
        char buf[10];
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        b.append(buf+1, 6);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 3, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.read(100, 40, &ret_val));
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.read(22, 3, &ret_val));
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.read(2, 0, &ret_val));
    }

    void test_read_peek() {
        void *ret_val;
        Buffer b;
        char buf[10];
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        b.append(buf+1, 6);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 3, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 10, b.read(0, 10, &ret_val));
        CPPUNIT_ASSERT(!b.bufRead);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.bufReadSize);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 4, b.read(11, 4, &ret_val));
        CPPUNIT_ASSERT(!b.bufRead);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.bufReadSize);
    }

    void test_read_copy() {
        void *ret_val;
        Buffer b;
        char buf[10];
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        b.append(buf+1, 6);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 3, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 14, b.read(0, 14, &ret_val));
        CPPUNIT_ASSERT(b.bufRead);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 14, b.bufReadSize);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 11, b.read(10, 11, &ret_val));
        CPPUNIT_ASSERT(b.bufRead);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 14+11, b.bufReadSize);
    } 

    void test_copy_lengthZero() {
        char dest[15];
        char buf[10];
        Buffer b;
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.copy(0, 0, dest));
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.copy(10, 0, dest));
    }

    void test_copy_normal() {
        char dest[15];
        char buf[10];
        Buffer b;
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 10, b.copy(0, 10, dest));
        CPPUNIT_ASSERT(!memcmp(buf, dest, 10));
        CPPUNIT_ASSERT_EQUAL((uint32_t) 5, b.copy(10, 5, dest+10));
        CPPUNIT_ASSERT(!memcmp(buf+2, dest+10, 5));
    } 

    void test_copy_spanningChunks() {
        char dest[15];
        char buf[10];
        Buffer b;
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 15, b.copy(0, 15, dest));
        CPPUNIT_ASSERT(!memcmp(dest, buf, 10));
        CPPUNIT_ASSERT(!memcmp(dest+10, buf+2, 5));

        CPPUNIT_ASSERT_EQUAL((uint32_t) 11, b.copy(4, 15, dest));
        CPPUNIT_ASSERT(!memcmp(dest, buf+4, 6));
        CPPUNIT_ASSERT(!memcmp(dest+6, buf+2, 5));

        CPPUNIT_ASSERT_EQUAL((uint32_t) 10, b.copy(3, 10, dest));
        CPPUNIT_ASSERT(!memcmp(dest, buf+3, 7));
        CPPUNIT_ASSERT(!memcmp(dest+7, buf+2, 3));
    }

    void test_copy_offsetGreaterThanTotalLength() {
        char dest[15];
        char buf[10];
        Buffer b;
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.copy(20, 10, dest));
    }

    void test_findChunk_normal() {
        char buf[10];
        Buffer b;
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.findChunk(6));
        CPPUNIT_ASSERT_EQUAL((uint32_t) 1, b.findChunk(12));
    }

    void test_findChunk_offsetGreaterThanTotalLength() {
        char buf[10];
        Buffer b;
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL(b.chunksUsed, b.findChunk(17));
    }

    void test_offsetOfChunk_normal() {
        char buf[10];
        Buffer b;
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.offsetOfChunk(0));
        CPPUNIT_ASSERT_EQUAL((uint32_t) 10, b.offsetOfChunk(1));

        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.offsetOfChunk(b.findChunk(4)));
    }

    void test_offsetOfChunk_edgeCases() {
        char buf[10];
        Buffer b;
        CPPUNIT_ASSERT(memcpy(buf, testString, 10));

        b.append(buf, 10);
        b.append(buf+2, 5);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 2, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL(b.totalLen, b.offsetOfChunk(6));
    }

    void test_allocateMoreChunks() {
        Buffer b;

        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.chunksUsed);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 10, b.chunksAvail);
        b.allocateMoreChunks();
        CPPUNIT_ASSERT_EQUAL((uint32_t) 20, b.chunksAvail);
        b.allocateMoreChunks();
        CPPUNIT_ASSERT_EQUAL((uint32_t) 40, b.chunksAvail);
    }
    
    void test_totalLength() {
        Buffer b;
        uint8_t buf1[100];
        bzero(buf1, 100);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.totalLength());
        b.prepend(buf1, 100);
        b.append(buf1, 10);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 110, b.totalLength());
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(BufferTest);

}  // namespace RAMCloud
