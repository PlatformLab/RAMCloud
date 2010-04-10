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
 * \file BufferTest.cc Unit tests for Buffer.
 */

#include <string.h>
#include <strings.h>

#include <Buffer.h>

#include <cppunit/extensions/HelperMacros.h>

namespace RAMCloud {

class BufferTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(BufferTest);

    CPPUNIT_TEST(test_constructor);

    CPPUNIT_TEST(test_prepend_zero);
    CPPUNIT_TEST(test_prepend_allocateMoreChunks);
    CPPUNIT_TEST(test_prepend_normal);

    CPPUNIT_TEST(test_append_zero);
    CPPUNIT_TEST(test_append_allocateMoreChunks);
    CPPUNIT_TEST(test_append_normal);

    CPPUNIT_TEST(test_peek_normal);
    CPPUNIT_TEST(test_peek_offsetGreaterThanTotalLength);

    CPPUNIT_TEST(test_getRange_inputEdgeCases);
    CPPUNIT_TEST(test_getRange_peek);
    CPPUNIT_TEST(test_getRange_copy);

    CPPUNIT_TEST(test_copy_lengthZero);
    CPPUNIT_TEST(test_copy_spanningChunks);
    CPPUNIT_TEST(test_copy_normal);
    CPPUNIT_TEST(test_copy_offsetGreaterThanTotalLength);

    CPPUNIT_TEST(test_findChunk_normal);
    CPPUNIT_TEST(test_findChunk_offsetGreaterThanTotalLength);

    CPPUNIT_TEST(test_allocateMoreChunks);

    CPPUNIT_TEST(test_allocateMoreExtraBufs);

    CPPUNIT_TEST(test_totalLength);

    CPPUNIT_TEST_SUITE_END();

    char testStr[30];
    char testStr1[10];
    char testStr2[10];
    char testStr3[10];
    char cmpBuf[30];    // To use for strcmp at the end of a test.
    Buffer *buf;

  public:
    BufferTest() : buf(NULL) {
        memcpy(testStr, "0123456789abcdefghijklmnopqrs\0", 30);
        memcpy(testStr1, "0123456789", 10);
        memcpy(testStr2, "abcdefghij", 10);
        memcpy(testStr3, "klmnopqrs\0", 10);
    }

    void setUp() {
        // Don't want to use append, since we are testing that separately. So,
        // manually adding chunks.
        buf = new Buffer();
        Buffer::Chunk* c = buf->chunks;

        c->data = testStr1;
        c->len = 10;
        c++;

        c->data = testStr2;
        c->len = 10;
        c++;

        c->data = testStr3;
        c->len = 10;

        buf->chunksUsed = 3;
        buf->totalLen = 30;
    }

    void tearDown() { delete buf; }

    void test_constructor() {
        // Basic sanity checks for the constructor.
        Buffer b;
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 10, b.chunksAvail);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.totalLen);
        CPPUNIT_ASSERT(b.extraBufs == NULL);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.extraBufsAvail);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.extraBufsUsed);
        CPPUNIT_ASSERT(b.chunks != NULL);
    }

    void test_prepend_zero() {
        Buffer b;

        b.prepend(testStr1, 0);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.chunksUsed);

        b.prepend(testStr3, 10);
        b.prepend(testStr2, 10);
        b.prepend(testStr1, 10);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 3, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 30, b.totalLen);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 30, b.copy(0, 30, cmpBuf));
        CPPUNIT_ASSERT_EQUAL(0, strcmp(cmpBuf, testStr));
    }

    void test_prepend_allocateMoreChunks() {
        Buffer b;
        char tmpBuf[150];
        int i;

        for (i = 0; i < 15; ++i) {
            CPPUNIT_ASSERT(memcpy(tmpBuf + (i*10), testStr, 10));
            b.prepend(tmpBuf + (i*10), 10);
        }

        CPPUNIT_ASSERT_EQUAL((uint32_t) 15, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 20, b.chunksAvail);

        for (i = 0; i < 15; ++i)
            CPPUNIT_ASSERT(!memcmp(b.chunks[i].data, tmpBuf + (i*10), 10));
    }

    void test_prepend_normal() {
        Buffer b;
        b.prepend(testStr3, 10);
        b.prepend(testStr2, 10);
        b.prepend(testStr1, 10);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 3, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 30, b.copy(0, 30, cmpBuf));
        CPPUNIT_ASSERT_EQUAL(0, strcmp(cmpBuf, testStr));
    }

    void test_append_zero() {
        Buffer b;

        b.append(testStr1, 0);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.chunksUsed);

        b.append(testStr1, 10);
        b.append(testStr2, 10);
        b.append(testStr3, 10);

        CPPUNIT_ASSERT_EQUAL((uint32_t) 3, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 30, b.totalLength());
        CPPUNIT_ASSERT_EQUAL((uint32_t) 30, b.copy(0, 30, cmpBuf));
        CPPUNIT_ASSERT_EQUAL(0, strcmp(cmpBuf, testStr));
    }

    void test_append_allocateMoreChunks() {
        Buffer b;
        char tmpBuf[150];
        int i;

        for (i = 0; i < 15; ++i) {
            CPPUNIT_ASSERT(memcpy(tmpBuf + (i*10), testStr, 10));
            b.prepend(tmpBuf + (i*10), 10);
        }

        CPPUNIT_ASSERT_EQUAL((uint32_t) 15, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 20, b.chunksAvail);

        for (i = 0; i < 15; ++i)
            CPPUNIT_ASSERT(!memcmp(b.chunks[i].data, tmpBuf + (i*10), 10));
    }

    void test_append_normal() {
        Buffer b;
        b.append(testStr1, 10);
        b.append(testStr2, 10);
        b.append(testStr3, 10);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 3, b.chunksUsed);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 30, b.copy(0, 30, cmpBuf));
        CPPUNIT_ASSERT_EQUAL(0, strcmp(cmpBuf, testStr));
    }

    void test_peek_normal() {
        void *ret_val;
        CPPUNIT_ASSERT_EQUAL((uint32_t) 10, buf->peek(0, &ret_val));
        CPPUNIT_ASSERT_EQUAL((uint32_t) 10, buf->peek(10, &ret_val));
        CPPUNIT_ASSERT_EQUAL((uint32_t) 5, buf->peek(25, &ret_val));
    }

    void test_peek_offsetGreaterThanTotalLength() {
        void *ret_val;
        CPPUNIT_ASSERT_EQUAL((uint32_t) 2, buf->peek(28, &ret_val));
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, buf->peek(50, &ret_val));
    }

    void test_getRange_inputEdgeCases() {
        CPPUNIT_ASSERT_EQUAL(reinterpret_cast<void*>(NULL),
                             buf->getRange(100, 40));
        CPPUNIT_ASSERT_EQUAL(reinterpret_cast<void*>(NULL),
                             buf->getRange(24, 10));
        CPPUNIT_ASSERT_EQUAL(reinterpret_cast<void*>(NULL),
                             buf->getRange(2, 0));
    }

    void test_getRange_peek() {
        void *ret_val;

        CPPUNIT_ASSERT_EQUAL((uint32_t) 3, buf->chunksUsed);

        ret_val = buf->getRange(0, 10);
        CPPUNIT_ASSERT(ret_val);
        CPPUNIT_ASSERT(!buf->extraBufs);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, buf->extraBufsUsed);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(reinterpret_cast<char*>(ret_val),
                                       testStr1, 10));

        ret_val = buf->getRange(11, 5);
        CPPUNIT_ASSERT(ret_val);
        CPPUNIT_ASSERT(!buf->extraBufs);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, buf->extraBufsUsed);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(reinterpret_cast<char*>(ret_val),
                                       testStr2+1, 9));
    }

    void test_getRange_copy() {
        void *ret_val;

        for (int i = 0; i < 15; ++i) {
            ret_val = buf->getRange(0, 14);
            CPPUNIT_ASSERT(buf->extraBufs);
            CPPUNIT_ASSERT_EQUAL((uint32_t) i+1, buf->extraBufsUsed);
            CPPUNIT_ASSERT_EQUAL(0, memcmp(reinterpret_cast<char*>(ret_val),
                                           testStr1, 10));
            CPPUNIT_ASSERT_EQUAL(0, memcmp(
                reinterpret_cast<char*>(ret_val) + 10, testStr2, 4));
        }

        ret_val = buf->getRange(45, 13);
        CPPUNIT_ASSERT(ret_val == NULL);
    }

    void test_copy_lengthZero() {
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, buf->copy(0, 0, cmpBuf));
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, buf->copy(10, 0, cmpBuf));
    }

    void test_copy_normal() {
        CPPUNIT_ASSERT_EQUAL((uint32_t) 10, buf->copy(0, 10, cmpBuf));
        CPPUNIT_ASSERT(!memcmp(cmpBuf, testStr1, 10));
        CPPUNIT_ASSERT_EQUAL((uint32_t) 5, buf->copy(10, 5, cmpBuf));
        CPPUNIT_ASSERT(!memcmp(cmpBuf, testStr2, 5));
        CPPUNIT_ASSERT_EQUAL((uint32_t) 5, buf->copy(12, 5, cmpBuf));
        CPPUNIT_ASSERT(!memcmp(cmpBuf, testStr2+2, 5));

        CPPUNIT_ASSERT_EQUAL((uint32_t) 30, buf->copy(0, 30, cmpBuf));
        CPPUNIT_ASSERT_EQUAL(0, strcmp(cmpBuf, testStr));
    }

    void test_copy_spanningChunks() {
        CPPUNIT_ASSERT_EQUAL((uint32_t) 30, buf->copy(0, 30, cmpBuf));
        CPPUNIT_ASSERT_EQUAL(0, strcmp(cmpBuf, testStr));
    }

    void test_copy_offsetGreaterThanTotalLength() {
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, buf->copy(40, 10, cmpBuf));
    }

    void test_findChunk_normal() {
        uint32_t chunkOffset;
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, buf->findChunk(6, &chunkOffset));
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, chunkOffset);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 1, buf->findChunk(12, &chunkOffset));
        CPPUNIT_ASSERT_EQUAL((uint32_t) 10, chunkOffset);
    }

    void test_findChunk_offsetGreaterThanTotalLength() {
        uint32_t chunkOffset;
        CPPUNIT_ASSERT_EQUAL(buf->chunksUsed, buf->findChunk(35, &chunkOffset));
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

    void test_allocateMoreExtraBufs() {
        Buffer b;
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.extraBufsUsed);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.extraBufsAvail);
        b.allocateMoreExtraBufs();
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.extraBufsUsed);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 10, b.extraBufsAvail);
        b.allocateMoreExtraBufs();
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.extraBufsUsed);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 20, b.extraBufsAvail);
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

    DISALLOW_COPY_AND_ASSIGN(BufferTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(BufferTest);

class BufferIteratorTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(BufferIteratorTest);
    CPPUNIT_TEST(test_normal);
    CPPUNIT_TEST(test_isDone);
    CPPUNIT_TEST(test_next);
    CPPUNIT_TEST(test_getData);
    CPPUNIT_TEST(test_getLength);
    CPPUNIT_TEST_SUITE_END();
    char x[30];

  public:
    void test_normal() {
        Buffer b;
        b.append(&x[0], 10);
        b.append(&x[10], 20);

        Buffer::Iterator iter(b);
        CPPUNIT_ASSERT(!iter.isDone());
        CPPUNIT_ASSERT(&x[0] == iter.getData());
        CPPUNIT_ASSERT(10 == iter.getLength());
        iter.next();
        CPPUNIT_ASSERT(!iter.isDone());
        CPPUNIT_ASSERT(&x[10] == iter.getData());
        CPPUNIT_ASSERT(20U == iter.getLength());
        iter.next();
        CPPUNIT_ASSERT(iter.isDone());
    }

    void test_isDone() {
        Buffer b;

        { // empty Buffer
            Buffer::Iterator iter(b);
            CPPUNIT_ASSERT(iter.isDone());
        }

        b.append(&x[0], 10);
        b.append(&x[10], 20);

        { // nonempty buffer
            Buffer::Iterator iter(b);
            CPPUNIT_ASSERT(!iter.isDone());
            iter.next();
            CPPUNIT_ASSERT(!iter.isDone());
            iter.next();
            CPPUNIT_ASSERT(iter.isDone());
        }
    }

    void test_next() {
        Buffer b;
        b.append(&x[0], 10);
        Buffer::Iterator iter(b);
        CPPUNIT_ASSERT(iter.chunkIndex == 0);
        iter.next();
        CPPUNIT_ASSERT(iter.chunkIndex == 1);
    }

    void test_getData() {
        Buffer b;
        b.append(&x[0], 10);
        Buffer::Iterator iter(b);
        CPPUNIT_ASSERT(iter.getData() == &x[0]);
    }

    void test_getLength() {
        Buffer b;
        b.append(&x[0], 10);
        Buffer::Iterator iter(b);
        CPPUNIT_ASSERT(iter.getLength() == 10);
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(BufferIteratorTest);

}  // namespace RAMCloud
