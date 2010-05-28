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
 * Unit tests for RAMCloud::Buffer.
 */

#include <string.h>
#include <strings.h>

#include <Buffer.h>

#include <cppunit/extensions/HelperMacros.h>

namespace RAMCloud {

class BufferTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(BufferTest);

    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_constructor_withParams);
    CPPUNIT_TEST(test_destructor);

    CPPUNIT_TEST(test_prepend);
    CPPUNIT_TEST(test_append);

    CPPUNIT_TEST(test_peek_normal);
    CPPUNIT_TEST(test_peek_offsetGreaterThanTotalLength);

    CPPUNIT_TEST(test_internal_copy);
    CPPUNIT_TEST(test_allocateScratchRange);

    CPPUNIT_TEST(test_getRange_inputEdgeCases);
    CPPUNIT_TEST(test_getRange_peek);
    CPPUNIT_TEST(test_getRange_copy);

    CPPUNIT_TEST(test_copy_noop);
    CPPUNIT_TEST(test_copy_normal);

    CPPUNIT_TEST_SUITE_END();

    /**
     * See #assertBufferEquals().
     */
    struct ComparisonChunk {
        void *data;
        uint32_t length;
    };

    /**
     * Asserts that a Buffer has the same contents as an array of pointer, length
     * pairs.
     * Also checks the buffer's number of chunks and total length fields.
     * \param buffer    The Buffer to test against \a chunks.
     * \param chunks    An array of data pointer, length pairs.
     * \param numChunks The number of elements in the \a chunks array.
     */
    void assertBufferEquals(const Buffer* buffer, ComparisonChunk* chunks,
                            uint32_t numChunks) {
        CPPUNIT_ASSERT(numChunks == buffer->numberChunks);
        uint32_t totalLength = 0;
        uint32_t i = 0;
        Buffer::Chunk* current = buffer->chunks;
        while (i < numChunks) {
            CPPUNIT_ASSERT(current != NULL);
            CPPUNIT_ASSERT_EQUAL(chunks[i].data, current->data);
            CPPUNIT_ASSERT_EQUAL(chunks[i].length, current->length);
            totalLength += chunks[i].length;
            ++i;
            current = current->next;
        }
        CPPUNIT_ASSERT(current == NULL);
        CPPUNIT_ASSERT_EQUAL(totalLength, buffer->totalLength);
    }

    // I've inserted padding in between these arrays so that we don't get lucky
    // by going past end of testStr1 and hitting testStr2, etc.
    char testStr[30];
    char pad1[50];
    char testStr1[10];
    char pad2[50];
    char testStr2[10];
    char pad3[50];
    char testStr3[10];
    char pad4[50];
    char cmpBuf[30];    // To use for strcmp at the end of a test.
    Buffer *buf;

  public:
    BufferTest() : buf(NULL) {
        memcpy(testStr, "0123456789abcdefghijklmnopqrs\0", 30);
        memcpy(testStr1, "0123456789", 10);
        memcpy(testStr2, "abcdefghij", 10);
        memcpy(testStr3, "klmnopqrs\0", 10);
        memset(pad1, 0xcc, sizeof(pad1));
        memset(pad2, 0xdd, sizeof(pad1));
        memset(pad3, 0xee, sizeof(pad1));
        memset(pad4, 0xff, sizeof(pad1));
    }

    void setUp() {
        // This uses prepend, so the tests for prepend
        // probably shouldn't use this.
        buf = new Buffer();
        buf->prepend(testStr3, 10);
        buf->prepend(testStr2, 10);
        buf->prepend(testStr1, 10);
    }

    void tearDown() { delete buf; }

    void test_constructor() {
        // Basic sanity checks for the constructor.
        Buffer b;
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.totalLength);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.numberChunks);
        CPPUNIT_ASSERT(b.chunks == NULL);
        CPPUNIT_ASSERT(b.scratchRanges == NULL);
    }

    void test_constructor_withParams() {
        Buffer b(testStr1, 10);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 10, b.totalLength);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 1, b.numberChunks);
        CPPUNIT_ASSERT(b.chunks != NULL);
        CPPUNIT_ASSERT(b.scratchRanges == NULL);
        CPPUNIT_ASSERT(b.chunks->next == NULL);
        CPPUNIT_ASSERT(b.chunks->data == testStr1);
        CPPUNIT_ASSERT(b.chunks->length == 10);
    }

    void test_destructor() {
        Buffer b(testStr1, 10);
        b.prepend(testStr1, 5);
        b.getRange(0, 15);
        b.~Buffer();
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.totalLength);
        CPPUNIT_ASSERT_EQUAL((uint32_t) 0, b.numberChunks);
        CPPUNIT_ASSERT(b.chunks == NULL);
        CPPUNIT_ASSERT(b.scratchRanges == NULL);
    }

    void test_prepend() {
        Buffer b;
        b.prepend(NULL, 0);
        b.prepend(testStr3, 10);
        b.prepend(testStr2, 10);
        b.prepend(testStr1, 10);

        ComparisonChunk exp[] = {
            {testStr1, 10},
            {testStr2, 10},
            {testStr3, 10},
            {NULL, 0},
        };
        assertBufferEquals(&b, exp, 4);
    }

    void test_append() {
        Buffer b;
        b.append(NULL, 0);
        b.append(testStr1, 10);
        b.append(testStr2, 10);
        b.append(testStr3, 10);

        ComparisonChunk exp[] = {
            {NULL, 0},
            {testStr1, 10},
            {testStr2, 10},
            {testStr3, 10},
        };
        assertBufferEquals(&b, exp, 4);
    }

    void test_peek_normal() {
        void *ret_val;
        CPPUNIT_ASSERT(10 == buf->peek(0, &ret_val));
        CPPUNIT_ASSERT(testStr1 == ret_val);
        CPPUNIT_ASSERT(1 == buf->peek(9, &ret_val));
        CPPUNIT_ASSERT(testStr1 + 9 == ret_val);
        CPPUNIT_ASSERT(10 == buf->peek(10, &ret_val));
        CPPUNIT_ASSERT(testStr2 == ret_val);
        CPPUNIT_ASSERT(5 == buf->peek(25, &ret_val));
        CPPUNIT_ASSERT(testStr3 + 5 == ret_val);
    }

    void test_peek_offsetGreaterThanTotalLength() {
        void *ret_val;
        CPPUNIT_ASSERT(0 == buf->peek(30, &ret_val));
        CPPUNIT_ASSERT(NULL == ret_val);
        CPPUNIT_ASSERT(0 == buf->peek(31, &ret_val));
        CPPUNIT_ASSERT(NULL == ret_val);
    }

    void test_internal_copy() {
        char exp[32];
        char scratch[32];
        memset(exp, 0xcc, sizeof(exp));
        memset(scratch, 0xcc, sizeof(scratch));

        // skip while loop
        buf->copy(buf->chunks, 0, 0, scratch + 1);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(exp, scratch, sizeof(exp)));
        memset(exp, 0xcc, sizeof(exp));
        memset(scratch, 0xcc, sizeof(scratch));

        // bytesToCopy > length on last chunk
        buf->copy(buf->chunks, 0, 29, scratch + 1);
        memcpy(exp + 1, testStr, 29);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(exp, scratch, sizeof(exp)));
        memset(exp, 0xcc, sizeof(exp));
        memset(scratch, 0xcc, sizeof(scratch));

        // nonzero offset on first chunk
        // also ends at exactly the end of the buffer
        buf->copy(buf->chunks, 1, 29, scratch + 1);
        memcpy(exp + 1, testStr + 1, 29);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(exp, scratch, sizeof(exp)));
        memset(exp, 0xcc, sizeof(exp));
        memset(scratch, 0xcc, sizeof(scratch));
    }

    void test_allocateScratchRange() {
        typedef Buffer::ScratchRange ScratchRange;
        Buffer b;

        void* r2 = b.allocateScratchRange(3);
        ScratchRange* cr2 = static_cast<ScratchRange*>(r2) - 1;
        CPPUNIT_ASSERT_EQUAL(b.scratchRanges, cr2);

        void* r1 = b.allocateScratchRange(4);
        ScratchRange* cr1 = static_cast<ScratchRange*>(r1) - 1;
        CPPUNIT_ASSERT_EQUAL(b.scratchRanges, cr1);

        CPPUNIT_ASSERT_EQUAL(cr2, cr1->next);
        CPPUNIT_ASSERT(NULL == cr2->next);
    }

    void test_getRange_inputEdgeCases() {
        CPPUNIT_ASSERT(NULL == buf->getRange(0, 0));
        CPPUNIT_ASSERT(NULL == buf->getRange(30, 1));
        CPPUNIT_ASSERT(NULL == buf->getRange(29, 2));
    }

    void test_getRange_peek() {
        CPPUNIT_ASSERT(testStr1 == buf->getRange(0, 10));
        CPPUNIT_ASSERT(testStr1 + 3 == buf->getRange(3, 2));
        CPPUNIT_ASSERT(testStr2 == buf->getRange(10, 10));
        CPPUNIT_ASSERT(testStr2 + 1 == buf->getRange(11, 5));
        CPPUNIT_ASSERT(testStr3 == buf->getRange(20, 1));
        CPPUNIT_ASSERT(testStr3 + 9 == buf->getRange(29, 1));
        CPPUNIT_ASSERT(NULL == buf->scratchRanges);
    }

    void test_getRange_copy() {
        CPPUNIT_ASSERT(0 == strncmp(testStr + 9,
                                    static_cast<char*>(buf->getRange(9, 2)),
                                    2));
        CPPUNIT_ASSERT(NULL != buf->scratchRanges);
    }

    void test_copy_noop() {
        Buffer b;
        CPPUNIT_ASSERT(0 == b.copy(0, 0, cmpBuf));
        CPPUNIT_ASSERT(0 == b.copy(1, 0, cmpBuf));
        CPPUNIT_ASSERT(0 == b.copy(1, 1, cmpBuf));
        CPPUNIT_ASSERT(0 == buf->copy(30, 0, cmpBuf));
        CPPUNIT_ASSERT(0 == buf->copy(30, 1, cmpBuf));
        CPPUNIT_ASSERT(0 == buf->copy(31, 1, cmpBuf));
    }

    void test_copy_normal() {
        char exp[32];
        char scratch[32];
        memset(exp, 0xcc, sizeof(exp));
        memset(scratch, 0xcc, sizeof(scratch));

        // skip while loop (0 offset)
        // offset + length == totalLength
        CPPUNIT_ASSERT(30 == buf->copy(0, 30, scratch + 1));
        memcpy(exp + 1, testStr, 30);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(exp, scratch, sizeof(exp)));
        memset(exp, 0xcc, sizeof(exp));
        memset(scratch, 0xcc, sizeof(scratch));

        // offset + length > totalLength
        // non-zero offset, first chunk
        // also ends at exactly the end of the buffer
        CPPUNIT_ASSERT(30 == buf->copy(0, 40, scratch + 1));
        memcpy(exp + 1, testStr, 30);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(exp, scratch, sizeof(exp)));
        memset(exp, 0xcc, sizeof(exp));
        memset(scratch, 0xcc, sizeof(scratch));

        // non-zero offset, last chunk
        CPPUNIT_ASSERT(8 == buf->copy(21, 8, scratch + 1));
        memcpy(exp + 1, testStr + 21, 8);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(exp, scratch, sizeof(exp)));
        memset(exp, 0xcc, sizeof(exp));
        memset(scratch, 0xcc, sizeof(scratch));
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
        CPPUNIT_ASSERT(iter.current == b.chunks);
        iter.next();
        CPPUNIT_ASSERT(iter.current == b.chunks->next);
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
