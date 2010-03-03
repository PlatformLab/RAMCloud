/* Copyright (c) 2009 Stanford University
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

// RAMCloud pragma [GCCWARN=5]

/**
 * \file
 * Unit tests for BufferPtr.
 */

#include <string.h>
#include <strings.h>

#include <BufferPtr.h>

#include <cppunit/extensions/HelperMacros.h>

namespace RAMCloud {

class BufferPtrTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(BufferPtrTest);
    CPPUNIT_TEST(test_prepend);
    CPPUNIT_TEST(test_append);
    CPPUNIT_TEST(test_read);
    CPPUNIT_TEST(test_copy);
    CPPUNIT_TEST(test_overwrite);
    CPPUNIT_TEST(test_totalLength);
    CPPUNIT_TEST_SUITE_END();

  public:
    BufferPtrTest() { }

    void setUp() { }

    void tearDown() { }

    void test_prepend() {
        BufferPtr *bp = new BufferPtr();
        uint8_t *buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);

        // Test input edge cases.
        CPPUNIT_ASSERT(bp->prepend((void*) buf, 0) == false);
        CPPUNIT_ASSERT(bp->prepend(NULL, 10) == false);
        
        CPPUNIT_ASSERT(bp->prepend(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->prepend(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->prepend(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->prepend(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->prepend(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->prepend(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->prepend(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->prepend(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->prepend(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->prepend(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->prepend(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->prepend(buf, 10));

        uint8_t* ret_buf = (uint8_t *) xmalloc(sizeof(uint8_t) * 120);
        CPPUNIT_ASSERT_EQUAL(120, (int) bp->copy(0, 120, (void*) ret_buf));
    }

    void test_append() {
        BufferPtr *bp = new BufferPtr();
        uint8_t *buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);

        // Test input edge cases.
        CPPUNIT_ASSERT(bp->append((void*) buf, 0) == false);
        CPPUNIT_ASSERT(bp->append(NULL, 10) == false);
        
        CPPUNIT_ASSERT(bp->append(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->append(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->append(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->append(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->append(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->append(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->append(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->append(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->append(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->append(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->append(buf, 10));
        buf = (uint8_t*) xmalloc(sizeof(uint8_t) * 10);
        bzero(buf, 10);
        CPPUNIT_ASSERT(bp->append(buf, 10));

        uint8_t* ret_buf = (uint8_t *) xmalloc(sizeof(uint8_t) * 120);
        CPPUNIT_ASSERT_EQUAL(120, (int) bp->copy(0, 120, (void*) ret_buf));
    }

    void test_read() {
        BufferPtr *bp = new BufferPtr();
        uint8_t* buf1 = (uint8_t *) xmalloc(sizeof(uint8_t) * 10);
        uint8_t* buf2 = (uint8_t *) xmalloc(sizeof(uint8_t) * 10);

        memcpy(buf1, "012345678\0", 10);
        memcpy(buf2, "987654321\0", 10);

        CPPUNIT_ASSERT(bp->append(buf1, 10));
        CPPUNIT_ASSERT(bp->append(buf2, 10));

        uint8_t* ret_buf;

        CPPUNIT_ASSERT_EQUAL(0, (int) bp->read(40, 10, (void **) &ret_buf));

        CPPUNIT_ASSERT_EQUAL((int) bp->read(0, 10, (void**) &ret_buf), 10);
        CPPUNIT_ASSERT_EQUAL(memcmp(ret_buf, buf1, 10), 0);

        CPPUNIT_ASSERT_EQUAL(10, (int) bp->read(10, 10, (void**) &ret_buf));
        CPPUNIT_ASSERT_EQUAL(memcmp(ret_buf, buf2, 10), 0);

        CPPUNIT_ASSERT_EQUAL(5, (int) bp->read(5, 10, (void**) &ret_buf));
        CPPUNIT_ASSERT_EQUAL(memcmp(ret_buf, buf1+5, 5), 0);
    }

    void test_copy() {
        BufferPtr *bp = new BufferPtr();
        uint8_t* buf1 = (uint8_t *) xmalloc(sizeof(uint8_t) * 10);
        uint8_t* buf2 = (uint8_t *) xmalloc(sizeof(uint8_t) * 10);

        memcpy(buf1, "0123456789", 10);
        memcpy(buf2, "9876543210", 10);

        CPPUNIT_ASSERT(bp->append(buf1, 10));
        CPPUNIT_ASSERT(bp->append(buf2, 10));

        uint8_t* ret_buf = (uint8_t *) xmalloc(sizeof(uint8_t) * 10);

        CPPUNIT_ASSERT_EQUAL(0, (int) bp->copy(50, 10, (void*) ret_buf));

        CPPUNIT_ASSERT_EQUAL(10, (int) bp->copy(0, 10, (void*) ret_buf));
        CPPUNIT_ASSERT_EQUAL(memcmp(ret_buf, buf1, 10), 0);

        CPPUNIT_ASSERT_EQUAL(10, (int) bp->copy(10, 10, (void*) ret_buf));
        CPPUNIT_ASSERT_EQUAL(memcmp(ret_buf, buf2, 10), 0);

        CPPUNIT_ASSERT_EQUAL(10, (int) bp->copy(5, 10, (void*) ret_buf));
        CPPUNIT_ASSERT_EQUAL(memcmp(ret_buf, buf1+5, 5), 0);
    }

    void test_overwrite() {
        BufferPtr *bp = new BufferPtr();
        uint8_t* buf1 = (uint8_t *) xmalloc(sizeof(uint8_t) * 100);
        bzero(buf1, 100);
        CPPUNIT_ASSERT(bp->append(buf1, 100));
        
        uint8_t* buf2 = (uint8_t *) xmalloc(sizeof(uint8_t) * 100);

        for (int i = 0; i < 10; ++i) {
            memcpy(buf1 + (i*10), "0123456789", 10);
        }

        CPPUNIT_ASSERT_EQUAL(0, (int) bp->overwrite(buf1, 10, 400));

        CPPUNIT_ASSERT_EQUAL(100, (int) bp->overwrite(buf1, 100, 0));

        uint8_t* ret_buf = (uint8_t *) xmalloc(sizeof(uint8_t) * 100);

        CPPUNIT_ASSERT_EQUAL(100, (int) bp->copy(0, 100, ret_buf));
        CPPUNIT_ASSERT_EQUAL(0, memcmp(ret_buf, buf1, 100));

        CPPUNIT_ASSERT_EQUAL(10, (int) bp->overwrite(buf1, 10, 10));
        CPPUNIT_ASSERT_EQUAL(10, (int) bp->copy(10, 10, ret_buf));
        CPPUNIT_ASSERT_EQUAL(0, memcmp(ret_buf, buf1, 10));
    }

    void test_totalLength() {
        BufferPtr *bp = new BufferPtr();
        uint8_t* buf1 = (uint8_t *) xmalloc(sizeof(uint8_t) * 100);
        bzero(buf1, 100);

        CPPUNIT_ASSERT(bp->prepend(buf1, 100));
        CPPUNIT_ASSERT(bp->append(buf1, 10));

        CPPUNIT_ASSERT_EQUAL(110, (int) bp->totalLength());
    }
        
};


CPPUNIT_TEST_SUITE_REGISTRATION(BufferPtrTest);

}  // namespace RAMCloud
