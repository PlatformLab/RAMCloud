/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.lie
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "Pool.h"
#include "TestUtil.h"

namespace RAMCloud {

class PoolTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(PoolTest);
    CPPUNIT_TEST(test_addAllocation);
    CPPUNIT_TEST(test_addAllocation_indivisible);
    CPPUNIT_TEST(test_allocate_empty);
    CPPUNIT_TEST(test_allocate);
    CPPUNIT_TEST(test_free);
    CPPUNIT_TEST(test_getFreeBlockCount);
    CPPUNIT_TEST(test_new);
    CPPUNIT_TEST(test_new_blockTooSmall);
    CPPUNIT_TEST(test_new_array);
    CPPUNIT_TEST(test_new_array_blockTooSmall);
    CPPUNIT_TEST_SUITE_END();

  public:
    static const uint64_t poolSize = 512;
    static const uint64_t blockSize = 256;
    char memory[poolSize];

    PoolTest()
    {
    }

    void
    test_addAllocation()
    {
        Pool pool(blockSize);
        pool.addAllocation(&memory[0], poolSize);
        CPPUNIT_ASSERT_EQUAL(&memory[blockSize], pool.allocate());
        CPPUNIT_ASSERT_EQUAL(&memory[0], pool.allocate());
        CPPUNIT_ASSERT_EQUAL(NULL, pool.allocate());
    }

    void
    test_addAllocation_indivisible()
    {
        Pool pool(blockSize);
        pool.addAllocation(&memory[0], poolSize - 1);
        CPPUNIT_ASSERT_EQUAL(&memory[0], pool.allocate());
        CPPUNIT_ASSERT_EQUAL(NULL, pool.allocate());
    }

    void
    test_allocate_empty()
    {
        Pool pool(blockSize);
        CPPUNIT_ASSERT_EQUAL(NULL, pool.allocate());
    }

    void
    test_allocate()
    {
        Pool pool(blockSize, &memory[0], poolSize);
        CPPUNIT_ASSERT_EQUAL(&memory[blockSize], pool.allocate());
    }

    void
    test_free()
    {
        Pool pool(blockSize, &memory[0], poolSize);
        void* block = pool.allocate();
        pool.free(block);
        CPPUNIT_ASSERT_EQUAL(&memory[blockSize], pool.allocate());
    }

    void
    test_getFreeBlockCount()
    {
        CPPUNIT_ASSERT_EQUAL(2, Pool(blockSize,
                                     &memory[0],
                                     poolSize).getFreeBlockCount());
    }

    struct Magic {
        Magic() : magic(0x123456789abcedf0) {}
        uint64_t magic;
    };

    void
    test_new()
    {
        Pool pool(blockSize, &memory[0], poolSize);
        Magic* m = new(&pool) Magic();
        CPPUNIT_ASSERT_EQUAL(0x123456789abcedf0, m->magic);
    }

    void
    test_new_blockTooSmall()
    {
        Pool pool(4, &memory[0], poolSize);
        CPPUNIT_ASSERT_THROW(new(&pool) Magic(),
                             std::bad_alloc);
    }

    void
    test_new_array()
    {
        Pool pool(blockSize, &memory[0], poolSize);
        Magic* ms = new(&pool) Magic[2];
        CPPUNIT_ASSERT_EQUAL(0x123456789abcedf0, ms[0].magic);
        CPPUNIT_ASSERT_EQUAL(0x123456789abcedf0, ms[1].magic);
    }

    void
    test_new_array_blockTooSmall()
    {
        Pool pool(blockSize, &memory[0], poolSize);
        CPPUNIT_ASSERT_THROW(new(&pool) char[blockSize + 1],
                             std::bad_alloc);
    }

    DISALLOW_COPY_AND_ASSIGN(PoolTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(PoolTest);

class GrowablePoolTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(GrowablePoolTest);
    CPPUNIT_TEST(test_allocate);
    CPPUNIT_TEST(test_grow);
    CPPUNIT_TEST_SUITE_END();

  public:
    static const uint64_t allocationSize = 512;
    static const uint64_t blockSize = 256;

    GrowablePoolTest()
    {
    }

    void
    test_allocate()
    {
        AutoPool pool(blockSize, allocationSize);
        CPPUNIT_ASSERT(pool.allocate());
        CPPUNIT_ASSERT(pool.allocate());
        CPPUNIT_ASSERT(pool.allocate());
    }

    void
    test_grow()
    {
        AutoPool pool(blockSize, allocationSize);
        CPPUNIT_ASSERT_EQUAL(0, pool.allocations.size());
        CPPUNIT_ASSERT_EQUAL(0, pool.getFreeBlockCount());
        pool.grow();
        CPPUNIT_ASSERT_EQUAL(1, pool.allocations.size());
        CPPUNIT_ASSERT_EQUAL(2, pool.getFreeBlockCount());
    }

    DISALLOW_COPY_AND_ASSIGN(GrowablePoolTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(GrowablePoolTest);

}  // namespace RAMCloud
