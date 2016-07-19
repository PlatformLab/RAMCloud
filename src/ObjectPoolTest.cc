/* Copyright (c) 2011 Stanford University
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
 * Unit tests for ObjectPool.
 */

#include "TestUtil.h"
#include "Common.h"
#include "ObjectPool.h"

namespace RAMCloud {
namespace {

class TestObject {
  public:
    explicit TestObject(bool throwException = false)
        : destroyed(NULL)
    {
        if (throwException)
            throw Exception(HERE, "Yes, me Lord.");
    }

    explicit TestObject(bool* destroyed)
        : destroyed(destroyed)
    {
    }

    ~TestObject()
    {
        if (destroyed)
            *destroyed = true;
    }

  private:
    bool* destroyed;

    DISALLOW_COPY_AND_ASSIGN(TestObject);
};
} //anonymous namespace

TEST(ObjectPoolTest, constructor) {
    ObjectPool<TestObject> pool;
    EXPECT_EQ(0U, pool.outstandingObjects);
}

TEST(ObjectPoolTest, destructor) {
    // shouldn't throw
    {
        ObjectPool<TestObject> pool;
        TestObject* a = pool.construct();
        TestObject* b = pool.construct();
        pool.destroy(a);
        pool.destroy(b);
    }

    {
        ObjectPool<TestObject> pool;
        TestObject* a = pool.construct();
        TestObject* b = pool.construct();
        pool.destroy(b);
        pool.destroy(a);
    }
}

TEST(ObjectPoolTest, destructor_objectsStillAllocated) {
    // should throw
    ObjectPool<TestObject>* pool = new ObjectPool<TestObject>();
    TestObject* a = pool->construct();
    (void)a;

    TestLog::Enable _;
    delete pool;
    EXPECT_EQ("~ObjectPool: Pool destroyed with 1 objects still outstanding!",
            TestLog::get());
}

TEST(ObjectPoolTest, construct) {
    ObjectPool<TestObject> pool;
    EXPECT_THROW(pool.construct(true), Exception);
    EXPECT_EQ(1U, pool.pool.size());
    TestObject*a = pool.construct();
    EXPECT_NE(static_cast<TestObject*>(NULL), a);
    EXPECT_EQ(1U, pool.outstandingObjects);
    pool.destroy(a);
}

TEST(ObjectPoolTests, destroy) {
    ObjectPool<TestObject> pool;
    bool destroyed = false;
    pool.destroy(pool.construct(&destroyed));
    EXPECT_TRUE(destroyed);
    EXPECT_EQ(0U, pool.outstandingObjects);
    EXPECT_EQ(1U, pool.pool.size());
}

TEST(ObjectPoolTests, destroy_inOrder) {
    ObjectPool<TestObject> pool;
    int count = 100;
    TestObject* toDestroy[count];

    for (int i = 0; i < count; i++)
        toDestroy[i] = pool.construct();
    for (int i = 0; i < count; i++)
        pool.destroy(toDestroy[i]);
}

TEST(ObjectPoolTests, destroy_reverseOrder) {
    ObjectPool<TestObject> pool;
    int count = 100;
    TestObject* toDestroy[count];

    for (int i = 0; i < count; i++)
        toDestroy[i] = pool.construct();
    for (int i = count - 1; i >= 0; i--)
        pool.destroy(toDestroy[i]);
}

TEST(ObjectPoolTests, destroy_randomOrder) {
    ObjectPool<TestObject> pool;
    int count = 100;
    TestObject* toDestroy[count];

    for (int i = 0; i < count; i++)
        toDestroy[i] = pool.construct();

    int destroyed = 0;
    while (destroyed < count) {
        int i = downCast<int>(generateRandom() % count);
        while (toDestroy[i] == NULL)
            i = (i + 1) % count;
        pool.destroy(toDestroy[i]);
        toDestroy[i] = NULL;
        destroyed++;
    }
}

} // namespace RAMCloud
