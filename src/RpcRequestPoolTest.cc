/* Copyright (c) 2014-2016 Stanford University
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
#include "RpcRequestPool.h"
#include "Memory.h"

namespace RAMCloud {

class RpcRequestPoolTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    RpcRequestPool pool;

    RpcRequestPoolTest()
        : logEnabler()
        , pool()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(RpcRequestPoolTest);
};

TEST_F(RpcRequestPoolTest, checkConstructor) {
    EXPECT_NE(0UL, (uint64_t)pool.basePointer);
    EXPECT_EQ(RpcRequestPool::SLABS_TOTAL, (int32_t)pool.freelist.size());
    EXPECT_NE(0, pool.ownerThreadId);
    EXPECT_TRUE(pool.freelistForAlienThreads.empty());

    char* lastPtr = pool.basePointer + RpcRequestPool::BYTES_TOTAL;
    while (!pool.freelist.empty()) {
        char* ptr = reinterpret_cast<char*>(pool.freelist.top());

        // Check alignment of entries
        uint64_t off = (uint64_t)ptr & (RpcRequestPool::BYTES_PER_REQUEST - 1);
        EXPECT_EQ(0UL, off);

        // Check size.
        EXPECT_EQ(RpcRequestPool::BYTES_PER_REQUEST, lastPtr - ptr);
        lastPtr = ptr;
        pool.freelist.pop();
    }
}

TEST_F(RpcRequestPoolTest, allocSmall) {
    // Check allignment of entries
    char* lastPtr = pool.basePointer + RpcRequestPool::BYTES_TOTAL;
    int count = 0;
    while (!pool.freelist.empty()) {
        int size = static_cast<int>(generateRandom()) % pool.BYTES_PER_REQUEST
                    + 1;
        char* ptr = reinterpret_cast<char*>(pool.alloc(size));

        // Check alignment of returned address.
        uint64_t off = (uint64_t)ptr & (RpcRequestPool::BYTES_PER_REQUEST - 1);
        EXPECT_EQ(0UL, off);

        // Check size of aligned memory.
        EXPECT_EQ(RpcRequestPool::BYTES_PER_REQUEST, lastPtr - ptr);
        lastPtr = ptr;
        count++;
    }
    EXPECT_EQ(RpcRequestPool::SLABS_TOTAL, count);
}

TEST_F(RpcRequestPoolTest, allocBig) {
    void* ptr = pool.alloc(RpcRequestPool::BYTES_PER_REQUEST + 1);
    EXPECT_NE(0UL, (uint64_t)ptr);
    EXPECT_EQ(RpcRequestPool::SLABS_TOTAL, (int32_t)pool.freelist.size());
    EXPECT_TRUE(ptr < pool.basePointer ||
                ptr >= pool.basePointer + pool.BYTES_TOTAL);
}

static void freePointers(RpcRequestPool* pool, void* ptr1, void* ptr2) {
    pool->free(ptr1);
    pool->free(ptr2);
}

TEST_F(RpcRequestPoolTest, allocComplex) {
    void* ptr1 = pool.alloc(1);
    void* ptr2 = pool.alloc(1);
    std::thread thread(freePointers, &pool, ptr1, ptr2);
    thread.join();

    EXPECT_EQ(pool.SLABS_TOTAL - 2, (int32_t)pool.freelist.size());
    EXPECT_EQ(2, (int32_t)pool.freelistForAlienThreads.size());

    // Now drain freelist entirely.
    int allocCount = 0;
    while (!pool.freelist.empty()) {
        pool.alloc(1);
        allocCount++;
    }
    EXPECT_EQ(pool.SLABS_TOTAL - 2, allocCount);

    // 1. Now try alloc which will invoke the swap of the two freelist stacks.
    void* ptr3 = pool.alloc(1);
    EXPECT_EQ(ptr2, ptr3);
    EXPECT_EQ(1, (int32_t)pool.freelist.size());
    EXPECT_EQ(0, (int32_t)pool.freelistForAlienThreads.size());

    // Alloc once more..
    void* ptr4 = pool.alloc(1);
    EXPECT_EQ(ptr1, ptr4);
    EXPECT_EQ(0, (int32_t)pool.freelist.size());
    EXPECT_EQ(0, (int32_t)pool.freelistForAlienThreads.size());

    // 2. Now freelist is empty. It calls POSIX malloc.
    void* ptr5 = pool.alloc(1);
    EXPECT_TRUE(ptr5 < pool.basePointer ||
                ptr5 >= pool.basePointer + pool.BYTES_TOTAL);
    pool.free(ptr5);
}

TEST_F(RpcRequestPoolTest, freeFromOwnerThread) {
    void* ptr1 = pool.alloc(1);
    void* ptr2 = pool.alloc(1);
    freePointers(&pool, ptr1, ptr2);
    EXPECT_EQ(pool.SLABS_TOTAL, (int32_t)pool.freelist.size());
    EXPECT_EQ(0, (int32_t)pool.freelistForAlienThreads.size());
    void* ptr3 = pool.alloc(1);
    void* ptr4 = pool.alloc(1);
    EXPECT_EQ(ptr2, ptr3);
    EXPECT_EQ(ptr1, ptr4);
    EXPECT_EQ(pool.SLABS_TOTAL - 2, (int32_t)pool.freelist.size());
}

TEST_F(RpcRequestPoolTest, freeMallocedMemory) {
    void* mallocedPtr = Memory::xmalloc(HERE, 1);
    pool.free(mallocedPtr);
    EXPECT_EQ(pool.SLABS_TOTAL, (int32_t)pool.freelist.size());
    EXPECT_EQ(0, (int32_t)pool.freelistForAlienThreads.size());
}

TEST_F(RpcRequestPoolTest, freeFromAlienThread) {
    void* ptr1 = pool.alloc(1);
    void* ptr2 = pool.alloc(1);
    std::thread thread(freePointers, &pool, ptr1, ptr2);
    thread.join();

    EXPECT_EQ(pool.SLABS_TOTAL - 2, (int32_t)pool.freelist.size());
    EXPECT_EQ(2, (int32_t)pool.freelistForAlienThreads.size());
}

}  // namespace RAMCloud
