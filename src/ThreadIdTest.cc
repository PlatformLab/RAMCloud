/* Copyright (c) 2011-2015 Stanford University
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
#include "ThreadId.h"

namespace RAMCloud {

class ThreadIdTest : public ::testing::Test {
  public:
    ThreadIdTest()
    {
        ThreadId::id = 0;
        ThreadId::highestId = 0;
    }
    DISALLOW_COPY_AND_ASSIGN(ThreadIdTest);
};

// Helper function that runs in a separate thread.  It reads its id and
// saves it in the variable pointed to by its argument.
static void readThreadId(int* p) {
    *p = ThreadId::get();
}

TEST_F(ThreadIdTest, basics) {
    int value;
    EXPECT_EQ(1, ThreadId::get());
    EXPECT_EQ(1, ThreadId::get());
    EXPECT_EQ(1, ThreadId::assign());
    std::thread thread1(readThreadId, &value);
    thread1.join();
    EXPECT_EQ(2, value);
    std::thread thread2(readThreadId, &value);
    thread2.join();
    EXPECT_EQ(3, value);
}

} // namespace RAMCloud
