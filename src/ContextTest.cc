/* Copyright (c) 2011 Facebook
 * Copyright (c) 2011-2015 Stanford University
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

namespace RAMCloud {

// See definition in Context.cc
extern int mockContextMemberThrowException;

TEST(Context, constructor) {
    TestLog::Enable _;
    Context context;
    EXPECT_TRUE(context.serverList == NULL);
    EXPECT_TRUE(context.dispatch != NULL);
    EXPECT_TRUE(context.transportManager != NULL);
    EXPECT_TRUE(context.workerManager == NULL);
    EXPECT_TRUE(context.coordinatorSession != NULL);
    EXPECT_TRUE(context.timeTrace != NULL);
    mockContextMemberThrowException = 2;
    TestLog::reset();
    EXPECT_THROW(Context inner(false), Exception);
    EXPECT_EQ("MockContextMember: 1 | "
              "MockContextMember: 2 | "
              "~MockContextMember: 1",
              TestLog::get());
}

TEST(Context, destructor) {
    TestLog::Enable _;
    {
        Context inner(false);
    }
    EXPECT_EQ("MockContextMember: 1 | "
              "MockContextMember: 2 | "
              "~MockContextMember: 2 | "
              "~MockContextMember: 1",
              TestLog::get());
}

}  // namespace RAMCloud
