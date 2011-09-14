/* Copyright (c) 2011 Facebook
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
    EXPECT_TRUE(Context::get().logger != NULL);
    EXPECT_TRUE(Context::get().dispatch != NULL);
    EXPECT_TRUE(Context::get().transportManager != NULL);
    EXPECT_TRUE(Context::get().serviceManager != NULL);
    mockContextMemberThrowException = 2;
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

TEST(Context, isSet) {
    // the test runner set up our current context
    EXPECT_TRUE(Context::isSet());
    Context::currentContext = NULL;
    EXPECT_FALSE(Context::isSet());
}

TEST(Context, friendlyGet) {
    Context::currentContext = NULL;
    EXPECT_THROW(Context::get(), FatalError);
}

TEST(Context, guard) {
    Context& outer = Context::get(); // the test runner set this one up
    Context inner(false);
    {
        Context::Guard g(inner);
        EXPECT_EQ(&inner, &Context::get());
    }
    EXPECT_EQ(&outer, &Context::get());
    {
        Context::Guard g(inner);
        EXPECT_EQ(&inner, &Context::get());
        g.leave();
        EXPECT_EQ(&outer, &Context::get());
    }
    EXPECT_EQ(&outer, &Context::get());
}

}  // namespace RAMCloud
