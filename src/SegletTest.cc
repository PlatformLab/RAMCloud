/* Copyright (c) 2012 Stanford University
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

#include "Seglet.h"
#include "SegletAllocator.h"
#include "ServerConfig.h"

namespace RAMCloud {

/**
 * Unit tests for Seglet.
 */
class SegletTest : public ::testing::Test {
  public:
    SegletTest()
        : serverConfig(ServerConfig::forTesting()),
          allocator(&serverConfig),
          buf(NULL),
          s(NULL)
    {
        vector<Seglet*> v;
        allocator.alloc(SegletAllocator::DEFAULT, 1, v);
        EXPECT_FALSE(v.empty());
        s = v[0];
        buf = s->buffer;
    }

    ~SegletTest()
    {
        if (s != NULL)
            s->free();
    }

    ServerConfig serverConfig;
    SegletAllocator allocator;
    void* buf;
    Seglet* s;

    DISALLOW_COPY_AND_ASSIGN(SegletTest);
};

TEST_F(SegletTest, constructor) {
    char buf[50];
    Seglet seglet(allocator, buf, sizeof(buf));
    EXPECT_EQ(&allocator, &seglet.segletAllocator);
    EXPECT_EQ(buf, seglet.buffer);
    EXPECT_EQ(sizeof(buf), seglet.length);
    EXPECT_EQ(static_cast<const vector<Seglet*>*>(NULL), seglet.sourcePool);
}

TEST_F(SegletTest, free) {
    s->free();
    EXPECT_EQ(allocator.defaultPool.back(), s);
    s = NULL;
}

TEST_F(SegletTest, get) {
    EXPECT_EQ(buf, s->get());
}

TEST_F(SegletTest, getLength) {
    EXPECT_EQ(serverConfig.segletSize, s->getLength());
}

TEST_F(SegletTest, setSourcePool) {
    const vector<Seglet*>* realPool = s->sourcePool;
    const vector<Seglet*>* p = reinterpret_cast<const vector<Seglet*>*>(83);
    s->setSourcePool(p);
    EXPECT_EQ(p, s->sourcePool);
    s->setSourcePool(realPool);
}

TEST_F(SegletTest, getSourcePool) {
    const vector<Seglet*>* realPool = s->sourcePool;
    const vector<Seglet*>* p = reinterpret_cast<const vector<Seglet*>*>(83);
    s->setSourcePool(p);
    EXPECT_EQ(p, s->getSourcePool());
    s->setSourcePool(realPool);
}

} // namespace RAMCloud
