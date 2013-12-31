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
 * Unit tests for LogSegment.
 */
class LogSegmentTest : public ::testing::Test {
  public:
    LogSegmentTest()
        : serverConfig(ServerConfig::forTesting()),
          allocator(&serverConfig),
          buf(new char[serverConfig.segletSize]),
          seglet(allocator, buf, sizeof(buf)),
          s()
    {
        vector<Seglet*> seglets;
        allocator.alloc(SegletAllocator::DEFAULT, 1, seglets);
        s.construct(seglets,
                    seglets[0]->getLength(),
                    seglets[0]->getLength(),
                    183,
                    38,
                    292,
                    false);
    }

    ~LogSegmentTest()
    {
        delete[] buf;
    }

    ServerConfig serverConfig;
    SegletAllocator allocator;
    char* buf;
    Seglet seglet;
    Tub<LogSegment> s;

    DISALLOW_COPY_AND_ASSIGN(LogSegmentTest);
};

TEST_F(LogSegmentTest, getMemoryUtilization) {
    EXPECT_EQ(0, s->getMemoryUtilization());
    s->entryLengths[LOG_ENTRY_TYPE_OBJ] =  3 * s->segletSize / 4;
    s->deadEntryLengths[LOG_ENTRY_TYPE_OBJ] = s->segletSize / 4;
    EXPECT_EQ(50, s->getMemoryUtilization());
}

TEST_F(LogSegmentTest, getDiskUtilization) {
    EXPECT_EQ(0, s->getDiskUtilization());
    s->entryLengths[LOG_ENTRY_TYPE_OBJ] = serverConfig.segmentSize / 2;
    EXPECT_EQ(50, s->getDiskUtilization());
}

} // namespace RAMCloud
