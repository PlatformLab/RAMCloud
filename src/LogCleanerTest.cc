/* Copyright (c) 2010-2012 Stanford University
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
 * Unit tests for LogCleaner.
 */

#include "TestUtil.h"

#include "Segment.h"
#include "SegmentIterator.h"
#include "ServerId.h"
#include "Log.h"
#include "LogEntryTypes.h"
#include "LogCleaner.h"

namespace RAMCloud {

/**
 * Unit tests for LogCleaner.
 */
class LogCleanerTest : public ::testing::Test {
  public:
    LogCleanerTest()
        : context()
        , serverId(5, 23)
    {
    }

    Context context;
    ServerId serverId;

  private:
    DISALLOW_COPY_AND_ASSIGN(LogCleanerTest);
};

TEST_F(LogCleanerTest, constructor) {
}

} // namespace RAMCloud
