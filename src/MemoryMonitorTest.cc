/* Copyright (c) 2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.xx
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <sys/mman.h>

#include "TestUtil.h"
#include "Logger.h"
#include "MemoryMonitor.h"

namespace RAMCloud {
class MemoryMonitorTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Dispatch dispatch;

    MemoryMonitorTest()
        : logEnabler()
        , dispatch(false)
    {
    }

    ~MemoryMonitorTest()
    {
        MemoryMonitor::statmFileContents = NULL;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(MemoryMonitorTest);
};

TEST_F(MemoryMonitorTest, handleTimerEvent) {
    MemoryMonitor monitor(&dispatch, 1.0, 5);

    // Output initial usage.
    monitor.handleTimerEvent();
    EXPECT_TRUE(TestUtil::matchesPosixRegex("Memory usage now .*increased",
            TestLog::get()));

    // Nothing has changed, so no output.
    TestLog::reset();
    monitor.handleTimerEvent();
    EXPECT_EQ("", TestLog::get());

    // Allocate a large block of memory and touch it to make sure physical
    // memory space has been allocated; then make sure another message
    // gets printed.
    size_t chunkSize = 16*1024*1024;
    char *chunk = static_cast<char*>(mmap(NULL, chunkSize,
            PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0));
    for (size_t i = 0; i < chunkSize; i += 1024) {
        chunk[i] = 'x';
    }
    TestLog::reset();
    monitor.handleTimerEvent();
    EXPECT_TRUE(TestUtil::matchesPosixRegex("Memory usage now .*increased",
            TestLog::get()));

    // Now free the chunk and make sure that memory usage drops.
    munmap(chunk, chunkSize);
    TestLog::reset();
    monitor.handleTimerEvent();
    EXPECT_TRUE(TestUtil::matchesPosixRegex("Memory usage now .*decreased",
            TestLog::get()));
}

TEST_F(MemoryMonitorTest, currentUsage_basics) {
    MemoryMonitor::statmFileContents = "0 2049 99 66";
    EXPECT_EQ(9, MemoryMonitor::currentUsage());
}
TEST_F(MemoryMonitorTest, currentUsage_unrecognizedFileFormat) {
    string message = "no exception";
    try {
        MemoryMonitor::statmFileContents = "asdf";
        MemoryMonitor::currentUsage();
    } catch (FatalError& e) {
        message = e.message;
    }
    EXPECT_EQ("Couldn't parse contents of /proc/self/statm: asdf",
            message);

    message = "no exception";
    try {
        MemoryMonitor::statmFileContents = "99 asdf";
        MemoryMonitor::currentUsage();
    } catch (FatalError& e) {
        message = e.message;
    }
    EXPECT_EQ("Couldn't parse contents of /proc/self/statm: 99 asdf",
            message);
}

}  // namespace RAMCloud
