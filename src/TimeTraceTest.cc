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

#include "TestUtil.h"
#include "Logger.h"
#include "TimeTrace.h"

namespace RAMCloud {
class TimeTraceTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    TimeTrace trace;

    TimeTraceTest()
        : logEnabler()
        , trace()
    {
        Cycles::mockCyclesPerSec = 2e09;
    }

    ~TimeTraceTest()
    {
        Cycles::mockCyclesPerSec = 0;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(TimeTraceTest);
};

TEST_F(TimeTraceTest, constructor) {
    EXPECT_EQ(0, trace.events[0].message);
    EXPECT_EQ(0, trace.events[0].message);
}

TEST_F(TimeTraceTest, record_basics) {
    trace.record("point a", 100);
    trace.record("point b", 200);
    trace.record("point c", 350);
    EXPECT_EQ("     0.0 ns (+   0.0 ns): point a\n"
            "    50.0 ns (+  50.0 ns): point b\n"
            "   125.0 ns (+  75.0 ns): point c",
            trace.getTrace());
}

TEST_F(TimeTraceTest, record_readerActive) {
    trace.readerActive = true;
    trace.record("point a", 100);
    trace.record("point b", 200);
    trace.readerActive = false;
    trace.record("point c", 350);
    EXPECT_EQ("     0.0 ns (+   0.0 ns): point c",
            trace.getTrace());
}

TEST_F(TimeTraceTest, record_wrapAround) {
    trace.nextIndex = TimeTrace::BUFFER_SIZE - 2;
    trace.record("near the end", 100);
    trace.record("at the end", 200);
    trace.record("beginning", 350);
    EXPECT_EQ(1, trace.nextIndex);
    trace.nextIndex = TimeTrace::BUFFER_SIZE - 2;
    EXPECT_EQ("     0.0 ns (+   0.0 ns): near the end\n"
            "    50.0 ns (+  50.0 ns): at the end\n"
            "   125.0 ns (+  75.0 ns): beginning",
            trace.getTrace());
}

TEST_F(TimeTraceTest, getTrace) {
    trace.record("point a", 100);
    EXPECT_EQ("     0.0 ns (+   0.0 ns): point a",
            trace.getTrace());
}

TEST_F(TimeTraceTest, printToLog) {
    trace.record("point a", 100);
    trace.printToLog();
    EXPECT_EQ("printInternal:      0.0 ns (+   0.0 ns): point a",
            TestLog::get());
}

TEST_F(TimeTraceTest, printInternal_emptyTrace_stringVersion) {
    trace.nextIndex = 0;
    EXPECT_EQ("No time trace events to print", trace.getTrace());
}
TEST_F(TimeTraceTest, printInternal_emptyTrace_logVersion) {
    trace.nextIndex = 0;
    trace.printInternal(NULL);
    EXPECT_EQ("printInternal: No time trace events to print", TestLog::get());
}
TEST_F(TimeTraceTest, printInternal_startAtBeginning) {
    trace.record("point a", 100);
    trace.record("point b", 200);
    trace.record("point c", 350);
    EXPECT_EQ("     0.0 ns (+   0.0 ns): point a\n"
            "    50.0 ns (+  50.0 ns): point b\n"
            "   125.0 ns (+  75.0 ns): point c",
            trace.getTrace());
}
TEST_F(TimeTraceTest, printInternal_startAtNextIndex) {
    trace.record("point a", 100);
    trace.record("point b", 200);
    trace.record("point c", 350);
    trace.nextIndex = 1;
    EXPECT_EQ("     0.0 ns (+   0.0 ns): point b\n"
            "    75.0 ns (+  75.0 ns): point c",
            trace.getTrace());
}
TEST_F(TimeTraceTest, printInternal_wrapAround) {
    trace.nextIndex = TimeTrace::BUFFER_SIZE - 2;
    trace.record("point a", 100);
    trace.record("point b", 200);
    trace.record("point c", 350);
    trace.nextIndex = TimeTrace::BUFFER_SIZE - 2;
    EXPECT_EQ("     0.0 ns (+   0.0 ns): point a\n"
            "    50.0 ns (+  50.0 ns): point b\n"
            "   125.0 ns (+  75.0 ns): point c",
            trace.getTrace());
}
TEST_F(TimeTraceTest, printInternal_printToLog) {
    trace.record("point a", 100);
    trace.record("point b", 200);
    trace.record("point c", 350);
    trace.printInternal(NULL);
    EXPECT_EQ("printInternal:      0.0 ns (+   0.0 ns): point a | "
            "printInternal:     50.0 ns (+  50.0 ns): point b | "
            "printInternal:    125.0 ns (+  75.0 ns): point c",
            TestLog::get());
}


}  // namespace RAMCloud
