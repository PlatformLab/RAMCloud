/* Copyright (c) 2014-2016 Stanford University
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
#include "Dispatch.h"
#include "Logger.h"
#include "TimeTrace.h"

namespace RAMCloud {
class TimeTraceTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    TimeTrace::Buffer buffer, buffer2, buffer3, buffer4;
    std::vector<TimeTrace::Buffer*> buffers;

    TimeTraceTest()
        : logEnabler()
        , buffer()
        , buffer2()
        , buffer3()
        , buffer4()
        , buffers()
    {
        Cycles::mockCyclesPerSec = 2e09;
        buffers.push_back(&buffer);
        buffers.push_back(&buffer2);
        buffers.push_back(&buffer3);
        buffers.push_back(&buffer4);
        TimeTrace::reset();
    }

    ~TimeTraceTest()
    {
        Cycles::mockCyclesPerSec = 0;
        delete TimeTrace::backgroundLogger;
        TimeTrace::backgroundLogger = NULL;
        TimeTrace::activeReaders = 0;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(TimeTraceTest);
};

TEST_F(TimeTraceTest, getTrace) {
    TimeTrace::record(100, "point a");
    buffer.record(100, "point b");
    TimeTrace::threadBuffers.push_back(&buffer);
    EXPECT_EQ("     0.0 ns (+   0.0 ns): point a\n"
            "     0.0 ns (+   0.0 ns): point b", TimeTrace::getTrace());
    TimeTrace::threadBuffers.pop_back();
}

TEST_F(TimeTraceTest, printInternal_startAtBeginning) {
    buffer.record(100, "point a");
    buffer.record(200, "point b");
    buffer.record(350, "point c");
    EXPECT_EQ("     0.0 ns (+   0.0 ns): point a\n"
            "    50.0 ns (+  50.0 ns): point b\n"
            "   125.0 ns (+  75.0 ns): point c",
            buffer.getTrace());
}
TEST_F(TimeTraceTest, printInternal_startAtNextIndexPlus1) {
    buffer.record(100, "point a");
    buffer.record(200, "point b");
    buffer.record(350, "point c");
    buffer.record(360, "point d");
    buffer.nextIndex = 1;
    EXPECT_EQ("     0.0 ns (+   0.0 ns): point c\n"
            "     5.0 ns (+   5.0 ns): point d",
            buffer.getTrace());
}
TEST_F(TimeTraceTest, printInternal_pickStartingTimeAndPrune) {
    buffer.record(100, "1.a");
    buffer.record(300, "1.b");
    buffer2.record(200, "2.a");
    buffer2.record(250, "2.b");
    buffer4.record(50, "4.a");
    buffer4.record(550, "4.b");
    TimeTrace::printInternal(&buffers, NULL);
    EXPECT_EQ("printInternal: Starting TSC 200, cyclesPerSec 2000000000 | "
            "printInternal:      0.0 ns (+   0.0 ns): 2.a | "
            "printInternal:     25.0 ns (+  25.0 ns): 2.b | "
            "printInternal:     50.0 ns (+  25.0 ns): 1.b | "
            "printInternal:    175.0 ns (+ 125.0 ns): 4.b",
            TestLog::get());
}
TEST_F(TimeTraceTest, printInternal_allEntriesInBufferBeforeStartingTime) {
    buffer.record(100, "1.a");
    buffer.record(300, "1.b");
    for (uint32_t i = 0; i < buffer.BUFFER_SIZE; i++) {
        buffer2.record(50, "2.xx");
    }
    TimeTrace::printInternal(&buffers, NULL);
    EXPECT_EQ("printInternal: Starting TSC 100, cyclesPerSec 2000000000 | "
            "printInternal:      0.0 ns (+   0.0 ns): 1.a | "
            "printInternal:    100.0 ns (+ 100.0 ns): 1.b",
            TestLog::get());
}
TEST_F(TimeTraceTest, printInternal_wrapAround) {
    buffer.nextIndex = TimeTrace::Buffer::BUFFER_SIZE - 2;
    buffer.record(100, "point a");
    buffer.record(200, "point b");
    buffer.record(350, "point c");
    // -3 needed below because the event at nextIndex gets skipped...
    buffer.nextIndex = TimeTrace::Buffer::BUFFER_SIZE - 3;
    EXPECT_EQ("     0.0 ns (+   0.0 ns): point a\n"
            "    50.0 ns (+  50.0 ns): point b\n"
            "   125.0 ns (+  75.0 ns): point c",
            buffer.getTrace());
}
TEST_F(TimeTraceTest, printInternal_printToStringWithArgs) {
    buffer.record(200, "point b %d %d %d %u", 99, 101, -1, -2);
    EXPECT_EQ("     0.0 ns (+   0.0 ns): point b 99 101 -1 4294967294",
            buffer.getTrace());
}
TEST_F(TimeTraceTest, printInternal_printToLogWithArgs) {
    buffer.record(100, "point a");
    buffer.record(200, "point b %d %d %d %u", 99, 101, -1, -2);
    buffer.record(350, "point c");
    TimeTrace::printInternal(&buffers, NULL);
    EXPECT_EQ("printInternal: Starting TSC 100, cyclesPerSec 2000000000 | "
            "printInternal:      0.0 ns (+   0.0 ns): point a | "
            "printInternal:     50.0 ns (+  50.0 ns): point b 99 101 "
            "-1 4294967294 | "
            "printInternal:    125.0 ns (+  75.0 ns): point c",
            TestLog::get());
}
TEST_F(TimeTraceTest, printInternal_emptyTrace_stringVersion) {
    buffer.nextIndex = 0;
    EXPECT_EQ("No time trace events to print", buffer.getTrace());
}
TEST_F(TimeTraceTest, printInternal_emptyTrace_logVersion) {
    buffer.nextIndex = 0;
    TimeTrace::printInternal(&buffers, NULL);
    EXPECT_EQ("printInternal: Starting TSC 0, cyclesPerSec 2000000000 | "
            "printInternal: No time trace events to print", TestLog::get());
}

TEST_F(TimeTraceTest, printToLog) {
    TimeTrace::record(100, "point a");
    buffer.record(100, "point b");
    TimeTrace::threadBuffers.push_back(&buffer);
    TimeTrace::printToLog();
    EXPECT_EQ("printInternal: Starting TSC 100, cyclesPerSec 2000000000 | "
            "printInternal:      0.0 ns (+   0.0 ns): point a | "
            "printInternal:      0.0 ns (+   0.0 ns): point b",
            TestLog::get());
    TimeTrace::threadBuffers.pop_back();
}

TEST_F(TimeTraceTest, printToLogBackground) {
    Dispatch dispatch(false);
    TimeTrace::record(100, "point a");
    TimeTrace::printToLogBackground(&dispatch);
    EXPECT_EQ("", TestLog::get());
    EXPECT_EQ(1, TimeTrace::activeReaders);
    dispatch.poll();
    for (int i = 0; i < 1000; i++) {
        if (TimeTrace::backgroundLogger->isFinished) {
            break;
        }
        usleep(1000);
    }
    EXPECT_TRUE(TimeTrace::backgroundLogger->isFinished);
    EXPECT_EQ("printInternal: Starting TSC 100, cyclesPerSec 2000000000 | "
            "printInternal:      0.0 ns (+   0.0 ns): point a",
            TestLog::get());
    EXPECT_EQ(0, TimeTrace::activeReaders);
}

TEST_F(TimeTraceTest, reset) {
    TimeTrace::record(100, "point a");
    buffer.record(100, "point b");
    TimeTrace::threadBuffers.push_back(&buffer);
    TimeTrace::reset();
    EXPECT_EQ(0, TimeTrace::threadBuffers[0]->nextIndex);
    EXPECT_TRUE(TimeTrace::threadBuffers[0]->events[0].format == NULL);
    EXPECT_EQ(0, buffer.nextIndex);
    EXPECT_TRUE(buffer.events[0].format == NULL);
    TimeTrace::threadBuffers.pop_back();
}

TEST_F(TimeTraceTest, Buffer_constructor) {
    EXPECT_EQ(0, buffer.events[0].format);
    EXPECT_EQ(0, buffer.events[0].format);
}

TEST_F(TimeTraceTest, Buffer_record_basics) {
    buffer.record(100, "point a");
    buffer.record(200, "point b %u %u %d %u", 1, 2, -3, 4);
    buffer.record(350, "point c");
    EXPECT_EQ("     0.0 ns (+   0.0 ns): point a\n"
            "    50.0 ns (+  50.0 ns): point b 1 2 -3 4\n"
            "   125.0 ns (+  75.0 ns): point c",
            buffer.getTrace());
}

TEST_F(TimeTraceTest, Buffer_record_readersActive) {
    TimeTrace::activeReaders = 1;
    buffer.record(100, "point a");
    buffer.record(200, "point b");
    TimeTrace::activeReaders = 0;
    buffer.record(350, "point c");
    EXPECT_EQ("     0.0 ns (+   0.0 ns): point c",
            buffer.getTrace());
}

TEST_F(TimeTraceTest, Buffer_record_wrapAround) {
    buffer.nextIndex = TimeTrace::Buffer::BUFFER_SIZE - 2;
    buffer.record(100, "near the end");
    buffer.record(200, "at the end");
    buffer.record(350, "beginning");
    EXPECT_EQ(1, buffer.nextIndex);
    // -3 needed below because the event at nextIndex gets skipped...
    buffer.nextIndex = TimeTrace::Buffer::BUFFER_SIZE - 3;
    EXPECT_EQ("     0.0 ns (+   0.0 ns): near the end\n"
            "    50.0 ns (+  50.0 ns): at the end\n"
            "   125.0 ns (+  75.0 ns): beginning",
            buffer.getTrace());
}

TEST_F(TimeTraceTest, Buffer_getTrace) {
    buffer.record(100, "point a");
    EXPECT_EQ("     0.0 ns (+   0.0 ns): point a",
            buffer.getTrace());
}

TEST_F(TimeTraceTest, Buffer_printToLog) {
    buffer.record(100, "point a");
    buffer.printToLog();
    EXPECT_EQ("printInternal: Starting TSC 100, cyclesPerSec 2000000000 | "
            "printInternal:      0.0 ns (+   0.0 ns): point a",
            TestLog::get());
}

TEST_F(TimeTraceTest, Buffer_reset) {
    buffer.record("first", 100);
    buffer.record("second", 200);
    buffer.record("third", 200);
    buffer.events[20].format = "sneaky";
    buffer.reset();
    EXPECT_TRUE(buffer.events[2].format == NULL);
    EXPECT_FALSE(buffer.events[20].format == NULL);
    EXPECT_EQ(0, buffer.nextIndex);
}

}  // namespace RAMCloud
