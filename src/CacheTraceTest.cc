/* Copyright (c) 2014 Stanford University
 *
 * Permissesion to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permissesion notice appear in all copies.xx
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
#include "CacheTrace.h"

namespace RAMCloud {
class CacheTraceTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    CacheTrace trace;

    CacheTraceTest()
        : logEnabler()
        , trace() { }

  private:
    DISALLOW_COPY_AND_ASSIGN(CacheTraceTest);
};

TEST_F(CacheTraceTest, constructor) {
    EXPECT_EQ(0, trace.events[0].message);
    EXPECT_EQ(0, trace.events[0].message);
}

TEST_F(CacheTraceTest, record_basics) {
    trace.record("point a", 1);
    trace.record("point b", 2);
    trace.record("point c", 4);
    EXPECT_EQ("0 misses (+0 misses): point a\n"
              "1 misses (+1 misses): point b\n"
              "3 misses (+2 misses): point c",
            trace.getTrace());
}

TEST_F(CacheTraceTest, record_wrapAround) {
    trace.nextIndex = CacheTrace::BUFFER_SIZE - 2;
    trace.record("near the end", 1);
    trace.record("at the end", 2);
    trace.record("beginning", 4);
    EXPECT_EQ(1, trace.nextIndex);
    trace.nextIndex = CacheTrace::BUFFER_SIZE - 2;
    EXPECT_EQ("0 misses (+0 misses): near the end\n"
              "1 misses (+1 misses): at the end\n"
              "3 misses (+2 misses): beginning",
            trace.getTrace());
}

TEST_F(CacheTraceTest, serialRecord) {
    Util::mockPmcValue = 1;
    trace.serialRecord("point a");
    Util::mockPmcValue = 2;
    trace.serialRecord("point b");
    Util::mockPmcValue = 4;
    trace.serialRecord("point c");
    EXPECT_EQ("0 misses (+0 misses): point a\n"
              "1 misses (+1 misses): point b\n"
              "3 misses (+2 misses): point c",
            trace.getTrace());
    Util::mockPmcValue = 0;
}

TEST_F(CacheTraceTest, getTrace) {
    trace.record("point a", 100);
    EXPECT_EQ("0 misses (+0 misses): point a",
            trace.getTrace());
}

TEST_F(CacheTraceTest, printToLog) {
    trace.record("point a", 100);
    trace.printToLog();
    EXPECT_EQ("printInternal: 0 misses (+0 misses): point a",
            TestLog::get());
}

TEST_F(CacheTraceTest, printInternal_emptyTrace_stringVersion) {
    trace.nextIndex = 0;
    EXPECT_EQ("No cache trace events to print", trace.getTrace());
}

TEST_F(CacheTraceTest, printInternal_emptyTrace_logVersion) {
    trace.nextIndex = 0;
    trace.printInternal(NULL);
    EXPECT_EQ("printInternal: No cache trace events to print", TestLog::get());
}

TEST_F(CacheTraceTest, printInternal_startAtBeginning) {
    trace.record("point a", 1);
    trace.record("point b", 2);
    trace.record("point c", 4);
    EXPECT_EQ("0 misses (+0 misses): point a\n"
              "1 misses (+1 misses): point b\n"
              "3 misses (+2 misses): point c",
            trace.getTrace());
}

TEST_F(CacheTraceTest, printInternal_startAtNextIndex) {
    trace.record("point a", 1);
    trace.record("point b", 2);
    trace.record("point c", 4);
    trace.nextIndex = 1;
    EXPECT_EQ("0 misses (+0 misses): point b\n"
              "2 misses (+2 misses): point c",
            trace.getTrace());
}

TEST_F(CacheTraceTest, printInternal_wrapAround) {
    trace.nextIndex = CacheTrace::BUFFER_SIZE - 2;
    trace.record("point a", 1);
    trace.record("point b", 2);
    trace.record("point c", 4);
    trace.nextIndex = CacheTrace::BUFFER_SIZE - 2;
    EXPECT_EQ("0 misses (+0 misses): point a\n"
              "1 misses (+1 misses): point b\n"
              "3 misses (+2 misses): point c",
            trace.getTrace());
}

TEST_F(CacheTraceTest, printInternal_printToLog) {
    trace.record("point a", 1);
    trace.record("point b", 2);
    trace.record("point c", 4);
    trace.printInternal(NULL);
    EXPECT_EQ("printInternal: 0 misses (+0 misses): point a | "
              "printInternal: 1 misses (+1 misses): point b | "
              "printInternal: 3 misses (+2 misses): point c",
            TestLog::get());
}

}  // namespace RAMCloud
