/* Copyright (c) 2010-2014 Stanford University
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
#include "PerfCounter.h"
#include "Cycles.h"

namespace RAMCloud {
using namespace Perf; // NOLINT

/**
 * This fixture is only used for backgroundWriter-related tests.
 */
class PerfCounterTest : public ::testing::Test {

    public:
    std::string cppName;
    std::string perfDir;
    std::string counterFileName;
    TestLog::Enable logEnabler;

    PerfCounterTest() : cppName(), perfDir(), counterFileName(),
        logEnabler()
    {
        char dirName[100];
        strncpy(dirName, "/tmp/ramcloud-perfcounter-test-delete-this-XXXXXX",
                sizeof(dirName));
        mkdtemp(dirName);
        cppName = std::string(dirName);
        perfDir = std::string(cppName + "/perfcounters");
        counterFileName = std::string(perfDir + "/TestServer_TestCounter");

        mkdir(perfDir.c_str(), 0700);
        Perf::setNameAndPath("TestServer", cppName + "/");
        Perf::EnabledCounter::enabled = true;
    }


    ~PerfCounterTest()
    {
        // Clean up the directory
        char cmd[200];
        memset(cmd, 0, sizeof(cmd));
        snprintf(cmd, sizeof(cmd), "%s %s", "rm -rf ", cppName.c_str());
        system(cmd);

        // Clean up state
        Perf::setNameAndPath("", "");
        Perf::EnabledCounter::enabled = false;
    }

    DISALLOW_COPY_AND_ASSIGN(PerfCounterTest);
};

TEST_F(PerfCounterTest, setNameAndPath) {
    Perf::setNameAndPath("server2.rc03", "logs/20140206192029/");
    EXPECT_EQ(Perf::serverName, std::string("server2.rc03"));
    EXPECT_EQ(Perf::logPath, std::string("logs/20140206192029/"));

    // Clean up by clearing name and path.
    // This is important because otherwise counters that were enabled
    // during the compilation before this test run will become confused and
    // try to write to an invalid file.
    Perf::serverName = "";
    Perf::logPath = "";
}

TEST_F(PerfCounterTest, writeCyclesPerSecond) {
    char fileName[100];
    strncpy(fileName, "/tmp/ramcloud-perfcounter-test-delete-this-XXXXXX",
            sizeof(fileName));
    int d = mkstemp(fileName);
    FILE* f = fdopen(d, "w");
    {
        EnabledCounter TestCounter("TestCounter");
        TestCounter.writeCyclesPerSecond(f);
    }
    fclose(f);

    // Verify what was written.
    f = fopen(fileName, "r");
    double output;
    fread(&output, sizeof(output), 1, f);
    EXPECT_EQ(output, Cycles::perSecond());
    fclose(f);

    // Clean up
    char cmd[200];
    memset(cmd, 0, sizeof(cmd));
    snprintf(cmd, sizeof(cmd), "%s %s", "rm -f ", fileName);
    system(cmd);
}

TEST_F(PerfCounterTest, backgroundWriter_lowThresholdReached) {
    {
        EnabledCounter TestCounter("TestCounter");
        for (uint32_t i = 0; i < LOW_THRESHOLD; i++) {
            TestCounter.recordTime(10U);
        }
    } // Destroy TestCounter once only.

    // Verify that file was written and has the correct size.
    EXPECT_EQ(access(counterFileName.c_str(), F_OK), 0);
    struct stat statbuf;
    stat(counterFileName.c_str(), &statbuf);
    EXPECT_EQ(statbuf.st_size, 800008);

}

TEST_F(PerfCounterTest, backgroundWriter_lowThresholdNotReached) {
    {
        EnabledCounter TestCounter("TestCounter");
        for (uint32_t i = 0; i < LOW_THRESHOLD - 20; i++) {
            TestCounter.recordTime(10U);
        }
    } // Destroy TestCounter once only.

    // Verify that file was written and has the correct size.
    EXPECT_EQ(access(counterFileName.c_str(), F_OK), 0);
    struct stat statbuf;
    stat(counterFileName.c_str(), &statbuf);
    EXPECT_EQ(statbuf.st_size, 799848);
}

TEST_F(PerfCounterTest, backgroundWriter_highThresholdReached) {
    Perf::setNameAndPath("", "");
    EnabledCounter TestCounter("TestCounter");
    TestCounter.terminateBackgroundThread();
    for (uint32_t i = 0; i < HIGH_THRESHOLD + 1; i++) {
        TestCounter.recordTime(10U);
    }
    EXPECT_EQ(TestCounter.ramQueue.size(), HIGH_THRESHOLD);
}

TEST_F(PerfCounterTest, backgroundWriter_wakeupNoName) {
    {
        EnabledCounter TestCounter("TestCounter");
        for (uint32_t i = 0; i < LOW_THRESHOLD - 1; i++) {
            TestCounter.recordTime(10U);
        }
        Perf::setNameAndPath("", "");
        EXPECT_EQ(TestCounter.ramQueue.size(), LOW_THRESHOLD - 1);
        EXPECT_EQ(TestCounter.diskQueue.size(), 0U);

        // Test correct flushing of in-memory buffers, both at LOW_THRESHOLD
        // and at termination.
        TestCounter.recordTime(10U);
        while (TestLog::get().find("No serverName, clearing the diskQueue!")
                == std::string::npos)
            usleep(100);
        EXPECT_EQ(TestCounter.ramQueue.size(), 0U);
        EXPECT_EQ(TestCounter.diskQueue.size(), 0U);
        TestLog::reset();

        TestCounter.recordTime(10U);
        EXPECT_EQ(TestCounter.ramQueue.size(), 1U);
        TestCounter.terminateBackgroundThread();
        EXPECT_EQ(TestCounter.ramQueue.size(), 0U);
    } // Destroy TestCounter once only.

    // Verify that the file was not written.
    EXPECT_EQ(access(counterFileName.c_str(), F_OK), -1);
}

TEST_F(PerfCounterTest, backgroundWriter_sleepOnCountersAccumulated) {
    EnabledCounter TestCounter("TestCounter");
    for (uint32_t i = 0; i < LOW_THRESHOLD - 1; i++) {
        TestCounter.recordTime(10U);
    }
    EXPECT_EQ(TestCounter.ramQueue.size(), LOW_THRESHOLD - 1);
    EXPECT_EQ(TestCounter.diskQueue.size(), 0U);

    while (TestLog::get().empty())
        usleep(100);
    EXPECT_EQ(TestLog::get(),
            "backgroundWriter: Not enough counters, going to sleep!");
    TestLog::reset();
    TestCounter.recordTime(10U);
    while (TestLog::get().empty())
        usleep(100);
    EXPECT_EQ(TestLog::get(), "backgroundWriter: backgroundWriter awakening!");
}

TEST_F(PerfCounterTest, terminateBackgroundThread) {
    EnabledCounter TestCounter("TestCounter");
    EXPECT_EQ(TestCounter.diskWriterThread.joinable(), true);
    TestCounter.terminateBackgroundThread();
    EXPECT_EQ(TestCounter.diskWriterThread.joinable(), false);
}

TEST_F(PerfCounterTest, recordTime) {
    EnabledCounter TestCounter("TestCounter");
    TestCounter.recordTime(10U);
    EXPECT_EQ(TestCounter.ramQueue[0], 10U);
}

TEST_F(PerfCounterTest, getFileName) {
    EnabledCounter TestCounter("TestCounter");
    Perf::setNameAndPath("server1.rc02", "logs/20140206192029/");
    EXPECT_EQ(TestCounter.getFileName(),
            std::string(
             "logs/20140206192029/perfcounters/server1.rc02_TestCounter"));

    Perf::serverName = "";
    Perf::logPath = "";
}

/**
 * Testing EnabledInterval Constructor and Destructor.
 */
TEST_F(PerfCounterTest, EnabledInterval) {
    EnabledCounter TestCounter("TestCounter");
    {
        EnabledInterval TestInterval(&TestCounter);
        usleep(1000);
    }
    EXPECT_GE(Cycles::toSeconds(TestCounter.ramQueue[0])*1.0e06, 1000U);
    EXPECT_LT(Cycles::toSeconds(TestCounter.ramQueue[0])*1.0e06, 1200U);
}

TEST_F(PerfCounterTest, start) {
    EnabledCounter TestCounter("TestCounter");
    EnabledInterval TestInterval(&TestCounter, false);
    uint64_t tempTime = Cycles::rdtsc();
    TestInterval.start(tempTime);
    EXPECT_EQ(TestInterval.startTime, tempTime);

    Cycles::mockTscValue = 5U;
    TestInterval.start();
    EXPECT_EQ(TestInterval.startTime, 5U);
    Cycles::mockTscValue = 0;
}

TEST_F(PerfCounterTest, stop) {
    EnabledCounter TestCounter("TestCounter");
    {
        EnabledInterval TestInterval(&TestCounter, false);
        usleep(1000);
        TestInterval.start();
        usleep(1000);
        TestInterval.stop();

        uint64_t tempTime = Cycles::rdtsc();
        TestInterval.start(tempTime);
        usleep(1000);
        TestInterval.stop();

        usleep(1000);
        TestInterval.start(tempTime);
        tempTime = Cycles::rdtsc();
        TestInterval.stop(tempTime);
    }
    EXPECT_GE(Cycles::toSeconds(TestCounter.ramQueue[0])*1.0e06, 1000U);
    EXPECT_LT(Cycles::toSeconds(TestCounter.ramQueue[0])*1.0e06, 1200U);
    EXPECT_GE(Cycles::toSeconds(TestCounter.ramQueue[1])*1.0e06, 1000U);
    EXPECT_LT(Cycles::toSeconds(TestCounter.ramQueue[1])*1.0e06, 1200U);
    EXPECT_GE(Cycles::toSeconds(TestCounter.ramQueue[2])*1.0e06, 2000U);
    EXPECT_LT(Cycles::toSeconds(TestCounter.ramQueue[2])*1.0e06, 2400U);
}

TEST_F(PerfCounterTest, double_stop) {
    EnabledCounter TestCounter("TestCounter");
    EnabledInterval TestInterval(&TestCounter);
    usleep(1000);
    TestInterval.stop();
    usleep(1000);
    TestInterval.stop();
    EXPECT_EQ(TestCounter.ramQueue.size(), 1U);
}
}
