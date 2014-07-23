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
#include "TestLog.h"
#include "ShortMacros.h"
#include "StringUtil.h"

namespace RAMCloud {

class LoggerTest : public ::testing::Test {
  public:
    static const uint32_t numLogLevels = static_cast<uint32_t>(NUM_LOG_LEVELS);

    LoggerTest() {}

    ~LoggerTest() {
        Logger::get().reset();
        unlink("__test.log");
    }

    // Given a string, read the log file in __test.log and return everything
    // in that file starting with the given string. If the string doesn't appear
    // in the log file, then return the entire file.
    string
    logSuffix(const char* s)
    {
        string output = TestUtil::readFile("__test.log");
        size_t pos = output.find(s);
        if (pos != string::npos) {
           return output.substr(pos);
        }
        return output;
    }

    DISALLOW_COPY_AND_ASSIGN(LoggerTest);
};

TEST_F(LoggerTest, constructor) {
    Logger l(WARNING);
    EXPECT_TRUE(l.stream == NULL);
    EXPECT_EQ(WARNING, l.logLevels[0]);
}

TEST_F(LoggerTest, setLogFile_basics) {
    Logger l(NOTICE);
    l.setLogFile("__test.log");
    l.logMessage(DEFAULT_LOG_MODULE, NOTICE, HERE, "message 1\n");
    EXPECT_TRUE(TestUtil::matchesPosixRegex("message 1",
            TestUtil::readFile("__test.log")));
    l.setLogFile("__test.log", false);
    l.logMessage(DEFAULT_LOG_MODULE, NOTICE, HERE, "message 2");
    EXPECT_TRUE(TestUtil::matchesPosixRegex("message 1.*message 2",
            TestUtil::readFile("__test.log")));
    l.setLogFile("__test.log", true);
    l.logMessage(DEFAULT_LOG_MODULE, NOTICE, HERE, "message 3");
    EXPECT_TRUE(TestUtil::doesNotMatchPosixRegex("message 1",
            TestUtil::readFile("__test.log")));
    EXPECT_TRUE(TestUtil::matchesPosixRegex("message 3",
            TestUtil::readFile("__test.log")));
}
TEST_F(LoggerTest, setLogFile_cantOpenFile) {
    Logger l(NOTICE);
    string message("no exception");
    try {
        l.setLogFile("__gorp/__xyz/__foo");
    } catch (Exception& e) {
        message = e.message;
    }
    EXPECT_EQ("couldn't open log file '__gorp/__xyz/__foo': "
            "No such file or directory", message);
}

TEST_F(LoggerTest, setLogLevel) {
    Logger l(WARNING);
    l.setLogLevel(DEFAULT_LOG_MODULE, NOTICE);
    EXPECT_EQ(NOTICE, l.logLevels[DEFAULT_LOG_MODULE]);
}

TEST_F(LoggerTest, setLogLevel_int) {
    Logger l(WARNING);

    l.setLogLevel(DEFAULT_LOG_MODULE, -1);
    EXPECT_EQ(0, l.logLevels[DEFAULT_LOG_MODULE]);

    l.setLogLevel(DEFAULT_LOG_MODULE, numLogLevels);
    EXPECT_EQ(numLogLevels - 1,
              downCast<unsigned>(l.logLevels[DEFAULT_LOG_MODULE]));

    l.setLogLevel(DEFAULT_LOG_MODULE, 0);
    EXPECT_EQ(0, l.logLevels[DEFAULT_LOG_MODULE]);

    l.setLogLevel(DEFAULT_LOG_MODULE, numLogLevels - 1);
    EXPECT_EQ(numLogLevels - 1,
              downCast<unsigned>(l.logLevels[DEFAULT_LOG_MODULE]));
}

TEST_F(LoggerTest, setLogLevel_string) {
    Logger l(WARNING);

    l.setLogLevel("default", "-1");
    EXPECT_EQ(0, l.logLevels[DEFAULT_LOG_MODULE]);

    l.setLogLevel("default", "1");
    EXPECT_EQ(1, l.logLevels[DEFAULT_LOG_MODULE]);

    l.setLogLevel("default", "NOTICE");
    EXPECT_EQ(NOTICE, l.logLevels[DEFAULT_LOG_MODULE]);

    l.setLogLevel("transport", "1");
    EXPECT_EQ(1, l.logLevels[TRANSPORT_MODULE]);

    TestLog::Enable _;
    l.setLogLevel("stabYourself", "1");
    l.setLogLevel("default", "");
    l.setLogLevel("default", "junk");
    EXPECT_EQ(
        "setLogLevel: Ignoring bad log module name: stabYourself | "
        "setLogLevel: Ignoring bad log module level:  | "
        "setLogLevel: Ignoring bad log module level: junk", TestLog::get());
}

TEST_F(LoggerTest, changeLogLevel) {
    Logger l(WARNING);
    l.changeLogLevel(DEFAULT_LOG_MODULE, -1);
    EXPECT_EQ(ERROR, l.logLevels[DEFAULT_LOG_MODULE]);
    l.changeLogLevel(DEFAULT_LOG_MODULE, 1);
    EXPECT_EQ(WARNING, l.logLevels[DEFAULT_LOG_MODULE]);
}

TEST_F(LoggerTest, setLogLevels) {
    Logger l(WARNING);
    l.setLogLevels(NOTICE);
    for (int i = 0; i < NUM_LOG_MODULES; i++)
        EXPECT_EQ(NOTICE, l.logLevels[i]);
}

TEST_F(LoggerTest, setLogLevels_int) {
    Logger l(WARNING);

    l.setLogLevels(-1);
    for (int i = 0; i < NUM_LOG_MODULES; i++)
        EXPECT_EQ(0, l.logLevels[i]);

    l.setLogLevels(numLogLevels);
    for (int i = 0; i < NUM_LOG_MODULES; i++)
        EXPECT_EQ(numLogLevels - 1, downCast<unsigned>(l.logLevels[i]));

    l.setLogLevels(0);
    for (int i = 0; i < NUM_LOG_MODULES; i++)
        EXPECT_EQ(0, l.logLevels[i]);

    l.setLogLevels(numLogLevels - 1);
    for (int i = 0; i < NUM_LOG_MODULES; i++)
        EXPECT_EQ(numLogLevels - 1, downCast<unsigned>(l.logLevels[i]));
}

TEST_F(LoggerTest, setLogLevels_string) {
    Logger l(WARNING);

    l.setLogLevels("-1");
    for (int i = 0; i < NUM_LOG_MODULES; i++)
        EXPECT_EQ(0, l.logLevels[i]);

    l.setLogLevels("2");
    for (int i = 0; i < NUM_LOG_MODULES; i++)
        EXPECT_EQ(2, l.logLevels[i]);

    l.setLogLevels("NOTICE");
    for (int i = 0; i < NUM_LOG_MODULES; i++)
        EXPECT_EQ(NOTICE, l.logLevels[i]);

    TestLog::Enable _;
    l.setLogLevels("oral trauma");
    EXPECT_EQ(
        "setLogLevels: Ignoring bad log module level: oral trauma",
        TestLog::get());
}

TEST_F(LoggerTest, changeLogLevels) {
    Logger l(WARNING);

    l.changeLogLevels(-1);
    for (int i = 0; i < NUM_LOG_MODULES; i++)
        EXPECT_EQ(ERROR, l.logLevels[i]);

    l.changeLogLevels(1);
    for (int i = 0; i < NUM_LOG_MODULES; i++)
        EXPECT_EQ(WARNING, l.logLevels[i]);
}

TEST_F(LoggerTest, isLogging) {
    Logger l(WARNING);
    EXPECT_TRUE(l.isLogging(DEFAULT_LOG_MODULE, ERROR));
    EXPECT_TRUE(l.isLogging(DEFAULT_LOG_MODULE, WARNING));
    EXPECT_TRUE(!l.isLogging(DEFAULT_LOG_MODULE, NOTICE));
}

TEST_F(LoggerTest, disableCollapsing_enableCollapsing) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 99, "func", "pretty"), "first ");
    logger.disableCollapsing();
    logger.disableCollapsing();
    logger.enableCollapsing();
    EXPECT_EQ(1, logger.collapsingDisableCount);

    // Make sure that collapsing really is disabled.
    logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 99, "func", "pretty"), "first ");
    const char* timeOrPidPattern = "[0-9]+[.:][0-9]+ ?";
    string log1 = StringUtil::regsub(TestUtil::readFile("__test.log"),
            timeOrPidPattern, "");

    // Make sure that collapsing can be re-enabled again.
    logger.enableCollapsing();
    EXPECT_EQ(0, logger.collapsingDisableCount);
    logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 99, "func", "pretty"), "first ");
    string log2 = StringUtil::regsub(TestUtil::readFile("__test.log"),
            timeOrPidPattern, "");
    EXPECT_EQ("file:99 in func default ERROR[]: first "
            "file:99 in func default ERROR[]: first ", log1);
    EXPECT_EQ("file:99 in func default ERROR[]: first "
            "file:99 in func default ERROR[]: first ", log2);
}

TEST_F(LoggerTest, logMessage_basics) { // also tests LOG
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");

    LOG(DEBUG, "x");
    EXPECT_EQ("", TestUtil::readFile("__test.log"));

    LOG(ERROR, "rofl: %d", 3);
    const char* pattern = "^[[:digit:]]\\{10\\}\\.[[:digit:]]\\{9\\} "
                            "src/LoggerTest.cc:[[:digit:]]\\{1,4\\} "
                            "in LoggerTest_logMessage_basics_Test::TestBody "
                            "default ERROR\\[[[:digit:]]\\{1,6\\}"
                            ":[[:digit:]]\\{1,5\\}\\]: "
                            "rofl: 3\n$";
    EXPECT_TRUE(TestUtil::matchesPosixRegex(pattern,
            TestUtil::readFile("__test.log")));
}

TEST_F(LoggerTest, logMessage_barelyFitsInBuffer) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.testingBufferSize = 30;

    logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, ERROR, HERE,
            "10: abcdef20: abcdef30: abcde");
    EXPECT_EQ(": 10: abcdef20: abcdef30: abcde",
            logSuffix(": 10:"));
}

TEST_F(LoggerTest, logMessage_doesntFitInBuffer) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.testingBufferSize = 30;

    logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, ERROR, HERE,
            "10: abcdef20: abcdef30: abcdexxx");
    EXPECT_EQ(": 10: abcdef20: abcdef30: abcde... (3 chars truncated)\n",
            logSuffix(": 10:"));
}

TEST_F(LoggerTest, logMessage_collapsingDisabled) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.disableCollapsing();

    for (int i = 0; i < 3; i++) {
        logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
                CodeLocation("file", 99, "func", "pretty"), "first ");
    }
    const char* timeOrPidPattern = "[0-9]+[.:][0-9]+ ?";
    EXPECT_EQ("file:99 in func default ERROR[]: first "
            "file:99 in func default ERROR[]: first "
            "file:99 in func default ERROR[]: first ",
            StringUtil::regsub(TestUtil::readFile("__test.log"),
            timeOrPidPattern, ""));
}

TEST_F(LoggerTest, logMessage_collapseDuplicates) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.collapseIntervalMs = 2;
    uint64_t start = Cycles::rdtsc();

    // Log a message several times; only the first should be printed.
    for (int i = 0; i < 5; i++) {
        logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
                CodeLocation("file", 99, "func", "pretty"), "first ");
    }

    // This regular expression pattern is used to erase timestamps and
    // pid+threadId elements, which can vary from run to run.
    const char* timeOrPidPattern = "[0-9]+[.:][0-9]+ ?";
    string output1 = StringUtil::regsub(TestUtil::readFile("__test.log"),
            timeOrPidPattern, "");

    // Log a different message; it should be printed immediately.
    logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 99, "func", "pretty"), "second ");
    string output2 = StringUtil::regsub(TestUtil::readFile("__test.log"),
            timeOrPidPattern, "");

    // If there were unexpected delays in the code above, it will mess up
    // the timing and could generate spurious error messages; if this
    // happens, just skip the rest of the test.
    if (Cycles::toSeconds(Cycles::rdtsc() - start) >= .002) {
        return;
    }

    // Wait a while and log the original message; one more copy should appear.
    usleep(3000);
    for (int i = 0; i < 2; i++) {
        logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
                CodeLocation("file", 99, "func", "pretty"), "first ");
    }
    EXPECT_EQ("file:99 in func default ERROR[]: first ", output1);
    EXPECT_EQ("file:99 in func default ERROR[]: first "
            "file:99 in func default ERROR[]: second ", output2);
    EXPECT_EQ("file:99 in func default ERROR[]: first "
            "file:99 in func default ERROR[]: second "
            "(4 duplicates of the following message were suppressed)\n"
            "file:99 in func default ERROR[]: first ",
            StringUtil::regsub(TestUtil::readFile("__test.log"),
            timeOrPidPattern, ""));
}

TEST_F(LoggerTest, logMessage_manageCollapseMap) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.collapseIntervalMs = 2;

    // Log a message 3x; only the first should be printed.
    for (int i = 0; i < 3; i++) {
        logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
                CodeLocation("file", 99, "func", "pretty"), "first ");
    }
    const char* timeOrPidPattern = "[0-9]+[.:][0-9]+ ?";
    string output1 = StringUtil::regsub(TestUtil::readFile("__test.log"),
            timeOrPidPattern, "");

    // Wait a while and log a different message; the first should get printed
    // again.
    usleep(3000);
    logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 99, "func", "pretty"), "second ");
    string output2 = StringUtil::regsub(TestUtil::readFile("__test.log"),
            timeOrPidPattern, "");

    // Wait a while and log a third message; the first and second should get
    // removed from collapseMap.
    usleep(3000);
    logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 99, "func", "pretty"), "third ");
    EXPECT_EQ("file:99 in func default ERROR[]: first ", output1);
    EXPECT_EQ("file:99 in func default ERROR[]: first "
            "(1 duplicates of the following message were suppressed)\n"
            "file:99 in func default ERROR[]: first "
            "file:99 in func default ERROR[]: second ", output2);
    EXPECT_EQ(1U, logger.collapseMap.size());
}

TEST_F(LoggerTest, logMessage_restrictSizeOfCollapseMap) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.collapseIntervalMs = 2;
    logger.maxCollapseMapSize = 2;

    logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 99, "func", "pretty"), "first ");
    logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 99, "func", "pretty"), "second ");
    logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 99, "func", "pretty"), "third ");
    EXPECT_EQ(2U, logger.collapseMap.size());

    usleep(3000);
    logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 99, "func", "pretty"), "fourth ");
    EXPECT_EQ(1U, logger.collapseMap.size());
}

TEST_F(LoggerTest, cleanCollapseMap_deleteEntries) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.collapseMap["message1\n"] = Logger::SkipInfo({1, 200000000}, 0);
    logger.collapseMap["message2\n"] = Logger::SkipInfo({1, 400000000}, 0);
    logger.collapseMap["message3\n"] = Logger::SkipInfo({1, 600000000}, 0);
    logger.cleanCollapseMap({1, 600000000});
    EXPECT_EQ("", TestUtil::readFile("__test.log"));
    EXPECT_EQ(0U, logger.collapseMap.size());
    EXPECT_EQ(1001, logger.nextCleanTime.tv_sec);
    EXPECT_EQ(600000000, logger.nextCleanTime.tv_nsec);
}

TEST_F(LoggerTest, cleanCollapseMap_print) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.collapseMap["message1\n"] = Logger::SkipInfo({1, 200000000}, 0);
    logger.collapseMap["message2\n"] = Logger::SkipInfo({1, 400000000}, 8);
    logger.collapseMap["message3\n"] = Logger::SkipInfo({1, 600000000}, 0);
    logger.cleanCollapseMap({1, 500000000});
    EXPECT_EQ("0000000001.400000000 (7 duplicates of the following message "
            "were suppressed)\n0000000001.400000000 message2\n",
            TestUtil::readFile("__test.log"));
    EXPECT_EQ(2U, logger.collapseMap.size());
    EXPECT_EQ(6, logger.collapseMap["message2\n"].nextPrintTime.tv_sec);
    EXPECT_EQ(500000000U,
            logger.collapseMap["message2\n"].nextPrintTime.tv_nsec);
    EXPECT_EQ(1U, logger.nextCleanTime.tv_sec);
    EXPECT_EQ(600000000U, logger.nextCleanTime.tv_nsec);
}

TEST_F(LoggerTest, logMessage_printMessage) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.printMessage({3, 50000000}, "message 1; ", 0);
    logger.printMessage({7, 90000000}, "message 2", 6);
    EXPECT_EQ("0000000003.050000000 message 1; "
            "0000000007.090000000 (6 duplicates of the following "
            "message were suppressed)\n"
            "0000000007.090000000 message 2",
            TestUtil::readFile("__test.log"));
}

TEST_F(LoggerTest, DIE) { // also tests getMessage
    Logger& logger = Logger::get();
    logger.stream = fmemopen(NULL, 1024, "w");
    assert(logger.stream != NULL);
    try {
        DIE("rofl: %d", 3);
    } catch (RAMCloud::FatalError& e) {
        int64_t streamPos = ftell(logger.stream);
        EXPECT_GT(streamPos, 0);
        EXPECT_EQ("rofl: 3", e.message);
        return;
    }
    EXPECT_STREQ("", "FatalError not thrown");
}

TEST_F(LoggerTest, redirectStderr) {
    // If the application redirects stderr, make sure that log messages
    // go there, not to the old stderr.
    Logger l(NOTICE);
    FILE *f = fopen("__test.log", "w");
    ASSERT_TRUE(f != NULL);
    FILE *savedStderr = stderr;
    stderr = f;
    l.logMessage(DEFAULT_LOG_MODULE, NOTICE, HERE, "message 99\n");
    stderr = savedStderr;
    fclose(f);
    EXPECT_TRUE(TestUtil::matchesPosixRegex("message 99",
            TestUtil::readFile("__test.log")));
}

TEST_F(LoggerTest, assertionError) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.assertionError("assertion info", "file", 99, "function");
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "file:99 in function .* Assertion `assertion info' failed",
            TestUtil::readFile("__test.log")));
}

}  // namespace RAMCloud
