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

#include <regex>
#include <sys/types.h>

#include "TestUtil.h"
#include "TestLog.h"
#include "ShortMacros.h"
#include "StringUtil.h"

namespace RAMCloud {

class LoggerTest : public ::testing::Test {
  public:
    static const uint32_t numLogLevels = static_cast<uint32_t>(NUM_LOG_LEVELS);
    struct timespec testTime;

    LoggerTest()
        : testTime({3, 500000000})
    {
        Logger::get().testingLogTime = &testTime;
    }

    ~LoggerTest()
    {
        Logger::get().reset();
        unlink("__test.log");
    }

    // Given a string, read the log file in __test.log and return everything
    // in that file starting with the given string. If the string doesn't appear
    // in the log file, then return the entire file.
    string
    logSuffix(const char* s)
    {
        Logger::get().sync();
        string output = TestUtil::readFile("__test.log");
        size_t pos = output.find(s);
        if (pos != string::npos) {
           return output.substr(pos);
        }
        return output;
    }

    // Read the log file from disk (waiting for any buffered data to be
    // output).
    string
    getLog(const char* fileName, bool sanitizeLineNumber = false)
    {
        Logger::get().sync();
        string result = TestUtil::readFile(fileName);
        if (sanitizeLineNumber) {
            std::regex exp("\\.cc:[[:digit:]]{1,4} ");
            result = std::regex_replace(result, exp, ".cc:xxx ");
        }
        return result;
    }

    DISALLOW_COPY_AND_ASSIGN(LoggerTest);
};

TEST_F(LoggerTest, constructor) {
    Logger l(WARNING);
    EXPECT_EQ(WARNING, l.logLevels[0]);
}

TEST_F(LoggerTest, destructor) {
    // Make sure the print thread exits
    TestLog::Enable _;
    Tub<Logger> l;
    l.construct(WARNING);
    l.destroy();
    EXPECT_EQ("printThreadMain: print thread exiting", TestLog::get());
}

TEST_F(LoggerTest, setLogFile_basics) {
    Logger l(NOTICE);
    l.setLogFile("__test.log");
    l.logMessage(false, DEFAULT_LOG_MODULE, NOTICE, HERE, "message 1\n");
    EXPECT_TRUE(TestUtil::matchesPosixRegex("message 1", getLog("__test.log")));
    l.setLogFile("__test.log", false);
    l.logMessage(false, DEFAULT_LOG_MODULE, NOTICE, HERE, "message 2");
    EXPECT_TRUE(TestUtil::matchesPosixRegex("message 1.*message 2",
            getLog("__test.log")));
    l.setLogFile("__test.log", true);
    l.logMessage(false, DEFAULT_LOG_MODULE, NOTICE, HERE, "message 3");
    EXPECT_TRUE(TestUtil::doesNotMatchPosixRegex("message 1",
            getLog("__test.log")));
    EXPECT_TRUE(TestUtil::matchesPosixRegex("message 3",
            getLog("__test.log")));
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

TEST_F(LoggerTest, logMessage_basics) { // also tests LOG
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");

    LOG(DEBUG, "x");
    EXPECT_EQ("", getLog("__test.log"));

    LOG(ERROR, "rofl: %d", 3);
    const char* pattern = "^0000000003.500000000 "
            "LoggerTest.cc:[[:digit:]]\\{1,4\\} in TestBody "
            "ERROR\\[1\\]: rofl: 3\n$";
    EXPECT_TRUE(TestUtil::matchesPosixRegex(pattern,
            TestUtil::readFile("__test.log")));

    logger.setLogFile("__test.log", true);
    for (int i = 0; i < 3; i++) {
        logger.logMessage(false, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
                CodeLocation("file", 99, "func", "pretty"), "first ");
    }
    EXPECT_EQ("0000000003.500000000 file:99 in func ERROR[1]: first "
            "0000000003.500000000 file:99 in func ERROR[1]: first "
            "0000000003.500000000 file:99 in func ERROR[1]: first ",
            getLog("__test.log"));
}

TEST_F(LoggerTest, logMessage_collapseDuplicates) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.collapseIntervalMs = 2;

    // Log a message several times; only the first should be printed.
    for (int i = 0; i < 5; i++) {
        logger.logMessage(true, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
                CodeLocation("file", 99, "func", "pretty"), "first ");
    }
    EXPECT_EQ("0000000003.500000000 file:99 in func ERROR[1]: first ",
            getLog("__test.log"));

    // Log a different message; it should be printed immediately.
    testTime.tv_nsec += 1000000;
    logger.setLogFile("__test.log", true);
    logger.logMessage(true, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 100, "func", "pretty"), "second ");
    EXPECT_EQ("0000000003.501000000 file:100 in func ERROR[1]: second ",
            getLog("__test.log"));

    // Wait a while and log the original message; one more copy should appear.
    testTime.tv_nsec += 2000000;
    logger.setLogFile("__test.log", true);
    for (int i = 0; i < 2; i++) {
        logger.logMessage(true, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
                CodeLocation("file", 99, "func", "pretty"), "first ");
    }
    EXPECT_EQ("0000000003.503000000 file:99 in func ERROR[1]: "
            "(4 duplicates of this message were skipped) first ",
            getLog("__test.log"));
}

TEST_F(LoggerTest, logMessage_restrictSizeOfCollapseMap) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.collapseIntervalMs = 2;
    logger.maxCollapseMapSize = 2;

    logger.logMessage(true, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 97, "func", "pretty"), "first ");
    logger.logMessage(true, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 98, "func", "pretty"), "second ");
    logger.logMessage(true, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 99, "func", "pretty"), "third ");
    EXPECT_EQ(2U, logger.collapseMap.size());
}

TEST_F(LoggerTest, logMessage_discardEntriesBecauseOfOverflow) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.collapseIntervalMs = 2;
    logger.maxCollapseMapSize = 2;
    logger.nextToInsert = 0;
    logger.nextToPrint = 10;

    logger.logMessage(false, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 97, "func", "pretty"), "first ");
    logger.logMessage(false, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 98, "func", "pretty"), "second ");
    EXPECT_EQ(2, logger.discardedEntries);
    logger.nextToPrint = 0;
    EXPECT_EQ("", getLog("__test.log"));

    logger.logMessage(false, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
            CodeLocation("file", 99, "func", "pretty"), "third ");
    EXPECT_EQ(0, logger.discardedEntries);
    EXPECT_EQ("0000000003.500000000 Logger.cc:xxx in logMessage "
            "WARNING[1]: 2 log messages lost because of buffer overflow\n"
            "0000000003.500000000 file:99 in func ERROR[1]: third ",
            getLog("__test.log", true));
}

TEST_F(LoggerTest, logMessage_messageTooLong) {
    // This test exercises all of the code to handle log messages that
    // are too long for the variable "buffer".
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.collapseIntervalMs = 2;

    // Overflow happens during warning about discarded entries.
    logger.discardedEntries = 3;
    logger.testingBufferSize = 20;
    logger.setLogFile("__test.log", true);
    logger.logMessage(true, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
                CodeLocation("file", 95, "func", "pretty"),
                "this is a %s", "fairly long message body");
    EXPECT_EQ("0000000003.50000000... (170 chars truncated)\n",
            getLog("__test.log"));

    // Overflow happens during standard prefix.
    logger.discardedEntries = 3;
    logger.testingBufferSize = 140;
    logger.setLogFile("__test.log", true);
    logger.logMessage(true, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
                CodeLocation("file", 96, "func", "pretty"),
                "this is a %s", "fairly long message body");
    EXPECT_EQ("0000000003.500000000 Logger.cc:xxx in logMessage "
            "WARNING[1]: 3 log messages lost because of buffer overflow\n"
            "0000000003.500000000 file:96 in... (50 chars truncated)\n",
            getLog("__test.log", true));

    // Overflow happens during warning the caller's log message.
    logger.discardedEntries = 3;
    logger.testingBufferSize = 188;
    logger.setLogFile("__test.log", true);
    logger.logMessage(true, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
                CodeLocation("file", 97, "func", "pretty"),
                "this is a %s", "fairly long message body");
    EXPECT_EQ("0000000003.500000000 Logger.cc:xxx in logMessage "
            "WARNING[1]: 3 log messages lost because of buffer overflow\n"
            "0000000003.500000000 file:97 in func ERROR[1]: this is a fairly "
            "long message bo... (2 chars truncated)\n",
            getLog("__test.log", true));

    // No overflow: everything just barely fits.
    logger.discardedEntries = 3;
    logger.testingBufferSize = 190;
    logger.setLogFile("__test.log", true);
    logger.logMessage(true, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
                CodeLocation("file", 98, "func", "pretty"),
                "this is a %s", "fairly long message body");
    EXPECT_EQ("0000000003.500000000 Logger.cc:xxx in logMessage "
            "WARNING[1]: 3 log messages lost because of buffer overflow\n"
            "0000000003.500000000 file:98 in func ERROR[1]: this is a fairly "
            "long message body",
            getLog("__test.log", true));

    // Overflow happens during warning about collapsed entries.
    for (int i = 0; i < 5; i++) {
        logger.logMessage(true, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
                CodeLocation("file", 98, "func", "pretty"),
                "this is a %s", "fairly long message body");
    }
    testTime.tv_nsec += 10000000;
    logger.testingBufferSize = 70;
    logger.setLogFile("__test.log", true);
    logger.logMessage(true, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
                CodeLocation("file", 98, "func", "pretty"),
                "this is a %s", "fairly long message body");
    EXPECT_EQ("0000000003.510000000 file:98 in func ERROR[1]: "
            "(5 duplicates of this ... (56 chars truncated)\n",
            getLog("__test.log"));
}

TEST_F(LoggerTest, cleanCollapseMap_deleteEntries) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.collapseMap[std::make_pair("LoggerTest.cc", 1)]
        = Logger::SkipInfo({1, 200000000}, 0, "message1\n");
    logger.collapseMap[std::make_pair("LoggerTest.cc", 2)]
        = Logger::SkipInfo({1, 400000000}, 0, "message2\n");
    logger.collapseMap[std::make_pair("LoggerTest.cc", 3)]
        = Logger::SkipInfo({1, 600000000}, 0, "message3\n");
    logger.cleanCollapseMap({1, 600000000});
    EXPECT_EQ("", getLog("__test.log"));
    EXPECT_EQ(0U, logger.collapseMap.size());
    EXPECT_EQ(1001, logger.nextCleanTime.tv_sec);
    EXPECT_EQ(600000000, logger.nextCleanTime.tv_nsec);
}

TEST_F(LoggerTest, cleanCollapseMap_print) {
    auto pair1 = std::make_pair("LoggerTest.cc", 1);
    auto pair2 = std::make_pair("LoggerTest.cc", 2);
    auto pair3 = std::make_pair("LoggerTest.cc", 3);
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.collapseMap[pair1] = Logger::SkipInfo({1, 200000000}, 0,
            "0000000001.000000001 WARNING[1]: message1\n");
    logger.collapseMap[pair2] = Logger::SkipInfo({1, 400000000}, 8,
            "0000000001.000000002 WARNING[1]: message2\n");
    logger.collapseMap[pair3] = Logger::SkipInfo({1, 600000000}, 0,
            "0000000001.000000003 WARNING[1]: message3\n");
    logger.cleanCollapseMap({1, 500000000});
    EXPECT_EQ("0000000001.500000000 WARNING[1]: "
            "(7 duplicates of this message were skipped) message2\n",
            getLog("__test.log"));
    EXPECT_EQ(2U, logger.collapseMap.size());
    EXPECT_EQ(6, logger.collapseMap[pair2].nextPrintTime.tv_sec);
    EXPECT_EQ(500000000U,
            logger.collapseMap[pair2].nextPrintTime.tv_nsec);
    EXPECT_EQ(1U, logger.nextCleanTime.tv_sec);
    EXPECT_EQ(600000000U, logger.nextCleanTime.tv_nsec);
}

TEST_F(LoggerTest, addToBuffer) {
    Logger& logger = Logger::get();
    logger.testingNoNotify = true;
    logger.nextToPrint = 15;
    logger.nextToInsert = logger.bufferSize - 20;
    logger.messageBuffer[logger.bufferSize - 5] = 'x';

    // First append: new characters all fit at the end of the buffer
    EXPECT_TRUE(logger.addToBuffer("abcdefghijklmno", 15));
    EXPECT_EQ(logger.bufferSize - 5, logger.nextToInsert);
    string check(logger.messageBuffer + logger.bufferSize - 20, 16);
    EXPECT_EQ("abcdefghijklmnox", check);

    // Second append: must wrap around.
    EXPECT_TRUE(logger.addToBuffer("0123456789ABCD", 14));
    EXPECT_EQ(9, logger.nextToInsert);
    check.assign(logger.messageBuffer + logger.bufferSize - 5, 5);
    EXPECT_EQ("01234", check);
    check.assign(logger.messageBuffer, 9);
    EXPECT_EQ("56789ABCD", check);

    // Third append: not enough space.
    EXPECT_FALSE(logger.addToBuffer("EFGHIJ", 6));
    EXPECT_EQ(9, logger.nextToInsert);
}

TEST_F(LoggerTest, printThreadMain_exit) {
    Tub<Logger> logger;
    TestLog::Enable _;
    logger.construct(NOTICE);
    TestLog::reset();
    logger.destroy();
    EXPECT_EQ("printThreadMain: print thread exiting", TestLog::get());
}
TEST_F(LoggerTest, printThreadMain_wrapAround) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.nextToInsert = logger.nextToPrint = logger.bufferSize - 10;
    logger.addToBuffer("abcdefghijklmnop", 16);
    EXPECT_EQ("abcdefghijklmnop", getLog("__test.log"));
    EXPECT_EQ(0, logger.nextToPrint);
    EXPECT_EQ(0, logger.nextToInsert);
}
// The following test is normally disabled, since it generates output
// on stderr.
#if 0
TEST_F(LoggerTest, printThreadMain_error) {
    Logger& logger = Logger::get();
    logger.fd = 99;
    logger.logMessage(true, RAMCLOUD_CURRENT_LOG_MODULE, ERROR,
                CodeLocation("file", 98, "func", "pretty"),
                "simple message");
    usleep(10000);
    EXPECT_EQ(0, logger.nextToPrint);
    EXPECT_EQ(0, logger.nextToInsert);
}
#endif

TEST_F(LoggerTest, DIE) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    try {
        DIE("rofl: %d", 3);
    } catch (RAMCloud::FatalError& e) {
        EXPECT_TRUE(TestUtil::contains(getLog("__test.log"), "rofl: 3"));
        EXPECT_EQ("rofl: 3", e.message);
        return;
    }
    EXPECT_STREQ("", "FatalError not thrown");
}

TEST_F(LoggerTest, assertionError) {
    Logger& logger = Logger::get();
    logger.setLogFile("__test.log");
    logger.assertionError("assertion info", "file", 99, "function");
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
            "file:99 in function .* Assertion `assertion info' failed",
            getLog("__test.log")));
}

}  // namespace RAMCloud
