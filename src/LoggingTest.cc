/* Copyright (c) 2010-2011 Stanford University
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

namespace RAMCloud {

class LoggingTest : public ::testing::Test {
  public:
    static const uint32_t numLogLevels = static_cast<uint32_t>(NUM_LOG_LEVELS);

    LoggingTest() {}

    ~LoggingTest() {
        logger.setLogLevels(WARNING);
        logger.stream = stderr;
    }
    DISALLOW_COPY_AND_ASSIGN(LoggingTest);
};

TEST_F(LoggingTest, constructor) {
    Logger l(WARNING);
    EXPECT_EQ(stderr, l.stream);
    EXPECT_EQ(WARNING, l.logLevels[0]);
}

TEST_F(LoggingTest, setLogLevel) {
    Logger l(WARNING);
    l.setLogLevel(DEFAULT_LOG_MODULE, NOTICE);
    EXPECT_EQ(NOTICE, l.logLevels[DEFAULT_LOG_MODULE]);
}

TEST_F(LoggingTest, setLogLevel_int) {
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

TEST_F(LoggingTest, setLogLevel_string) {
    logger.setLogLevels(SILENT_LOG_LEVEL);
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

TEST_F(LoggingTest, changeLogLevel) {
    Logger l(WARNING);
    l.changeLogLevel(DEFAULT_LOG_MODULE, -1);
    EXPECT_EQ(ERROR, l.logLevels[DEFAULT_LOG_MODULE]);
    l.changeLogLevel(DEFAULT_LOG_MODULE, 1);
    EXPECT_EQ(WARNING, l.logLevels[DEFAULT_LOG_MODULE]);
}

TEST_F(LoggingTest, setLogLevels) {
    Logger l(WARNING);
    l.setLogLevels(NOTICE);
    for (int i = 0; i < NUM_LOG_MODULES; i++)
        EXPECT_EQ(NOTICE, l.logLevels[i]);
}

TEST_F(LoggingTest, setLogLevels_int) {
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

TEST_F(LoggingTest, setLogLevels_string) {
    logger.setLogLevels(SILENT_LOG_LEVEL);
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

TEST_F(LoggingTest, changeLogLevels) {
    Logger l(WARNING);

    l.changeLogLevels(-1);
    for (int i = 0; i < NUM_LOG_MODULES; i++)
        EXPECT_EQ(ERROR, l.logLevels[i]);

    l.changeLogLevels(1);
    for (int i = 0; i < NUM_LOG_MODULES; i++)
        EXPECT_EQ(WARNING, l.logLevels[i]);
}

TEST_F(LoggingTest, isLogging) {
    Logger l(WARNING);
    EXPECT_TRUE(l.isLogging(DEFAULT_LOG_MODULE, ERROR));
    EXPECT_TRUE(l.isLogging(DEFAULT_LOG_MODULE, WARNING));
    EXPECT_TRUE(!l.isLogging(DEFAULT_LOG_MODULE, NOTICE));
}

TEST_F(LoggingTest, LOG) { // also tests logMessage
    char* buf = NULL;
    size_t size = 0;

    logger.stream = open_memstream(&buf, &size);
    assert(logger.stream != NULL);

    LOG(DEBUG, "x");
    EXPECT_EQ(0U, size);

    LOG(ERROR, "rofl: %d", 3);
    const char* pattern = "^[[:digit:]]\\{10\\}\\.[[:digit:]]\\{9\\} "
                            "src/LoggingTest.cc:[[:digit:]]\\{1,4\\} "
                            "in LoggingTest_LOG_Test::TestBody "
                            "default ERROR\\[[[:digit:]]\\{1,5\\}\\]: "
                            "rofl: 3\n$";
    EXPECT_TRUE(TestUtil::matchesPosixRegex(pattern, buf));

    fclose(logger.stream);
    free(buf);
}

TEST_F(LoggingTest, DIE) { // also tests getMessage
    logger.stream = fmemopen(NULL, 1024, "w");
    assert(logger.stream != NULL);
    try {
        DIE("rofl: %d", 3);
    } catch (RAMCloud::FatalError& e) {
        int64_t streamPos = ftell(logger.stream);
        fclose(logger.stream);
        EXPECT_GT(streamPos, 0);
        EXPECT_EQ("rofl: 3", e.message);
        return;
    }
    fclose(logger.stream);
    EXPECT_STREQ("", "FatalError not thrown");
}

}  // namespace RAMCloud
