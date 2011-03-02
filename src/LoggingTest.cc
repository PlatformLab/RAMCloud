/* Copyright (c) 2010 Stanford University
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

class LoggingTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(LoggingTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_setLogLevel);
    CPPUNIT_TEST(test_setLogLevel_int);
    CPPUNIT_TEST(test_setLogLevel_string);
    CPPUNIT_TEST(test_changeLogLevel);
    CPPUNIT_TEST(test_setLogLevels);
    CPPUNIT_TEST(test_setLogLevels_int);
    CPPUNIT_TEST(test_setLogLevels_string);
    CPPUNIT_TEST(test_changeLogLevels);
    CPPUNIT_TEST(test_isLogging);
    CPPUNIT_TEST(test_LOG);
    CPPUNIT_TEST(test_DIE);
    CPPUNIT_TEST_SUITE_END();

    static const uint32_t numLogLevels = static_cast<uint32_t>(NUM_LOG_LEVELS);

  public:

    void setUp() {
        logger.setLogLevels(NOTICE);
    }

    void tearDown() {
        logger.stream = stderr;
    }

    void test_constructor() {
        Logger l(WARNING);
        CPPUNIT_ASSERT_EQUAL(stderr, l.stream);
        CPPUNIT_ASSERT_EQUAL(WARNING, l.logLevels[0]);
    }

    void test_setLogLevel() {
        Logger l(WARNING);
        l.setLogLevel(DEFAULT_LOG_MODULE, NOTICE);
        CPPUNIT_ASSERT_EQUAL(NOTICE, l.logLevels[DEFAULT_LOG_MODULE]);
    }

    void test_setLogLevel_int() {
        Logger l(WARNING);

        l.setLogLevel(DEFAULT_LOG_MODULE, -1);
        CPPUNIT_ASSERT_EQUAL(0, l.logLevels[DEFAULT_LOG_MODULE]);

        l.setLogLevel(DEFAULT_LOG_MODULE, numLogLevels);
        CPPUNIT_ASSERT_EQUAL(numLogLevels - 1,
                             l.logLevels[DEFAULT_LOG_MODULE]);

        l.setLogLevel(DEFAULT_LOG_MODULE, 0);
        CPPUNIT_ASSERT_EQUAL(0, l.logLevels[DEFAULT_LOG_MODULE]);

        l.setLogLevel(DEFAULT_LOG_MODULE, numLogLevels - 1);
        CPPUNIT_ASSERT_EQUAL(numLogLevels - 1,
                             l.logLevels[DEFAULT_LOG_MODULE]);
    }

    void test_setLogLevel_string() {
        logger.setLogLevels(SILENT_LOG_LEVEL);
        Logger l(WARNING);

        l.setLogLevel("default", "-1");
        CPPUNIT_ASSERT_EQUAL(0, l.logLevels[DEFAULT_LOG_MODULE]);

        l.setLogLevel("default", "1");
        CPPUNIT_ASSERT_EQUAL(1, l.logLevels[DEFAULT_LOG_MODULE]);

        l.setLogLevel("default", "NOTICE");
        CPPUNIT_ASSERT_EQUAL(NOTICE, l.logLevels[DEFAULT_LOG_MODULE]);

        l.setLogLevel("transport", "1");
        CPPUNIT_ASSERT_EQUAL(1, l.logLevels[TRANSPORT_MODULE]);

        TestLog::Enable _;
        l.setLogLevel("stabYourself", "1");
        l.setLogLevel("default", "");
        l.setLogLevel("default", "junk");
        CPPUNIT_ASSERT_EQUAL(
            "setLogLevel: Ignoring bad log module name: stabYourself | "
            "setLogLevel: Ignoring bad log module level:  | "
            "setLogLevel: Ignoring bad log module level: junk", TestLog::get());
    }

    void test_changeLogLevel() {
        Logger l(WARNING);
        l.changeLogLevel(DEFAULT_LOG_MODULE, -1);
        CPPUNIT_ASSERT_EQUAL(ERROR, l.logLevels[DEFAULT_LOG_MODULE]);
        l.changeLogLevel(DEFAULT_LOG_MODULE, 1);
        CPPUNIT_ASSERT_EQUAL(WARNING, l.logLevels[DEFAULT_LOG_MODULE]);
    }

    void test_setLogLevels() {
        Logger l(WARNING);
        l.setLogLevels(NOTICE);
        for (int i = 0; i < NUM_LOG_MODULES; i++)
            CPPUNIT_ASSERT_EQUAL(NOTICE, l.logLevels[i]);
    }

    void test_setLogLevels_int() {
        Logger l(WARNING);

        l.setLogLevels(-1);
        for (int i = 0; i < NUM_LOG_MODULES; i++)
            CPPUNIT_ASSERT_EQUAL(0, l.logLevels[i]);

        l.setLogLevels(numLogLevels);
        for (int i = 0; i < NUM_LOG_MODULES; i++)
            CPPUNIT_ASSERT_EQUAL(numLogLevels - 1, l.logLevels[i]);

        l.setLogLevels(0);
        for (int i = 0; i < NUM_LOG_MODULES; i++)
            CPPUNIT_ASSERT_EQUAL(0, l.logLevels[i]);

        l.setLogLevels(numLogLevels - 1);
        for (int i = 0; i < NUM_LOG_MODULES; i++)
            CPPUNIT_ASSERT_EQUAL(numLogLevels - 1, l.logLevels[i]);
    }

    void test_setLogLevels_string() {
        logger.setLogLevels(SILENT_LOG_LEVEL);
        Logger l(WARNING);

        l.setLogLevels("-1");
        for (int i = 0; i < NUM_LOG_MODULES; i++)
            CPPUNIT_ASSERT_EQUAL(0, l.logLevels[i]);

        l.setLogLevels("2");
        for (int i = 0; i < NUM_LOG_MODULES; i++)
            CPPUNIT_ASSERT_EQUAL(2, l.logLevels[i]);

        l.setLogLevels("NOTICE");
        for (int i = 0; i < NUM_LOG_MODULES; i++)
            CPPUNIT_ASSERT_EQUAL(NOTICE, l.logLevels[i]);

        TestLog::Enable _;
        l.setLogLevels("oral trauma");
        CPPUNIT_ASSERT_EQUAL(
            "setLogLevels: Ignoring bad log module level: oral trauma",
            TestLog::get());
    }

    void test_changeLogLevels() {
        Logger l(WARNING);

        l.changeLogLevels(-1);
        for (int i = 0; i < NUM_LOG_MODULES; i++)
            CPPUNIT_ASSERT_EQUAL(ERROR, l.logLevels[i]);

        l.changeLogLevels(1);
        for (int i = 0; i < NUM_LOG_MODULES; i++)
            CPPUNIT_ASSERT_EQUAL(WARNING, l.logLevels[i]);
    }

    void test_isLogging() {
        Logger l(WARNING);
        CPPUNIT_ASSERT(l.isLogging("UnknownFileGetsDefaultModule.cc", ERROR));
        CPPUNIT_ASSERT(l.isLogging("UnknownFileGetsDefaultModule.cc", WARNING));
        CPPUNIT_ASSERT(!l.isLogging("UnknownFileGetsDefaultModule.cc", NOTICE));
    }

    void test_LOG() { // also tests logMessage
        char* buf = NULL;
        size_t size = 0;

        logger.stream = open_memstream(&buf, &size);
        assert(logger.stream != NULL);

        LOG(DEBUG, "x");
        CPPUNIT_ASSERT_EQUAL(0, size);

        LOG(ERROR, "rofl: %d", 3);
        const char* pattern = "^[[:digit:]]\\{10\\}\\.[[:digit:]]\\{9\\} "
                              "src/LoggingTest.cc:[[:digit:]]\\{1,4\\} "
                              "in LoggingTest::test_LOG "
                              "default ERROR\\[[[:digit:]]\\{1,5\\}\\]: "
                              "rofl: 3\n$";
        assertMatchesPosixRegex(pattern, buf);

        fclose(logger.stream);
        free(buf);
    }

    void test_DIE() { // also tests getMessage
        logger.stream = fmemopen(NULL, 1024, "w");
        assert(logger.stream != NULL);
        try {
            DIE("rofl: %d", 3);
        } catch (RAMCloud::FatalError& e) {
            int64_t streamPos = ftell(logger.stream);
            fclose(logger.stream);
            CPPUNIT_ASSERT(streamPos > 0);
            CPPUNIT_ASSERT_EQUAL("rofl: 3", e.message);
            return;
        }
        fclose(logger.stream);
        CPPUNIT_FAIL("FatalError not thrown");
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(LoggingTest);

}  // namespace RAMCloud
