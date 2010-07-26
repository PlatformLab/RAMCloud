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

/**
 * \file
 * Unit tests for debug logs.
 */

#include <TestUtil.h>
#include <regex.h>

namespace RAMCloud {

class LoggerTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(LoggerTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_setLogLevel);
    CPPUNIT_TEST(test_setLogLevel_int);
    CPPUNIT_TEST(test_changeLogLevel);
    CPPUNIT_TEST(test_setLogLevels);
    CPPUNIT_TEST(test_setLogLevels_int);
    CPPUNIT_TEST(test_changeLogLevels);
    CPPUNIT_TEST(test_isLogging);
    CPPUNIT_TEST(test_LOG);
    CPPUNIT_TEST_SUITE_END();

    static const uint32_t numLogLevels = static_cast<uint32_t>(NUM_LOG_LEVELS);

  public:

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
        CPPUNIT_ASSERT(l.isLogging(DEFAULT_LOG_MODULE, ERROR));
        CPPUNIT_ASSERT(l.isLogging(DEFAULT_LOG_MODULE, WARNING));
        CPPUNIT_ASSERT(!l.isLogging(DEFAULT_LOG_MODULE, NOTICE));
    }

    void test_LOG() { // also tests logMessage
        char* buf = NULL;
        size_t size = 0;

        logger.stream = open_memstream(&buf, &size);
        assert(logger.stream != NULL);

        LOG(DEBUG, "x");
        CPPUNIT_ASSERT_EQUAL(0, size);

        LOG(ERROR, "rofl: %d", 3);
        const char* pattern = "^[[:digit:]]\\{10\\}\\.[[:digit:]]\\{6\\} "
                              "src/LoggingTest.cc:[[:digit:]]\\{1,4\\} "
                              "default ERROR: rofl: 3\n$";
        regex_t pregStorage;
        assert(regcomp(&pregStorage, pattern, 0) == 0);
        int r = regexec(&pregStorage, buf, 0, NULL, 0);
        if (r != 0) {
            fprintf(stderr, "test_LOG: '%s' returned %d\n", buf, r);
            CPPUNIT_ASSERT(false);
        }
        regfree(&pregStorage);

        free(buf);
        fclose(logger.stream);
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(LoggerTest);

}  // namespace RAMCloud
