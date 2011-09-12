/* Copyright (c) 2009 Stanford University
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

#include <getopt.h>
#include <stdlib.h>
#include <string.h>

#include <gtest/gtest.h>

#include <typeinfo>

#include "Common.h"
#include "ClientException.h"
#include "Dispatch.h"

namespace {

/**
 * Replaces gtest's PrettyUnitTestResultPrinter with something less verbose.
 * This forwards callbacks to the default printer if and when they might be
 * interesting.
 */
class QuietUnitTestResultPrinter : public testing::TestEventListener {
  public:
    /**
     * Constructor.
     * \param prettyPrinter
     *      gtest's default unit test result printer. This object takes
     *      ownership of prettyPrinter and will delete it later.
     */
    explicit QuietUnitTestResultPrinter(TestEventListener* prettyPrinter)
        : prettyPrinter(prettyPrinter)
        , currentTestCase(NULL)
        , currentTestInfo(NULL)
    {}
    void OnTestProgramStart(const testing::UnitTest& unit_test) {
        prettyPrinter->OnTestProgramStart(unit_test);
    }
    void OnTestIterationStart(const testing::UnitTest& unit_test,
                              int iteration) {
        prettyPrinter->OnTestIterationStart(unit_test, iteration);
    }
    void OnEnvironmentsSetUpStart(const testing::UnitTest& unit_test) {}
    void OnEnvironmentsSetUpEnd(const testing::UnitTest& unit_test) {}
    void OnTestCaseStart(const testing::TestCase& test_case) {
        currentTestCase = &test_case;
    }
    void OnTestStart(const testing::TestInfo& test_info) {
        currentTestInfo = &test_info;
    }
    void OnTestPartResult(const testing::TestPartResult& test_part_result) {
        if (test_part_result.type() != testing::TestPartResult::kSuccess) {
            if (currentTestCase != NULL) {
                prettyPrinter->OnTestCaseStart(*currentTestCase);
                currentTestCase = NULL;
            }
            if (currentTestInfo != NULL) {
                prettyPrinter->OnTestStart(*currentTestInfo);
                currentTestInfo = NULL;
            }
            prettyPrinter->OnTestPartResult(test_part_result);
        }
    }
    void OnTestEnd(const testing::TestInfo& test_info) {
        currentTestInfo = NULL;
    }
    void OnTestCaseEnd(const testing::TestCase& test_case) {
        currentTestCase = NULL;
    }
    void OnEnvironmentsTearDownStart(const testing::UnitTest& unit_test) {}
    void OnEnvironmentsTearDownEnd(const testing::UnitTest& unit_test) {}
    void OnTestIterationEnd(const testing::UnitTest& unit_test,
                            int iteration) {
        prettyPrinter->OnTestIterationEnd(unit_test, iteration);
    }
    void OnTestProgramEnd(const testing::UnitTest& unit_test) {
        prettyPrinter->OnTestProgramEnd(unit_test);
    }
  private:
    /// gtest's default unit test result printer.
    std::unique_ptr<TestEventListener> prettyPrinter;
    /// The currently running TestCase that hasn't been printed, or NULL.
    const testing::TestCase* currentTestCase;
    /// The currently running TestInfo that hasn't been printed, or NULL.
    const testing::TestInfo* currentTestInfo;
    DISALLOW_COPY_AND_ASSIGN(QuietUnitTestResultPrinter);
};

char testName[256];
bool progress = false;

void __attribute__ ((noreturn))
usage(char *arg0)
{
    printf("Usage: %s "
            "[-p] [-t testName]\n"
           "\t-t\t--test\tRun a specific test..\n"
           "\t-p\t--progress\tShow test progress.\n",
           "\t-g\t--google\tRun google tests only.\n",
           arg0);
    exit(EXIT_FAILURE);
}

void
cmdline(int argc, char *argv[])
{
    struct option long_options[] = {
        {"test", required_argument, NULL, 't'},
        {"progress", no_argument, NULL, 'p'},
        {"google", no_argument, NULL, 'g'},
        {0, 0, 0, 0},
    };

    int c;
    int i = 0;
    while ((c = getopt_long(argc, argv, "t:pg",
                            long_options, &i)) >= 0)
    {
        switch (c) {
        case 't':
            strncpy(testName, optarg, sizeof(testName));
            testName[sizeof(testName) - 1] = '\0';
            break;
        case 'p':
            progress = true;
            break;
        default:
            usage(argv[0]);
        }
    }
}

} // anonymous namespace

int
main(int argc, char *argv[])
{
    int googleArgc = 0;
    char* googleArgv[] = {NULL};
    ::testing::InitGoogleTest(&googleArgc, googleArgv);

    const char *defaultTest = "";
    strncpy(testName, defaultTest, sizeof(testName));
    cmdline(argc, argv);

    // First run gtest tests.
    // set log levels for gtest unit tests
    struct LoggerEnvironment : public ::testing::Environment {
        void SetUp()
        {
            RAMCloud::logger.setLogLevels(RAMCloud::WARNING);
        }
    };
    ::testing::AddGlobalTestEnvironment(new LoggerEnvironment());

    int r = 0;
    if (!strcmp(testName, defaultTest)) {
        auto unitTest = ::testing::UnitTest::GetInstance();
        if (!progress) {
            auto& listeners = unitTest->listeners();
            auto defaultPrinter = listeners.Release(
                                    listeners.default_result_printer());
            listeners.Append(new QuietUnitTestResultPrinter(defaultPrinter));
        }
        r += unitTest->Run();
    }

    return r;
}
