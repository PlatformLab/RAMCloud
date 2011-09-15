/* Copyright (c) 2009-2011 Stanford University
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
#include <gtest/gtest.h>

#include "Common.h"
#include "Tub.h"

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

bool progress = false;

void __attribute__ ((noreturn))
usage(char *arg0)
{
    printf("Usage: %s "
            "[-p]\n"
           "\t-p\t--progress\tShow test progress.\n",
           arg0);
    exit(EXIT_FAILURE);
}

void
cmdline(int argc, char *argv[])
{
    struct option long_options[] = {
        {"progress", no_argument, NULL, 'p'},
        {0, 0, 0, 0},
    };

    int c;
    int i = 0;
    while ((c = getopt_long(argc, argv, ":p",
                            long_options, &i)) >= 0)
    {
        switch (c) {
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

    cmdline(argc, argv);

    auto unitTest = ::testing::UnitTest::GetInstance();
    auto& listeners = unitTest->listeners();

    // set up the environment for unit tests
    struct GTestSetupListener : public ::testing::EmptyTestEventListener {
        GTestSetupListener()
            : testContext()
            , scopedContext() {}
        // this fires before each test fixture's constructor
        void OnTestStart(const ::testing::TestInfo& testInfo) {
            testContext.construct(false);
            scopedContext.construct(*testContext);
            testContext->logger->setLogLevels(RAMCloud::WARNING);
        }
        // this fires after each test fixture's destructor
        void OnTestEnd(const ::testing::TestInfo& testInfo) {
            scopedContext.destroy();
            testContext.destroy();
        }
        RAMCloud::Tub<RAMCloud::Context> testContext;
        RAMCloud::Tub<RAMCloud::Context::Guard> scopedContext;
    };
    listeners.Append(new GTestSetupListener());

    if (!progress) {
        // replace default output printer with quiet one
        auto defaultPrinter = listeners.Release(
                                listeners.default_result_printer());
        listeners.Append(new QuietUnitTestResultPrinter(defaultPrinter));
    }

    return RUN_ALL_TESTS();
}
