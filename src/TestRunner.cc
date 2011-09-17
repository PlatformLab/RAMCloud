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

#include <gtest/gtest.h>
#include <boost/program_options.hpp>

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

} // anonymous namespace

int
main(int argc, char *argv[])
{
    namespace po = boost::program_options;
    po::options_description configOptions("TestRunner");
    configOptions.add_options()
        ("help,h", "Produce help message")
        ("verbose,v",
         po::value<bool>(&progress)->
            default_value(false),
         "Show test progress");
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, configOptions), vm);
    po::notify(vm);
    if (vm.count("help")) {
        std::cerr << configOptions << std::endl;
        exit(1);
    }

    int googleArgc = 0;
    char* googleArgv[] = {NULL};
    ::testing::InitGoogleTest(&googleArgc, googleArgv);

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
