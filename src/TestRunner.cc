/* Copyright (c) 2009-2013 Stanford University
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

#include <boost/program_options.hpp>

#include "TestUtil.h"

#include "Common.h"
#include "AbstractServerList.h"
#include "CoordinatorService.h"
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
        ("help,h", "Produce this help message")
        ("verbose,v", "Show test progress");
    po::variables_map vm;
    po::parsed_options parsed = po::command_line_parser(argc, argv).options(
        configOptions).allow_unregistered().run();
    po::store(parsed, vm);
    po::notify(vm);
    std::vector<std::string> unrecognizedOptions = po::collect_unrecognized(
        parsed.options, po::include_positional);
    if (vm.count("help")) {
        std::cerr << configOptions << std::endl;

        std::cerr << "-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-" << std::endl;
        std::cerr << "The following arguments are passed to gtest" << std::endl;
        std::cerr << "-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-" << std::endl;

        int googleArgc = 2;
        char* googleArgv[] = {
            strdup(argv[0]),    // Expects an executable name in argv[0] to skip
            strdup("--help"),
            NULL
        };
        ::testing::InitGoogleTest(&googleArgc, googleArgv);

        exit(1);
    }
    progress = vm.count("verbose");

    int googleArgc = 1 + static_cast<int>(unrecognizedOptions.size());
    char** googleArgv = new char*[googleArgc + 1];
    googleArgv[0] = strdup(argv[0]);

    char** nextArgv = &googleArgv[1];
    foreach (std::string& gtestOption, unrecognizedOptions) {
        *nextArgv++ = strdup(gtestOption.c_str());
    }
    googleArgv[googleArgc] = NULL;

    ::testing::InitGoogleTest(&googleArgc, googleArgv);

    auto unitTest = ::testing::UnitTest::GetInstance();
    auto& listeners = unitTest->listeners();

    // set up the environment for unit tests
    struct GTestSetupListener : public ::testing::EmptyTestEventListener {
        GTestSetupListener() {}
        // this fires before each test fixture's constructor
        void OnTestStart(const ::testing::TestInfo& testInfo) {
            // Common initialization for all tests:
            RAMCloud::Logger::get().setLogLevels(RAMCloud::WARNING);
            RAMCloud::AbstractServerList::skipServerIdCheck = true;
            RAMCloud::CoordinatorService::forceSynchronousInit = true;
        }
        // this fires after each test fixture's destructor
        void OnTestEnd(const ::testing::TestInfo& testInfo) {
            // Currently does nothing.
        }
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
