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

#include <cppunit/CompilerOutputter.h>
#include <cppunit/Message.h>
#include <cppunit/Protector.h>
#include <cppunit/TestResult.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/extensions/TypeInfoHelper.h>
#include <cppunit/ui/text/TestRunner.h>

#include <typeinfo>

#include "Common.h"

char testName[256];
bool progress = false;

void __attribute__ ((noreturn))
usage(char *arg0)
{
    printf("Usage: %s "
            "[-p] [-t testName]\n"
           "\t-t\t--test\tRun a specific test..\n"
           "\t-p\t--progress\tShow test progress.\n",
           arg0);
    exit(EXIT_FAILURE);
}

void
cmdline(int argc, char *argv[])
{
    struct option long_options[] = {
        {"test", required_argument, NULL, 't'},
        {"progress", no_argument, NULL, 'p'},
        {0, 0, 0, 0},
    };

    int c;
    int i = 0;
    while ((c = getopt_long(argc, argv, "t:p",
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

int
main(int argc, char *argv[])
{
    const char *defaultTest = "";
    strncpy(testName, defaultTest, sizeof(testName));
    cmdline(argc, argv);

    CppUnit::TextUi::TestRunner runner;
    CppUnit::TestFactoryRegistry& registry =
            CppUnit::TestFactoryRegistry::getRegistry();

    // This thing will print RAMCloud::Exception::message when RAMCloud
    // exceptions are thrown in our unit tests.
    class RAMCloudProtector : public CppUnit::Protector {
        bool protect(const CppUnit::Functor& functor,
                     const CppUnit::ProtectorContext& context) {
            try {
                return functor();
            } catch (const RAMCloud::Exception& e) {
                std::string className(
                    CppUnit::TypeInfoHelper::getClassName(typeid(e)));
                CppUnit::Message message(className + ":\n    " + e.message);
                reportError(context, message);
            }
            return false;
        }
    };
    // CppUnit's ProtectorChain::pop() will call delete on our protector, so I
    // guess they want us to use new to allocate it.
    runner.eventManager().pushProtector(new RAMCloudProtector());

    runner.addTest(registry.makeTest());
    return !runner.run(testName, false, true, progress);
}
