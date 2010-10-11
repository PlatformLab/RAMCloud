/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "OptionParser.h"

namespace RAMCloud {

namespace {
const char* localLocator = "fast+udp:host=1.2.3.4,port=54321";
const char* coordinatorLocator = "tcp:ip=4.3.2.1,port=12345";
}

class OptionParserTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(OptionParserTest);
    CPPUNIT_TEST(test_constructor_noAppSpecific);
    CPPUNIT_TEST(test_constructor_appSpecific);
    CPPUNIT_TEST_SUITE_END();

  public:

    void
    setUp()
    {
    }

    void
    tearDown()
    {
    }

    OptionParserTest()
    {
    }

    void
    test_constructor_noAppSpecific()
    {
        int argc = 5;
        const char* argv[] = { "fooprogram"
                             , "-L", localLocator
                             , "-C", coordinatorLocator
                             };
        OptionParser parser(argc, const_cast<char**>(argv));

        CPPUNIT_ASSERT_EQUAL(localLocator, parser.options.getLocalLocator());
        CPPUNIT_ASSERT_EQUAL(coordinatorLocator,
                             parser.options.getCoordinatorLocator());
    }

    void
    test_constructor_appSpecific()
    {
        bool value = false;
        int argc = 6;
        const char* argv[] = { "fooprogram"
                             , "-L", localLocator
                             , "-C", coordinatorLocator
                             , "-t"
                             };
        OptionsDescription appOptions;
        appOptions.add_options()
            ("test,t", ProgramOptions::bool_switch(&value), "test message");
        OptionParser parser(appOptions, argc, const_cast<char**>(argv));

        CPPUNIT_ASSERT_EQUAL(true, value);
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(OptionParserTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(OptionParserTest);

}  // namespace RAMCloud
