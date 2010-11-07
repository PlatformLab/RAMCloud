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
#include "IpAddress.h"

namespace RAMCloud {

class IpAddressTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(IpAddressTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_toString);
    CPPUNIT_TEST_SUITE_END();

  public:
    IpAddressTest() {}

    // Used to save message from exceptions in situations where the
    // exception object is too transient.
    char message[200];

    string tryLocator(const char *locator) {
        try {
            IpAddress(ServiceLocator(locator));
        } catch (IpAddress::BadIpAddressException& e) {
            return e.message;
        }
        return "ok";
    }

    void test_constructor() {
        CPPUNIT_ASSERT_EQUAL("ok",
                tryLocator("fast+udp: host=171.67.64.21, port=80"));
        CPPUNIT_ASSERT_EQUAL("ok",
                tryLocator("fast+udp: host=localhost, port=80"));
        CPPUNIT_ASSERT_EQUAL("Service locator 'fast+udp: "
                "host=garbage.host.name, port=80' couldn't be converted "
                "to IP address: couldn't find host 'garbage.host.name'",
                tryLocator("fast+udp: host=garbage.host.name, port=80"));
        CPPUNIT_ASSERT_EQUAL("Service locator 'fast+udp: port=80' couldn't "
                "be converted to IP address: The option with key 'host' "
                "was not found in the ServiceLocator.",
                tryLocator("fast+udp: port=80"));
        CPPUNIT_ASSERT_EQUAL(
                "Service locator 'fast+udp: host=localhost' couldn't "
                "be converted to IP address: The option with key 'port' "
                "was not found in the ServiceLocator.",
                tryLocator("fast+udp: host=localhost"));
        CPPUNIT_ASSERT_EQUAL("Service locator 'fast+udp: "
                "host=localhost, port=badInteger' couldn't be "
                "converted to IP address: bad lexical cast: source "
                "type value could not be interpreted as target"
                "\nCould not convert from source type std::string"
                " to target type unsigned short\n",
                tryLocator("fast+udp: host=localhost, port=badInteger"));
    }

    void test_toString() {
        IpAddress a(ServiceLocator("fast+udp: host=171.67.64.21, port=80"));
        CPPUNIT_ASSERT_EQUAL("171.67.64.21:80", a.toString());
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(IpAddressTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(IpAddressTest);

}  // namespace RAMCloud
