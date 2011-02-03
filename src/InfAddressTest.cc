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
#include "Infiniband.h"

namespace RAMCloud {

typedef RealInfiniband Infiniband;

class InfAddressTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(InfAddressTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_toString);
    CPPUNIT_TEST_SUITE_END();

  public:
    InfAddressTest() {}
    char x[0];

    string tryLocator(const char *locator) {
        try {
            // dangerous cast!
            Infiniband::Address(*reinterpret_cast<RealInfiniband*>(x), 0,
                       ServiceLocator(locator));
        } catch (Infiniband::Address::BadAddressException& e) {
            return e.message;
        }
        return "ok";
    }

    void test_constructor() {
        CPPUNIT_ASSERT_EQUAL("ok", tryLocator("fast+infud: lid=0, qpn=0"));
        CPPUNIT_ASSERT_EQUAL("ok",
            tryLocator("fast+infud: lid=65535, qpn=4294967295"));

        CPPUNIT_ASSERT_EQUAL("Service locator 'fast+infud: lid=65536, qpn=0' "
            "couldn't be converted to Infiniband address: Could not parse lid. "
            "Invalid or out of range.",
            tryLocator("fast+infud: lid=65536, qpn=0"));

        CPPUNIT_ASSERT_EQUAL("Service locator 'fast+infud: lid=0, "
            "qpn=4294967296' couldn't be converted to Infiniband address: "
            "Could not parse qpn. Invalid or out of range.",
            tryLocator("fast+infud: lid=0, qpn=4294967296"));

        CPPUNIT_ASSERT_EQUAL("Service locator 'fast+infud: foo=0, qpn=0' "
            "couldn't be converted to Infiniband address: Could not parse "
            "lid. Invalid or out of range.",
            tryLocator("fast+infud: foo=0, qpn=0"));

        CPPUNIT_ASSERT_EQUAL("Service locator 'fast+infud: lid=0, bar=0' "
            "couldn't be converted to Infiniband address: Could not parse "
            "qpn. Invalid or out of range.",
            tryLocator("fast+infud: lid=0, bar=0"));
    }

    void test_toString() {
        // dangerous cast!
        Infiniband::Address a(*reinterpret_cast<RealInfiniband*>(x), 0,
                          ServiceLocator("fast+infud: lid=721, qpn=23472"));
        CPPUNIT_ASSERT_EQUAL("721:23472", a.toString());
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(InfAddressTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(InfAddressTest);

}  // namespace RAMCloud
