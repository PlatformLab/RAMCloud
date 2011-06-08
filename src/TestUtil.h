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

/**
 * \file
 * Declares various things that help in writing tests, such as
 * extensions of CPPUNIT_ASSERT_EQUAL.
 */

#ifndef RAMCLOUD_TESTUTIL_H
#define RAMCLOUD_TESTUTIL_H

#include <cppunit/extensions/HelperMacros.h>
#include <gtest/gtest.h>
#include <regex.h>

// Arrange for private and protected structure members to be public so they
// can easily be accessed by gtest tests (see Common.h for details).
#ifdef RAMCLOUD_COMMON_H
#error "TestUtil.h must be included before Common.h"
#endif
#define EXPOSE_PRIVATES

#include "Common.h"
#include "Buffer.h"
#include "ClientException.h"
#include "Dispatch.h"
#include "Transport.h"

// The following redefinitions are based on CppUnit code, so they probably need
// to be licensed under the LGPL.
// Updated to support c++0x
#undef CPPUNIT_TEST_SUITE_END
#define CPPUNIT_TEST_SUITE_END()                                               \
        }                                                                      \
                                                                               \
    static CPPUNIT_NS::TestSuite *suite()                                      \
    {                                                                          \
              const CPPUNIT_NS::TestNamer &namer = getTestNamer__();           \
              std::unique_ptr<CPPUNIT_NS::TestSuite> suite(                    \
                  new CPPUNIT_NS::TestSuite(namer.getFixtureName()));          \
              CPPUNIT_NS::ConcretTestFixtureFactory<TestFixtureType> factory;  \
              CPPUNIT_NS::TestSuiteBuilderContextBase context(*suite.get(),    \
                                                              namer,           \
                                                              factory);        \
              TestFixtureType::addTestsToSuite(context);                       \
              return suite.release();                                          \
            }                                                                  \
  private: /* dummy typedef so that the macro can still end with ';'*/         \
    typedef int CppUnitDummyTypedefForSemiColonEnding__

namespace CppUnit {

extern void assertEquals(const char *expected, const char *actual,
        SourceLine sourceLine, const std::string &message);
void assertEquals(const char *expected, const std::string& actual,
        SourceLine sourceLine, const std::string &message);
void assertEquals(uint64_t expected, const uint64_t actual,
        SourceLine sourceLine, const std::string &message);
void assertEquals(void *expected, const void *actual,
        SourceLine sourceLine, const std::string &message);

} // namespace CppUnit

namespace RAMCloud {

void assertMatchesPosixRegex(const string& pattern, const string& subject);
void assertNotMatchesPosixRegex(const string& pattern, const string& subject);
void convertChar(char c, string *out);
string bufferToDebugString(Buffer* buffer);
string checkLargeBuffer(Buffer* buffer, int expectedLength);
void fillLargeBuffer(Buffer* buffer, int size);
const char *getStatus(Buffer* buffer);
string toString(const char *buf, uint32_t length);
string toString(Buffer* buffer);
bool waitForRpc(Transport::ClientRpc& rpc);

} // namespace RAMCloud

#endif  // RAMCLOUD_TESTUTIL_H
