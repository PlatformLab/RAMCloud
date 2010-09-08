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
#include <regex.h>
#include "Common.h"

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

void assertMatchesPosixRegex(const char* pattern, const char* subject);
void bufToString(const char *buf, uint32_t length, string* const s);

} // namespace RAMCloud

#endif  // RAMCLOUD_TESTUTIL_H
