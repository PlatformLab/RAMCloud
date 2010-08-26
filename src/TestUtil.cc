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
 * Defines various things that help in writing tests, such as
 * extensions of CPPUNIT_ASSERT_EQUAL.
 */

#include <TestUtil.h>

// The following code extends CppUnit to enable CPPUNIT_ASSERT_EQUAL
// to be used on some additional combinations of types that aren't
// supported by default.
namespace CppUnit {
// Note: the recommended way to extend CPPUNIT_ASSERT_EQUAL is to
// define assertion_traits objects.  However, all of the extensions
// below required a different approach, because assertion_traits
// objects didn't produce the desired result.

#if 0
// This is the recommended way to enable CPPUNIT_ASSERT_EQUAL
// comparisons between char *'s.  Unfortunately it doesn't seem to
// work reliably (compiler bugs?). The compiler seems to choose
// the default (less specialized) implementation in place of this
// one.
template<>
struct assertion_traits<char *> {
    static bool equal(const char *x, const char *y) {
        return strcmp(x, y) == 0;
    }

    static std::string toString(const char *x) {
        return std::string(x);
    }
};
#endif

// Allow CPPUNIT_ASSERT_EQUAL comparisons between char* strings.
// This functionality has to be implemented using the non-standard
// approach below because the assertion_traits approach doesn't
// seem to work (the compiler picks the wrong template).  Even the
// approach below occasionally fails, requiring arguments to
// be cast to (char *).
void assertEquals(const char *expected, const char *actual,
        SourceLine sourceLine, const std::string &message) {
    if (strcmp(actual, expected) != 0) {
        Asserter::failNotEqual(std::string(expected), std::string(actual),
                sourceLine, message);
    }
}

// Allow CPPUNIT_ASSERT_EQUAL comparisons between char* and std::string.
// This functionality has to be implemented using the non-standard
// approach below because the types of the arguments are different.
void assertEquals(const char *expected, const std::string& actual,
        SourceLine sourceLine, const std::string &message) {
    if (actual != expected) {
        Asserter::failNotEqual(std::string(expected),
            assertion_traits<std::string>::toString(actual),
                sourceLine, message);
    }
}

// Allow CPPUNIT_ASSERT_EQUAL comparisons between uint32_t's.
// This functionality has to be implemented using the non-standard
// approach below because we sometimes supply an enum value for
// the first argument; the approach below will automatically
// convert it to integer, but the assertion_traits approach
// will not, so the types won't match.
void assertEquals(uint32_t expected, const uint32_t actual,
        SourceLine sourceLine, const std::string &message) {
    if (expected != actual) {
        char buf1[20], buf2[20];
        snprintf(buf1, sizeof(buf1), "%d", expected);
        snprintf(buf2, sizeof(buf2), "%d", actual);
        Asserter::failNotEqual(std::string(buf1), std::string(buf2),
                sourceLine, message);
    }
}

// Allow CPPUNIT_ASSERT_EQUAL comparisons between void *'s.
// This functionality has to be implemented using the non-standard
// approach below because we sometimes supply a char* value for
// the first argument; the approach below will automatically
// convert it to void*, but the assertion_traits approach
// will not, so the types won't match.
void assertEquals(void *expected, const void *actual,
        SourceLine sourceLine, const std::string &message) {
    if (expected != actual) {
        char buf1[20], buf2[20];
        snprintf(buf1, sizeof(buf1), "%p", expected);
        snprintf(buf2, sizeof(buf2), "%p", actual);
        Asserter::failNotEqual(std::string(buf1), std::string(buf2),
                sourceLine, message);
    }
}

} // namespace CppUnit

namespace RAMCloud {

/**
 * A wrapper around regerror(3) that returns a std::string.
 * \param errorCode
 *      See regerror(3).
 * \param storage
 *      See regerror(3).
 * \return
 *      The full error message from regerror(3).
 */
static string
friendlyRegerror(int errorCode, const regex_t* storage)
{
    size_t errorBufSize = regerror(errorCode, storage, NULL, 0);
    char errorBuf[errorBufSize];
    size_t errorBufSize2 = regerror(errorCode, storage, errorBuf,
                                    errorBufSize);
    assert(errorBufSize == errorBufSize2);
    return errorBuf;
}

/**
 * Fail the CPPUNIT test case if the given string doesn't match the given POSIX
 * regular expression.
 * \param pattern
 *      A POSIX regular expression.
 * \param subject
 *      The string that should match \a pattern.
 */
void
assertMatchesPosixRegex(const char* pattern, const char* subject)
{
    regex_t pregStorage;
    int r;

    r = regcomp(&pregStorage, pattern, 0);
    if (r != 0) {
        string errorMsg = "Pattern '";
        errorMsg += pattern;
        errorMsg += "' failed to compile: ";
        errorMsg += friendlyRegerror(r, &pregStorage);
        CPPUNIT_FAIL(errorMsg);
    }

    r = regexec(&pregStorage, subject, 0, NULL, 0);
    if (r != 0) {
        string errorMsg = "Pattern '";
        errorMsg += pattern;
        errorMsg += "' did not match subject '";
        errorMsg += subject;
        errorMsg += "'";
        regfree(&pregStorage);
        CPPUNIT_FAIL(errorMsg);
    }

    regfree(&pregStorage);
}

} // namespace RAMCloud
