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

#include <string.h>
#include "TestUtil.h"

using namespace RAMCloud;

// The following code extends CppUnit to enable CPPUNIT_ASSERT_EQUAL
// to be used on some additional combinations of types that aren't
// supported by default.
namespace CppUnit {

//
// Note: the recommended way to extend CPPUNIT_ASSERT_EQUAL is to
// define assertion_traits objects.  However, all of the extensions
// below required a different approach, because assertion_traits
// objects didn't produce the desired result.

#if 0
// This is the recommended way to enable CPPUNIT_ASSERT_EQUAL
// comparisons between char*'s.  Unfortunately it doesn't seem to
// work reliably (compiler bugs?). The compiler seems to choose
// the default (less specialized) implementation in place of this
// one.
template<>
struct assertion_traits<char*> {
    static bool equal(const char* x, const char* y) {
        return strcmp(x, y) == 0;
    }

    static std::string toString(const char* x) {
        return std::string(x);
    }
};
#endif

// Allow CPPUNIT_ASSERT_EQUAL comparisons between char* strings.
// This functionality has to be implemented using the non-standard
// approach below because the assertion_traits approach doesn't
// seem to work (the compiler picks the wrong template).  Even the
// approach below occasionally fails, requiring arguments to
// be cast to (char*).
void
assertEquals(const char* expected, const char* actual,
        SourceLine sourceLine, const std::string &message) {
    if (actual == NULL || expected == NULL) {
        if (actual != expected) {
            Asserter::failNotEqual(std::string(expected ?: "(NULL)"),
                                   std::string(actual ?: "(NULL)"),
                                   sourceLine, message);
        }
    } else {
        if (strcmp(actual, expected) != 0) {
            Asserter::failNotEqual(std::string(expected),
                                   std::string(actual),
                                   sourceLine, message);
        }
    }
}

// Allow CPPUNIT_ASSERT_EQUAL comparisons between char* and std::string.
// This functionality has to be implemented using the non-standard
// approach below because the types of the arguments are different.
void
assertEquals(const char* expected, const std::string& actual,
        SourceLine sourceLine, const std::string &message) {
    if (actual != expected) {
        Asserter::failNotEqual(std::string(expected),
            assertion_traits<std::string>::toString(actual),
                sourceLine, message);
    }
}

// Allow CPPUNIT_ASSERT_EQUAL comparisons between uint64_t's;
// This also works for smaller integers such as int32_t and it
// works for both signed and unsigned values.  This functionality
// has to be implemented using the non-standard approach below
// because we sometimes supply an enum value for the first argument;
// the approach below will automatically convert it to integer, but
// the assertion_traits approach will not, so the types won't match.
void
assertEquals(uint64_t expected, const uint64_t actual,
        SourceLine sourceLine, const std::string &message) {
    if (expected != actual) {
        string s1(format("%ld (0x%lx)", expected, expected));
        string s2(format("%ld (0x%lx)", actual, actual));
        Asserter::failNotEqual(s1, s2, sourceLine, message);
    }
}

// Allow CPPUNIT_ASSERT_EQUAL comparisons between void*'s.
// This functionality has to be implemented using the non-standard
// approach below because we sometimes supply a char* value for
// the first argument; the approach below will automatically
// convert it to void*, but the assertion_traits approach
// will not, so the types won't match.
void
assertEquals(void* expected, const void* actual,
        SourceLine sourceLine, const std::string &message) {
    if (expected != actual) {
        string s1(format("%p", expected));
        string s2(format("%p", actual));
        Asserter::failNotEqual(s1, s2, sourceLine, message);
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
 * Convert a character to a printable form (if it isn't already) and append
 * to a string. This method is used by other methods such as
 * bufferToDebugString and toString.
 *
 * \param c
 *      Character to convert.
 * \param[out] out
 *      Append the converted result here. Non-printing characters get
 *      converted to a form using "/" (not "\"!).  This produces a result
 *      that can be cut and pasted from test output into test code: the
 *      result will never contain any characters that require quoting
 *      if used in a C string, such as backslashes or quotes.
 */
void
convertChar(char c, string *out) {
    if ((c >= 0x20) && (c < 0x7f) && (c != '"') && (c != '\\'))
        out->append(&c, 1);
    else if (c == '\0')
        out->append("/0");
    else if (c == '\n')
        out->append("/n");
    else
        out->append(format("/x%02x", c & 0xff));
}

/**
 * Create a printable representation of the contents of the memory
 * to a string.
 *
 * \param buf
 *      Convert the contents of this to ASCII.
 * \param length
 *      The length of the data in buf.
 */
string
toString(const char *buf, uint32_t length)
{
    string s;
    uint32_t i = 0;
    const char* separator = "";

    // Each iteration through the following loop processes a piece
    // of the buffer consisting of either:
    // * 4 bytes output as a decimal integer
    // * or, a string output as a string
    while (i < length) {
        s.append(separator);
        separator = " ";
        if ((i+4) <= length) {
            const char *p = &buf[i];
            if ((p[0] < ' ') || (p[1] < ' ')) {
                int value = *reinterpret_cast<const int*>(p);
                s.append(format(
                    ((value > 10000) || (value < -1000)) ? "0x%x" : "%d",
                    value));
                i += 4;
                continue;
            }
        }

        // This chunk of data looks like a string, so output it out as one.
        while (i < length) {
            char c = buf[i];
            i++;
            convertChar(c, &s);
            if (c == '\0') {
                break;
            }
        }
    }

    return s;
}

/**
 * Create a printable representation of the contents of the buffer.
 * The string representation was designed primarily for printing
 * network packets during testing.
 *
 * \param buffer
 *      The Buffer to create a string representation of.
 * \return
 *      A string describing the contents of
 *      buffer. The string consists of one or more items separated
 *      by white space, with each item representing a range of bytes
 *      in the buffer (these ranges do not necessarily correspond to
 *      the buffer's internal chunks).  A chunk can be either an integer
 *      representing 4 contiguous bytes of the buffer or a null-terminated
 *      string representing any number of bytes.  String format is preferred,
 *      but is only used for things that look like strings.  Integers
 *      are printed in decimal if they are small, otherwise hexadecimal.
 */
string
toString(Buffer* buffer)
{
    uint32_t length = buffer->getTotalLength();
    const char* buf = static_cast<const char*>(buffer->getRange(0, length));
    return toString(buf, length);
}

/**
 * Generate a string describing the contents of the buffer in a way
 * that displays its internal chunk structure.
 *
 * \return A string that describes the contents of the buffer. It
 *         consists of the contents of the various chunks separated
 *         by " | ", with long chunks abbreviated and non-printing
 *         characters converted to something printable.
 */
string
bufferToDebugString(Buffer* buffer)
{
    // The following declaration defines the maximum number of characters
    // to display from each chunk.
    static const uint32_t CHUNK_LIMIT = 20;
    const char *separator = "";
    uint32_t chunkLength;
    string s;

    for (uint32_t offset = 0; ; offset += chunkLength) {
        const char *chunk;
        chunkLength = buffer->peek(offset,
                                   reinterpret_cast<const void **>(&chunk));
        if (chunkLength == 0)
            break;
        s.append(separator);
        separator = " | ";
        for (uint32_t i = 0; i < chunkLength; i++) {
            if (i >= CHUNK_LIMIT) {
                // This chunk is too big to print in its entirety;
                // just print a count of the remaining characters.
                s.append(format("(+%d chars)", chunkLength-i));
                break;
            }
            convertChar(chunk[i], &s);
        }
    }
    return s;
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
assertMatchesPosixRegex(const string& pattern, const string& subject)
{
    regex_t pregStorage;
    int r;

    r = regcomp(&pregStorage, pattern.c_str(), 0);
    if (r != 0) {
        string errorMsg = "Pattern '";
        errorMsg += pattern;
        errorMsg += "' failed to compile: ";
        errorMsg += friendlyRegerror(r, &pregStorage);
        CPPUNIT_FAIL(errorMsg);
    }

    r = regexec(&pregStorage, subject.c_str(), 0, NULL, 0);
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

/**
 * Fail the CPPUNIT test case if the given string does match the given POSIX
 * regular expression.
 * \param pattern
 *      A POSIX regular expression.
 * \param subject
 *      The string that should not match \a pattern.
 */
void
assertNotMatchesPosixRegex(const string& pattern, const string& subject)
{
    regex_t pregStorage;
    int r;
    bool fail = true;
    string errorMsg;

    r = regcomp(&pregStorage, pattern.c_str(), 0);
    if (r != 0) {
        errorMsg = "Pattern '";
        errorMsg += pattern;
        errorMsg += "' failed to compile: ";
        errorMsg += friendlyRegerror(r, &pregStorage);
    }

    r = regexec(&pregStorage, subject.c_str(), 0, NULL, 0);
    if (r != 0) {
        errorMsg = "Pattern '";
        errorMsg += pattern;
        errorMsg += "' did not match subject '";
        errorMsg += subject;
        errorMsg += "'";
        regfree(&pregStorage);
        fail = false;
    }

    regfree(&pregStorage);

    if (fail)
        CPPUNIT_FAIL(errorMsg);
}

} // namespace RAMCloud
