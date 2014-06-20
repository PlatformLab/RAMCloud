/* Copyright (c) 2010-2014 Stanford University
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

#include <string.h>
#include "TestUtil.h"
#include "Dispatch.h"
#include "WireFormat.h"

using namespace RAMCloud;

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
TestUtil::convertChar(char c, string *out)
{
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
 * Fill a region of memory with random alphanumeric characters.
 *
 * \param buf
 *      Address of the first byte to fill.
 * \param size
 *      Total number of bytes to fill.
 */
void
TestUtil::fillPrintableRandom(void* buf, uint32_t size)
{
    static const uint8_t tab[256] = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
        'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
        'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd',
        'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
        'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
        'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
        'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
        'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b',
        'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
        'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
        'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5',
        '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F',
        'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
        'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
        'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
        'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3',
        '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D',
        'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
        'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
        'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
        'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
        's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1',
        '2', '3', '4', '5', '6', '7'
    };
    fillRandom(buf, size);
    uint8_t* b = static_cast<uint8_t*>(buf);
    for (uint32_t i = 0; i < size; i++)
        b[i] = tab[b[i]];
}

/**
 * Fill a region of memory with random data.
 *
 * \param buf
 *      Address of the first byte to fill.
 * \param size
 *      Total number of bytes to fill.
 */
void
TestUtil::fillRandom(void* buf, uint32_t size)
{
    static int fd = open("/dev/urandom", O_RDONLY);
    assert(fd >= 0);
    ssize_t bytesRead = read(fd, buf, size);
    assert(bytesRead == size);
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
TestUtil::toString(const void* buf, uint32_t length)
{
    string s;
    uint32_t i = 0;
    const char* separator = "";
    const char* charBuf = reinterpret_cast<const char*>(buf);

    // Each iteration through the following loop processes a piece
    // of the buffer consisting of either:
    // * 4 bytes output as a decimal integer
    // * or, a string output as a string
    while (i < length) {
        s.append(separator);
        separator = " ";
        if ((i+4) <= length) {
            const char *p = &charBuf[i];
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
            char c = charBuf[i];
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
 * Create a printable representation of the contents of the buffer,
 * starting at a given offset and spanning a given length of bytes.
 * The string representation was designed primarily for printing
 * network packets during testing.
 *
 * \param buffer
 *      The buffer to create a string representation of.
 * \param offset
 *      Offset of the first byte in the buffer to be stringified.
 * \param length
 *      Total number of bytes in the buffer to be stringified, starting
 *      at the given offset.
 * \return
 *      See the toString(Buffer*) method's return documentation.
 */
string
TestUtil::toString(Buffer* buffer, uint32_t offset, uint32_t length)
{
    return toString(buffer->getRange(offset, length), length);
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
TestUtil::toString(Buffer* buffer)
{
    return toString(buffer, 0, buffer->getTotalLength());
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
TestUtil::bufferToDebugString(Buffer* buffer)
{
    // The following declaration defines the maximum number of characters
    // to display from each chunk.
    static const uint32_t CHUNK_LIMIT = 20;
    const char *separator = "";
    uint32_t chunkLength;
    string s;

    for (uint32_t offset = 0; ; offset += chunkLength) {
        char *chunk;
        chunkLength = buffer->peek(offset,
                                   reinterpret_cast<void **>(&chunk));
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
 * Fail a gtest test case if the given string doesn't match the given POSIX
 * regular expression.
 * \param pattern
 *      A POSIX regular expression.
 * \param subject
 *      The string that should match \a pattern.
 * \return
 *      A value that can be tested with EXPECT_TRUE.
 */
::testing::AssertionResult
TestUtil::matchesPosixRegex(const string& pattern, const string& subject)
{
    regex_t pregStorage;
    int r;

    r = regcomp(&pregStorage, pattern.c_str(), 0);
    if (r != 0) {
        string message(format("Pattern '%s' failed to compile: %s",
                pattern.c_str(),
                friendlyRegerror(r, &pregStorage).c_str()));
        regfree(&pregStorage);
        return ::testing::AssertionFailure() << message;
    }

    r = regexec(&pregStorage, subject.c_str(), 0, NULL, 0);
    if (r != 0) {
        string message(format("Pattern '%s' did not match subject '%s'",
                pattern.c_str(), subject.c_str()));
        regfree(&pregStorage);
        return ::testing::AssertionFailure() << message;
    }
    regfree(&pregStorage);
    return ::testing::AssertionSuccess();
}

/**
 * Fail a gtest test case if the given string matches the given POSIX
 * regular expression.
 * \param pattern
 *      A POSIX regular expression.
 * \param subject
 *      The string that should *not* match \a pattern.
 * \return
 *      A value that can be tested with EXPECT_TRUE.
 */
::testing::AssertionResult
TestUtil::doesNotMatchPosixRegex(const string& pattern, const string& subject)
{
    regex_t pregStorage;
    int r;

    r = regcomp(&pregStorage, pattern.c_str(), 0);
    if (r != 0) {
        string message(format("Pattern '%s' failed to compile: %s",
                pattern.c_str(),
                friendlyRegerror(r, &pregStorage).c_str()));
        regfree(&pregStorage);
        return ::testing::AssertionFailure() << message;
    }

    r = regexec(&pregStorage, subject.c_str(), 0, NULL, 0);
    if (r == 0) {
        string message(format("Pattern '%s' matched subject '%s'",
                pattern.c_str(), subject.c_str()));
        regfree(&pregStorage);
        return ::testing::AssertionFailure() << message;
    }
    regfree(&pregStorage);
    return ::testing::AssertionSuccess();
}

/**
 * Fail a gtest test case if the given string doesn't contain a given
 * substring.
 * \param s
 *      Test output.
 * \param substring
 *      Substring expected to appear somewhere in s.
 * \return
 *      A value that can be tested with EXPECT_TRUE.
 */
::testing::AssertionResult
TestUtil::contains(const string& s, const string& substring)
{
    if (s.find(substring) == string::npos) {
        string message(format("Substring '%s' not present in '%s'",
                substring.c_str(), s.c_str()));
        return ::testing::AssertionFailure() << message;
    }
    return ::testing::AssertionSuccess();
}

/**
 * This method fills a buffer with a given amount of data, in a form that
 * can be checked easily by #checkLargeBuffer. It's intended for testing
 * proper handling of large amounts of data.
 *
 * \param buffer
 *      Buffer to fill with data; any pre-existing contents are deleted.
 * \param size
 *      Number of bytes of data to add to the buffer.
 */
void
TestUtil::fillLargeBuffer(Buffer* buffer, int size)
{
    char chunk[200];
    buffer->reset();
    int i = 1;
    uint32_t bytesLeft = size;
    while (bytesLeft > 0) {
        snprintf(chunk, sizeof(chunk),
                "word %d, word %d, word %d, word %d, word %d; ",
                i, i+1, i+2, i+3, i+4);
        uint32_t chunkLength = downCast<uint32_t>(strlen(chunk));
        if (chunkLength > bytesLeft) {
            chunkLength = bytesLeft;
        }
        buffer->appendCopy(chunk, chunkLength);
        bytesLeft -= chunkLength;
        i += 5;
    }
}

/**
 * This method checks that a buffer contains the expected data, assuming
 * it was filled by calling #fillLargeBuffer.
 *
 * \param buffer
 *      Buffer whose contents are to be checked.
 * \param expectedLength
 *      The buffer should contain this many bytes.
 *
 * \return
 *      If the buffer has the "expected" contents then "ok" is returned;
 *      otherwise the returned string contains error information about
 *      what was wrong with the buffer.
 */
string
TestUtil::checkLargeBuffer(Buffer* buffer, int expectedLength)
{
    int length = buffer->getTotalLength();
    if (length != expectedLength) {
        return format("expected %d bytes, found %d bytes",
                expectedLength, length);
    }
    Buffer comparison;
    fillLargeBuffer(&comparison, expectedLength);
    for (int i = 0; i < expectedLength; i++) {
        char c1 = *buffer->getOffset<char>(i);
        char c2 = *comparison.getOffset<char>(i);
        if (c1 != c2) {
            int start = i - 10;
            const char* prefix = "...";
            const char* suffix = "...";
            if (start <= 0) {
                start = 0;
                prefix = "";
            }
            int length = 20;
            if (start+length >= expectedLength) {
                length = expectedLength - start;
                suffix = "";
            }
            return format("expected '%c', got '%c' (\"%s%.*s%s\" "
                    "vs \"%s%.*s%s\")", c2, c1, prefix, length,
                    static_cast<const char*>(comparison.getRange(start,
                    length)), suffix, prefix, length,
                    static_cast<const char*>(buffer->getRange(start,
                    length)), suffix);
        }
    }
    return string("ok");
}

/**
 * Given a buffer containing the response from an RPC, extract the error status
 * from the buffer and return the symbolic name for the status code.
 *
 * \param buffer
 *      Contains the response from an RPC.
 *
 * \result
 *      A symbolic status value, such as "STATUS_OK", or "empty reply message"
 *      if the buffer didn't contain a valid RPC response.
 */
const char *TestUtil::getStatus(Buffer* buffer)
{
    const WireFormat::ResponseCommon* responseCommon =
            buffer->getStart<WireFormat::ResponseCommon>();
    if (responseCommon == NULL) {
        return "empty reply message";
    }
    return statusToSymbol(responseCommon->status);
}

/**
 * Wait for an RPC request to complete (but give up if it takes too long).
 *
 * \param context
 *      RAMCloud Context whose dispatcher should be used for polling.
 * \param wrapper
 *      RPC request that is expected to finish very soon.
 * \param ms
 *      How long to wait (in milliseconds) before giving up.
 *
 * \result
 *      True if the request finishes within a reasonable time period,
 *      false if it doesn't.
 */
bool
TestUtil::waitForRpc(Context* context, MockWrapper& wrapper, int ms)
{
    for (int i = 0; i < ms; i++) {
        context->dispatch->poll();
        if (wrapper.completedCount || wrapper.failedCount)
            return true;
        usleep(1000);
    }
    return false;
}

/**
 * Read a file and return its contents.
 *
 * \param path
 *      Name of the file to read.
 *
 * \result
 *      The contents of the file, or an empty string if the file couldn't
 *      be opened.
 */
string
TestUtil::readFile(const char* path)
{
    char buffer[5000];
    string result;
    FILE* f = fopen(path, "r");
    if (f != NULL) {
        while (1) {
            size_t count = fread(buffer, 1, sizeof(buffer), f);
            if (count <= 0)
                break;
            result.append(buffer, count);
        }
    }
    return result;
}


} // namespace RAMCloud
