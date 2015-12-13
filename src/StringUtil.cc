/* Copyright (c) 2011 Facebook
 * Copyright (c) 2012 Stanford University
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

#include "StringUtil.h"

#include <regex.h>
#include <climits>
#include <sstream>

namespace RAMCloud {
namespace StringUtil {

/// Return true if haystack begins with needle.
bool
startsWith(const string& haystack, const string& needle)
{
    return (haystack.compare(0, needle.length(), needle) == 0);
}

/// Return true if haystack ends with needle.
bool
endsWith(const string& haystack, const string& needle)
{
    if (haystack.length() < needle.length())
        return false;
    return (haystack.compare(haystack.length() - needle.length(),
                             needle.length(), needle) == 0);
}

/// Return true if needle exists somewhere in haystack.
bool
contains(const string& haystack, const string& needle)
{
    if (haystack.length() < needle.length())
        return false;
    return haystack.find(needle) != string::npos;
}

/**
 * Basic regular expression substitution.
 * \param subject
 *      String on which to perform substitution.
 * \param pattern
 *      A POSIX regular expression (supports extended syntax, including
 *      "+" etc.).
 * \param replacement
 *      Literal string to replace each instance of \a pattern
 *      in \a subject.
 *
 * \return
 *      The return value consists of \a subject with each instance of
 *      \a pattern replaced with \a replacement. If there are no
 *      instances of \a pattern in \a subject, then \a subject is returned.
 *      If there is an error in \a pattern, then the returned value is the
 *      regex error message.
 */
string
regsub(const string& subject, const string& pattern, const string& replacement)
{
    regex_t pregStorage;
    int r;

    r = regcomp(&pregStorage, pattern.c_str(), REG_EXTENDED);
    if (r != 0) {
        char message[1000];
        regerror(r, &pregStorage, message, sizeof(message));
        regfree(&pregStorage);
        return message;
    }

    string result;
    uint32_t cursor = 0;

    // Each iteration through the following loop finds the next match and
    // performs the corresponding substitution.
    while (cursor < subject.size()) {
        regmatch_t matches[1];
        r = regexec(&pregStorage, subject.c_str() + cursor, 1, matches, 0);
        if (r != 0) {
            break;
        }

        result.append(subject, cursor, matches[0].rm_so);
        result.append(replacement);
        cursor += matches[0].rm_eo;
    }

    // Copy to the result their unmatched text at the end of the subject.
    result.append(subject, cursor, subject.size() - cursor);
    regfree(&pregStorage);
    return result;
}

/**
 * Take the binary string given and convert it into a printable string.
 * Any printable ASCII characters (including space, but not other
 * whitespace) will be unchanged. Any non-printable characters will
 * be represented in escaped hexadecimal form, for example "\xf8\x07".
 *
 * \param input
 *      Pointer to some memory that may or may not contain ascii
 *      characters.
 * \param length
 *      Length of the input in bytes.
 */
string
binaryToString(const void* input, uint32_t length)
{
    string s = "";
    const unsigned char* c = reinterpret_cast<const unsigned char*>(input);

    for (uint16_t i = 0; i < length; i++) {
        if (isprint(c[i]))
            s += c[i];
        else
            s += format("\\x%02x", static_cast<uint32_t>(c[i]));
    }

    return s;
}

/**
 * Split a std::string based on a character delimiter.
 * \param s
 *      String to be split.
 * \param delim
 *      Delimiter to split string on.
 */
std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim))
        elems.push_back(item);
    return elems;
}

/**
 * Convenience method for converting strings to integers.
 * \param s
 *      String consisting of whitespace following by a positive or
 *      negative number.
 * \param error
 *      Set to true if s did not contain a syntactically valid number,
 *      the number was out of range for a long int, or there was extra
 *      information in s after the number.
 * \return
 *      The number corresponding to s, or 0 in the case of an error.
 */
int64_t
stringToInt(const char* s, bool* error)
{
    char* end;
    int64_t result = strtol(s, &end, 0);
    if ((end == s) || (*end != 0) || (result == LONG_MAX)
            || (result == LONG_MIN)) {
        *error = true;
        return 0;
    }
    *error = false;
    return result;
}

} // namespace StringUtil
} // namespace RAMCloud
