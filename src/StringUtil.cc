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

#include <regex.h>
#include "StringUtil.h"

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

} // namespace StringUtil
} // namespace RAMCloud
