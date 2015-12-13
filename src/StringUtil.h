/* Copyright (c) 2011 Facebook
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

#ifndef RAMCLOUD_STRINGUTIL_H
#define RAMCLOUD_STRINGUTIL_H

#include "Common.h"

namespace RAMCloud {

/**
 * Utilities for working with strings.
 */
namespace StringUtil {

bool startsWith(const string& haystack, const string& needle);
bool endsWith(const string& haystack, const string& needle);
bool contains(const string& haystack, const string& needle);
string regsub(const string& subject, const string& pattern,
        const string& replacement);
string binaryToString(const void* input, uint32_t length);
std::vector<std::string> split(const std::string &s, char delim);
int64_t stringToInt(const char* s, bool* error);

} // end StringUtil

} // end RAMCloud

#endif  // RAMCLOUD_STRINGUTIL_H
