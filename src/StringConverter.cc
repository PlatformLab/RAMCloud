/* Copyright (c) 2010 Stanford University
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

/**
 * \file
 * Implementation for #RAMCloud::StringConverter.
 */

#include <errno.h>

#include "StringConverter.h"

namespace RAMCloud {

template<> const string&
StringConverter::convert<const string&>(const string& value) const
{
    return value;
}

template<> const char*
StringConverter::convert<const char*>(const string& value) const
{
    return value.c_str();
}

template<> bool
StringConverter::convert<bool>(const string& value) const
{
    const char* valueCStr = value.c_str();
    if (value.length() == 1) {
        switch (*valueCStr) {
            case '0':
            case 'n':
            case 'N':
            case 'f':
            case 'F':
                return false;
            case '1':
            case 'y':
            case 'Y':
            case 't':
            case 'T':
                return true;
            default:
                break;
        }
    } else if (strcasecmp(valueCStr, "false") == 0) // NOLINT
        return false;
    else if (strcasecmp(valueCStr, "no") == 0)
        return false;
    else if (strcasecmp(valueCStr, "true") == 0)
        return true;
    else if (strcasecmp(valueCStr, "yes") == 0)
        return true;
    throw BadFormatException(value);
}

#define CONVERT_VALUE_FLOATING(type, strtoxfn) \
template<> type \
StringConverter::convert<type>(const string& value) const \
{ \
    const char* valueCStr = value.c_str(); \
    char* end; \
    errno = 0; \
    type v = strtoxfn(valueCStr, &end); \
    if (errno != 0) \
        throw OutOfRangeException(value); \
    if (*end != '\0' || *valueCStr == '\0') \
        throw BadFormatException(value); \
    return v; \
}

CONVERT_VALUE_FLOATING(float, strtof);
CONVERT_VALUE_FLOATING(double, strtod);

template<> int64_t
StringConverter::convert<int64_t>(const string& value) const
{
    const char* valueCStr = value.c_str();
    char* end;
    errno = 0;
    int64_t v = strtol(valueCStr, &end, 0);
    if (errno != 0)
        throw OutOfRangeException(value);
    if (*end != '\0' || *valueCStr == '\0')
        throw BadFormatException(value);
    return v;
}

template<> uint64_t
StringConverter::convert<uint64_t>(const string& value) const
{
    const char* valueCStr = value.c_str();
    char* end;
    if (*valueCStr == '-')
        throw OutOfRangeException(value);
    errno = 0;
    uint64_t v = strtoul(valueCStr, &end, 0);
    if (errno != 0)
        throw OutOfRangeException(value);
    if (*end != '\0' || *valueCStr == '\0')
        throw BadFormatException(value);
    return v;
}

#define CONVERT_VALUE_SIGNED(type) \
template<> type \
StringConverter::convert<type>(const string& value) const \
{ \
    int64_t v = convert<int64_t>(value); \
    if (static_cast<int64_t>(static_cast<type>(v)) != v) \
        throw OutOfRangeException(value); \
    return static_cast<type>(v); \
}

CONVERT_VALUE_SIGNED(int8_t);
CONVERT_VALUE_SIGNED(int16_t);
CONVERT_VALUE_SIGNED(int32_t);

#define CONVERT_VALUE_UNSIGNED(type) \
template<> type \
StringConverter::convert<type>(const string& value) const \
{ \
    uint64_t v = convert<uint64_t>(value); \
    if (static_cast<uint64_t>(static_cast<type>(v)) != v) \
        throw OutOfRangeException(value); \
    return static_cast<type>(v); \
}

CONVERT_VALUE_UNSIGNED(uint8_t);
CONVERT_VALUE_UNSIGNED(uint16_t);
CONVERT_VALUE_UNSIGNED(uint32_t);

} // namespace RAMCloud
