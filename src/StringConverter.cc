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

template<> float
StringConverter::convert<float>(const string& value) const
{
    return convertFloating<float, strtof>(value);
}

template<> double
StringConverter::convert<double>(const string& value) const
{
    return convertFloating<double, strtod>(value);
}

template<> int8_t
StringConverter::convert<int8_t>(const string& value) const
{
    return convertUsingLossOfPrecision<int8_t, int64_t>(value);
}

template<> uint8_t
StringConverter::convert<uint8_t>(const string& value) const
{
    return convertUsingLossOfPrecision<uint8_t, uint64_t>(value);
}

template<> int16_t
StringConverter::convert<int16_t>(const string& value) const
{
    return convertUsingLossOfPrecision<int16_t, int64_t>(value);
}

template<> uint16_t
StringConverter::convert<uint16_t>(const string& value) const
{
    return convertUsingLossOfPrecision<uint16_t, uint64_t>(value);
}

template<> int32_t
StringConverter::convert<int32_t>(const string& value) const
{
    return convertUsingLossOfPrecision<int32_t, int64_t>(value);
}

template<> uint32_t
StringConverter::convert<uint32_t>(const string& value) const
{
    return convertUsingLossOfPrecision<uint32_t, uint64_t>(value);
}

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

// Helpers:

/**
 * A helper for coercing floating point types. 
 * \param value
 *      See #convert().
 * \tparam T
 *      See #convert().
 * \tparam strtox
 *      A function that behaves like strtod (3).
 * return
 *      See #convert().
 */
template<typename T, T (*strtox)(const char*, char**)> T
StringConverter::convertFloating(const string& value) const
{
    const char* valueCStr = value.c_str();
    char* end;
    errno = 0;
    T v = strtox(valueCStr, &end);
    if (errno != 0)
        throw OutOfRangeException(value);
    if (*end != '\0' || *valueCStr == '\0')
        throw BadFormatException(value);
    return v;
}

/**
 * A helper for coercing smaller integer types using the convert
 * implementation of a larger integer type. 
 * \param value
 *      See #convert().
 * \tparam T
 *      See #convert().
 * \tparam P
 *      A type similar to T but with more precision.
 * return
 *      See #convert().
 */
template<typename T, typename P> T
StringConverter::convertUsingLossOfPrecision(const string& value) const
{
    P v = convert<P>(value);
    if (static_cast<P>(static_cast<T>(v)) != v)
        throw OutOfRangeException(value);
    return static_cast<T>(v);
}

} // namespace RAMCloud
