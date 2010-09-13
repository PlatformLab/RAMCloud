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
 * Header file for #RAMCloud::StringConverter.
 */

#ifndef RAMCLOUD_STRINGCONVERTER_H
#define RAMCLOUD_STRINGCONVERTER_H

#include "Common.h"

namespace RAMCloud {

/**
 * Converts values from strings to different types.
 */
class StringConverter {
  public:
    /**
     * An abstract exception type that is a superclass of
     * #RAMCloud::StringConverter::BadFormatException, and
     * #RAMCloud::StringConverter::OutOfRangeException.
     * This is useful for catching malformed values in general.
     */
    struct BadValueException : public Exception {
      protected:
        explicit BadValueException(const string& value) : value(value) {
            message = "The value '" + value + "' was invalid.";
        }
      public:
        string value;
    };

    /**
     * An exception thrown when the option with the given key did not have the
     * right format to be coerced to the requested type.
     */
    struct BadFormatException : public BadValueException {
        explicit BadFormatException(const string& value)
            : BadValueException(value) {}
    };

    /**
     * An exception thrown when the option with the given key was too big or
     * too small to be coerced to the requested type.
     */
    struct OutOfRangeException : public BadValueException {
        explicit OutOfRangeException(const string& value)
            : BadValueException(value) {}
    };

    /**
     * Coerce a value from a string to the given type.
     * \param value
     *      The string to be coerced.
     * \tparam T
     *      The type to which to coerce the value. The only valid types are as
     *      follows, and any others will result in a linker error:
     *       - const string&
     *       - const char*
     *       - bool \n
     *       - float
     *       - double
     *       - int8_t
     *       - uint8_t
     *       - int16_t
     *       - uint16_t
     *       - int32_t
     *       - uint32_t
     *       - int64_t
     *       - uint64_t
     *
     * For bool, valid values are 0/f/false/n/no for false and 1/t/true/y/yes
     * for true (case insensitive).
     *
     * For integers, valid values can be expressed in decimal or hex. A
     * preceding '+' is optional for positive values.
     *
     * For floating point numbers, valid values include 'nan', 'inf', and
     * 'infinity'.
     *
     * \throw BadValueException
     *      The value could not be coerced to the given type.
     */
    template<typename T> T convert(const string& value) const;

  private:

    template<typename T, T (*strtox)(const char*, char**)> T
    convertFloating(const string& value) const;

    template<typename T, typename P> T
    convertUsingLossOfPrecision(const string& value) const;
};

} // end RAMCloud

#endif  // RAMCLOUD_STRINGCONVERTER_H
