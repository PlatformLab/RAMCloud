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
        BadValueException(const string& key, const string& value)
            : key(), value(value) {
            setKey(key);
        }
        explicit BadValueException(const string& value)
            : key(), value(value) {
            message = "The value '" + value + "' was invalid.";
        }
      public:
        /**
         * Update #key. This is useful if the key was not available at the time
         * the exception was thrown but only higher up the stack.
         */
        void setKey(const string& key) {
            this->key = key;
            message = "The value '" + value + "' for option '" + key +
                "' was invalid.";
        }
        string key;
        string value;
    };

    /**
     * An exception thrown when the option with the given key did not have the
     * right format to be coerced to the requested type.
     */
    struct BadFormatException : public BadValueException {
        BadFormatException(const string& key, const string& value)
            : BadValueException(key, value) {}
        explicit BadFormatException(const string& value)
            : BadValueException(value) {}
    };

    /**
     * An exception thrown when the option with the given key was too big or
     * too small to be coerced to the requested type.
     */
    struct OutOfRangeException : public BadValueException {
        OutOfRangeException(const string& key, const string& value)
            : BadValueException(key, value) {}
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
     *       - bool
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
     * \throw BadValueException
     *      The value could not be coerced to the given type.
     */
    template<typename T> T convert(const string& value) const;
};

} // end RAMCloud

#endif  // RAMCLOUD_STRINGCONVERTER_H
