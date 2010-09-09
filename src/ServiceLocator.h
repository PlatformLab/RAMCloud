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
 * Header file for #RAMCloud::ServiceLocator.
 */

#ifndef RAMCLOUD_SERVICELOCATOR_H
#define RAMCLOUD_SERVICELOCATOR_H

#include <errno.h>
#include <pcrecpp.h>

#include <map>
#include <stdexcept>
#include <vector>

#include "Common.h"

namespace RAMCloud {

/**
 * A ServiceLocator describes one way to access a particular service.
 * It specifies the protocol to be used (which any number of concrete
 * #RAMCloud::Transport classes may implement) and a set of protocol-specific
 * arguments for how to communicate with the service, including addressing
 * information and protocol options. For an example of how to use this class,
 * see #RAMCloud::ServiceLocatorTest::test_usageExample().
 *
 * For example, in the service locator string
 *  "tcp+ip: host=example.org, port=8081"
 * the protocol is tcp+ip and the options are a host of example.org and a port
 * of 8081. The protocol stack has tcp at the top, followed by ip.
 */
class ServiceLocator {
  friend class ServiceLocatorTest;
  public:

    /**
     * An exception thrown when the string passed to the constructor could not
     * be parsed.
     */
    struct BadServiceLocatorException : public Exception {
        BadServiceLocatorException(const string& original,
                                   const string& remaining)
            : original(original), remaining(remaining) {
            message = "The ServiceLocator string '" + original +
                "' could not be parsed, starting at '" + remaining + "'";
        }
        /**
         * The string that was given to the constructor.
         */
        string original;

        /**
         * The remaining part of the original string that could not be
         * understood. Everything in \a original before \a remaining was
         * successfully parsed, but something at the start of \a remaining is
         * causing trouble.
         */
        string remaining;
    };

    /**
     * An exception thrown when peeking or popping past the end of the protocol
     * stack.
     */
    struct NoMoreProtocolsException : public Exception {
        explicit NoMoreProtocolsException(const string& protocol)
            : protocol(protocol) {
            message = "Peek or pop past the end of a protocol stack: '" +
                protocol + "'";
        }
        string protocol;
    };

    /**
     * An exception thrown when no option with the requested key was found.
     */
    struct NoSuchKeyException : public Exception {
        explicit NoSuchKeyException(const string& key)
            : key(key) {
            message = "The option with key '" + key +
                "' was not found in the ServiceLocator.";
        }
        string key;
    };


    /**
     * An abstract exception type that is a superclass of
     * #RAMCloud::ServiceLocator::BadFormatException, and
     * #RAMCloud::ServiceLocator::OutOfRangeException.
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

    explicit ServiceLocator(const string& serviceLocator);

    template<typename T> T getOption(const string& key) const;

    const string& getOption(const string& key) const;

    template<typename T> T getOption(const string& key, T defaultValue) const;

    const string& getOption(const string& key,
                            const string& defaultValue) const;

    /**
     * Return the original service locator string passed to the constructor.
     * \return
     *      See above.
     */
    const string& getOriginalString() const {
        return originalString;
    }

    /**
     * Return the part of the ServiceLocator string that generally specifies
     * the transport protocol.
     * For example, in "tcp: host=example.org, port=8081", this will return
     * "tcp".
     * \return
     *      See above.
     */
    const string& getOriginalProtocol() const {
        return originalProtocol;
    }

    /**
     * Return the individual protocols specified in the original protocol
     * string, indexed from left to right.
     * \return
     *      See above.
     */
    const std::vector<string>& getProtocolStack() const {
        return protocolStack;
    }

    /**
     * Return the number of times pop can be called before it throws
     * a NoMoreProtocolsException.
     * \return
     *      See above.
     */
    bool getNumProtocolsRemaining() const {
        return protocolStack.size() - protocolStackIndex;
    }

    /**
     * Return the next protocol off the protocol stack.
     * \return
     *      See above.
     * \throw NoMoreProtocolsException
     *      Attempting to peek past the end of the protocol stack.
     *      See #getNumProtocolsRemaining().
     */
    const string& peekProtocol() const {
        try {
            return protocolStack.at(protocolStackIndex);
        } catch (std::out_of_range e) {
            throw NoMoreProtocolsException(originalProtocol);
        }
    }

    /**
     * Pop the next protocol off the protocol stack.
     * \return
     *      See above.
     * \throw NoMoreProtocolsException
     *      Attempting to pop past the end of the protocol stack.
     *      See #getNumProtocolsRemaining().
     */
    const string& popProtocol() {
        try {
            return protocolStack.at(protocolStackIndex++);
        } catch (std::out_of_range e) {
            throw NoMoreProtocolsException(originalProtocol);
        }
    }

    /**
     * Restore all the protocols that have been popped off the protocol stack.
     * The next call to #peekProtocol or #popProtocol will return the very
     * first protocol from #originalProtocol.
     */
    void resetProtocolStack() {
        protocolStackIndex = 0;
    }

  private:

    /**
     * Coerce a value from a string to the given type.
     * \param value
     *      The string to be coerced.
     * \tparam T
     *      The type to which to coerce the value.
     *      There is a limited list of valid types allowed here, and any others
     *      will result in a linker error. See #getOption for this list.
     * \throw BadValueException
     *      The value could not be coerced to the given type.
     */
    template<typename T> static T convertValue(const string& value);

    /**
     * See #getOriginalString().
     */
    const string originalString;

    /**
     * See #getOriginalProtocol().
     * This is const after construction.
     */
    string originalProtocol;

    /**
     * See #getProtocolStack().
     * This is const after construction.
     */
    std::vector<string> protocolStack;

    /**
     * The index into #protocolStack of the string that the next call to peek
     * or pop should return.
     */
    uint32_t protocolStackIndex;

    /**
     * A map from key to value of parsed options from #originalString.
     * This is const after construction.
     */
    std::map<string, string> options;

    DISALLOW_COPY_AND_ASSIGN(ServiceLocator);
};

// Unfortunately, these need to be defined in the header file because they're
// templates that are instantiated in multiple files:

/**
 * Return the value for the option with the given key, coerced to the given
 * type.
 * \param key
 *      The key for the desired option.
 * \tparam T
 *      The type to which to coerce the option's value.
 *      The only valid types are as follows, and any others will result in a
 *      linker error:
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
 * \return
 *      See above.
 * \throw NoSuchKeyException
 *      The key does not name an option that was specified.
 * \throw BadValueException
 *      The value could not be coerced to the given type.
 */
template<typename T> T
ServiceLocator::getOption(const string& key) const {
    std::map<string, string>::const_iterator i = options.find(key);
    if (i == options.end())
        throw NoSuchKeyException(key);
    try {
        return convertValue<T>(i->second);
    } catch (BadFormatException e) {
        e.key = key;
        throw;
    } catch (OutOfRangeException e) {
        e.key = key;
        throw;
    }
}

/**
 * Return the value for the option with the given key, coerced to the given
 * type, or a given value if no such option is available.
 * \param key
 *      The key for the desired option.
 * \param defaultValue
 *      The value to return if the key does not name an available option.
 * \tparam T
 *      See #getOption<>().
 * \return
 *      The value for the option with the given key, coerced to the given type.
 *      If the key does not name an available option, \a defaultValue will be
 *      returned.
 * \throw BadValueException
 *      The value could not be coerced to the given type.
 */
template<typename T> T
ServiceLocator::getOption(const string& key, T defaultValue) const {
    std::map<string, string>::const_iterator i = options.find(key);
    if (i == options.end())
        return defaultValue;
    try {
        return convertValue<T>(i->second);
    } catch (BadFormatException e) {
        e.key = key;
        throw;
    } catch (OutOfRangeException e) {
        e.key = key;
        throw;
    }
}

} // end RAMCloud

#endif  // RAMCLOUD_SERVICELOCATOR_H
