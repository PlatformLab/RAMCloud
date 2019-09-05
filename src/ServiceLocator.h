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

#ifndef RAMCLOUD_SERVICELOCATOR_H
#define RAMCLOUD_SERVICELOCATOR_H

#include <errno.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Weffc++"
#include <pcrecpp.h>
#include <boost/lexical_cast.hpp>
#pragma GCC diagnostic pop

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
 */
class ServiceLocator {
  public:

    /**
     * An exception thrown when the string passed to the constructor could not
     * be parsed.
     */
    struct BadServiceLocatorException : public Exception {
        BadServiceLocatorException(const CodeLocation& where,
                                   const string& original,
                                   const string& remaining)
            : Exception(where), original(original), remaining(remaining) {
            message = "The ServiceLocator string '" + original +
                "' could not be parsed, starting at '" + remaining + "'";
        }
        ~BadServiceLocatorException() throw() {}
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
     * An exception thrown when no option with the requested key was found.
     */
    struct NoSuchKeyException : public Exception {
        explicit NoSuchKeyException(const CodeLocation& where,
                                    const string& key)
            : Exception(where), key(key) {
            message = "The option with key '" + key +
                "' was not found in the ServiceLocator.";
        }
        ~NoSuchKeyException() throw() {}
        string key;
    };

    static vector<ServiceLocator>
    parseServiceLocators(const string& serviceLocator);

    explicit ServiceLocator(const string& serviceLocator);

    template<typename T> T getOption(const string& key) const;

    const string& getOption(const string& key) const;

    template<typename T> T getOption(const string& key, T defaultValue) const;

    const string& getOption(const string& key,
                            const string& defaultValue) const;

    /**
     * Return whether the given key names an option that was specified.
     * \param key
     *      See above.
     * \return
     *      See above.
     */
    bool hasOption(const string& key) const {
        return (options.find(key) != options.end());
    }

    /**
     * Return the original service locator string passed to the constructor.
     * \return
     *      See above.
     */
    const string& getOriginalString() const {
        return originalString;
    }

    /**
     * Return the service locator string for the driver by dropping the
     * transport information (i.e., dropping anything before "+" in the
     * original locator string).
     * \return
     *      See above.
     * \throw Exception
     *      The service locator doesn't contain driver information.
     */
    const string getDriverLocatorString() const {
        size_t pos = originalString.find_first_of('+');
        if (pos == string::npos) {
            return originalString;
        } else if (pos + 1 < originalString.size()) {
            return originalString.substr(pos + 1);
        } else {
            string errMsg =
                    "couldn't find driver information in service locator: ";
            errMsg += originalString;
            throw Exception(HERE, errMsg.data());
        }
    }

    /**
     * Return the part of the ServiceLocator string that generally specifies
     * the transport protocol.
     * For example, in "tcp: host=example.org, port=8081", this will return
     * "tcp".
     * \return
     *      See above.
     */
    const string& getProtocol() const {
        return protocol;
    }

    bool operator==(const ServiceLocator& other) const {
        return (this->originalString == other.originalString);
    }

  PRIVATE:
    ServiceLocator();

    void init(pcrecpp::StringPiece* serviceLocator);

    /**
     * See #getOriginalString().
     * This is const after construction.
     */
    string originalString;

    /**
     * See #getProtocol().
     * This is const after construction.
     */
    string protocol;

    /**
     * A map from key to value of parsed options from #originalString.
     * This is const after construction.
     */
    std::map<string, string> options;
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
 *      The only valid types are as follows those for which
 *      boost::lexical_cast<>() may be called.
 * \return
 *      See above.
 * \throw NoSuchKeyException
 *      The key does not name an option that was specified.
 * \throw boost::bad_lexical_cast
 *      The value could not be coerced to the given type.
 */
template<typename T> T
ServiceLocator::getOption(const string& key) const {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
        return boost::lexical_cast<T>(getOption(key));
#pragma GCC diagnostic pop
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
 *      0x cannot be used as a prefix to specify hexadecimal numbers.
 *      If an unsigned integral type is expected, a sign cannot
 *      precede it. 
 *      Wrap-around effects will occur when signed integral types are
 *      coerced into.
 *      An out-of-range coercion will result in a
 *      boost::bad_lexical_cast exception being thrown.
 *      "0" and "1" are the only supported ways to specify boolean
 *      values through strings.
 *      If a int8_t or uint8_t is expected, conversions are treated
 *      like they are being done to char types.
 * \throw boost::bad_lexical_cast
 *      The value could not be coerced to the given type.
 */
template<typename T> T
ServiceLocator::getOption(const string& key, T defaultValue) const {
    std::map<string, string>::const_iterator i = options.find(key);
    if (i == options.end())
        return defaultValue;
    return boost::lexical_cast<T>(i->second);
}

template<> const char*
ServiceLocator::getOption(const string& key) const;

template<> const char*
ServiceLocator::getOption(const string& key,
                          const char* defaultValue) const;

typedef std::vector<ServiceLocator> ServiceLocatorList;

} // end RAMCloud

#endif  // RAMCLOUD_SERVICELOCATOR_H
