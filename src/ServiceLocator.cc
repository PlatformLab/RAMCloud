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
 * Implementation for #RAMCloud::ServiceLocator.
 */

#include "ServiceLocator.h"

namespace RAMCloud {

/**
 * Used in #getOption() to convert string values to requested types.
 */
StringConverter ServiceLocator::stringConverter;

/**
 * Construct a service locator from a string representation.
 * \param serviceLocator
 *      A string like "tcp: host=example.org, port=8081". If an option is
 *      specified multiple times in the string, only the last time is used.
 *      Double-quotes and commas should be escaped with a backslash in unquoted
 *      values for options.
 * \throw BadServiceLocatorException
 *      \a serviceLocator could not be parsed.
 */
ServiceLocator::ServiceLocator(const string& serviceLocator)
    : originalString(serviceLocator),
      originalProtocol(), protocolStack(), protocolStackIndex(0),
      options()
{

    // Building up regular expressions to parse the service locator string:

// Optional whitespace.
#define SPACES " *"

// A '+'-separated component of a protocol.
#define PROTOCOL "\\w+"
// A pattern to consume a protocol component, up to and including the ':'. The
// first group is the protocol component, and the second group is a ':' for the
// last protocol component or empty otherwise.
#define PROTOCOL_PATTERN SPACES "(" PROTOCOL ")(?:\\+|" SPACES "(:)" SPACES ")"
    static const pcrecpp::RE protocolRe(PROTOCOL_PATTERN);
    assert(protocolRe.NumberOfCapturingGroups() == 2);
#undef PROTOCOL_PATTERN
#undef PROTOCOL

// A key for an option.
#define KEY_NAME "\\w+"
// A value that is not enclosed in quotes.
#define UNQUOTED_VALUE "(?:[^\\\\\",]+|\\\\.)*"
// A value that is enclosed in quotes.
#define QUOTED_VALUE "\"(?:[^\\\\\"]+|\\\\.)*\""
// A value that may have been enclosed in quotes. The first group is the value,
// including quotes (if present).
#define MAY_BE_QUOTED_VALUE "(" UNQUOTED_VALUE "|" QUOTED_VALUE ")"
// A key, equals sign, and a value. The first group is the key, and the second
// group is the value, including quotes (if present).
#define KEY_VALUE  "(" KEY_NAME ")" SPACES "=" SPACES MAY_BE_QUOTED_VALUE
// A pattern to consume a key value pair, up to the end of the line. The first
// group is the key, and the second group is the value, including quotes (if
// present).
#define KEY_VALUE_PATTERN KEY_VALUE SPACES "(?:," SPACES "|$)"
    static const pcrecpp::RE keyValueRe(KEY_VALUE_PATTERN);
    assert(keyValueRe.NumberOfCapturingGroups() == 2);
#undef KEY_VALUE_PATTERN
#undef KEY_VALUE
#undef MAY_BE_QUOTED_VALUE
#undef QUOTED_VALUE
#undef UNQUOTED_VALUE
#undef KEY_NAME

#undef SPACES

    pcrecpp::StringPiece remainingSubject(serviceLocator);

    // Each iteration through the following loop parses one element of the
    // protocol.
    while (true) {
        string protocolComponent;
        // Sentinel will be non-empty when there are no more protocol
        // components to consume.
        string sentinel;
        bool r = protocolRe.Consume(&remainingSubject, &protocolComponent,
                                    &sentinel);
        if (!r) {
            throw BadServiceLocatorException(serviceLocator,
                                             remainingSubject.as_string());
        }
        protocolStack.push_back(protocolComponent);
        originalProtocol += protocolComponent;
        if (sentinel.empty())
            originalProtocol += "+";
        else
            break;
    }

    // Each iteration through the following loop parses one key-value pair.
    while (!remainingSubject.empty()) {
        string key;
        string value;
        bool r = keyValueRe.Consume(&remainingSubject, &key, &value);
        if (!r) {
            throw BadServiceLocatorException(serviceLocator,
                                             remainingSubject.as_string());
        }
        if (!value.empty() && value[0] == '"') {
            // The value was quoted. Strip the surrounding quotes and remove
            // escape characters from internal quotes.
            value.erase(0, 1);
            value.erase(value.size() - 1, 1);
            pcrecpp::RE("\\\\(\")").GlobalReplace("\\1", &value);
        } else {
            // The value was not quoted. Remove escape characters from
            // quotes and commas.
            pcrecpp::RE("\\\\([\",])").GlobalReplace("\\1", &value);
        }
        options[key] = value;
    }
}

// Simpler form without a type template parameter, identical to:
// T getOption<T=const string&>(const string& key)
// (This isn't a doxygen comment because doxygen concatenates this
// documentation with that of getOption<> for getOption<>).
const string&
ServiceLocator::getOption(const string& key) const
{
    return getOption<const string&>(key);
}

// Simpler form without a type template parameter, identical to:
// T getOption<T=const string&>(const string& key, T defaultValue)
const string&
ServiceLocator::getOption(const string& key, const string& defaultValue) const
{
    return getOption<const string&>(key, defaultValue);
}

} // namespace RAMCloud
