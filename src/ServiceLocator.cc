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

#include "ServiceLocator.h"

namespace RAMCloud {

/**
 * Parse a list of ServiceLocator objects from a service locator string.
 * \param[in] serviceLocator
 *      A ';'-delimited list of \ref ServiceLocatorStrings.
 * \return
 *      ServiceLocator objects parsed from \a serviceLocator.
 * \throw BadServiceLocatorException
 *      Any part of \a serviceLocator could not be parsed.
 */
vector<ServiceLocator>
ServiceLocator::parseServiceLocators(const string& serviceLocator)
{
    vector<ServiceLocator> ret;
    pcrecpp::StringPiece remainingServiceLocator(serviceLocator);
    while (!remainingServiceLocator.empty()) {
        ServiceLocator locator;
        locator.init(&remainingServiceLocator);
        ret.push_back(std::move(locator));
    }
    return ret;
}

/**
 * Private constructor needed for #parseServiceLocators().
 */
ServiceLocator::ServiceLocator()
    : originalString(), protocol(), options() {}

/**
 * Construct a ServiceLocator from a string representation.
 * \param serviceLocator
 *      A \ref ServiceLocatorStrings with a single way of accessing a service.
 *      If you have a string that might describe multiple ways of accessing a
 *      service, you want #parseServiceLocators() instead.
 * \throw BadServiceLocatorException
 *      \a serviceLocator could not be parsed.
 */
ServiceLocator::ServiceLocator(const string& serviceLocator)
    : originalString(), protocol(), options()
{
    pcrecpp::StringPiece serviceLocatorPiece(serviceLocator);
    init(&serviceLocatorPiece);
    if (!serviceLocatorPiece.empty()) {
        throw BadServiceLocatorException(HERE, serviceLocator,
                                         serviceLocatorPiece.as_string());
    }
}

/**
 * Initialize a ServiceLocator from a string representation.
 * This is a private helper for the constructor and #parseServiceLocators().
 * \param remainingServiceLocator
 *      A ';'-delimited list of \ref ServiceLocatorStrings. This will be
 *      advanced through the next ';'.
 * \throw BadServiceLocatorException
 *      The first alternative way of accessing the service specified in \a
 *      remainingServiceLocator could not be parsed.
 */
void
ServiceLocator::init(pcrecpp::StringPiece* remainingServiceLocator)
{

    // Building up regular expressions to parse the service locator string:

// Optional whitespace.
#define SPACES " *"

// The protocol.
#define PROTOCOL "[\\w\\+]+"
// A pattern to consume a protocol, up to and including the ':' and any spaces
// following it and an optional semicolon after that. The first group is the
// protocol and the second is the optional semicolon.
#define PROTOCOL_PATTERN SPACES "(" PROTOCOL ")" SPACES ":" SPACES "(;)?"
    static const pcrecpp::RE protocolRe(PROTOCOL_PATTERN);
    assert(protocolRe.NumberOfCapturingGroups() == 2);
#undef PROTOCOL_PATTERN
#undef PROTOCOL

// A key for an option.
#define KEY_NAME "\\w+"
// A value that is not enclosed in quotes.
#define UNQUOTED_VALUE "(?:[^\\\\\",;]+|\\\\.)*"
// A value that is enclosed in quotes.
#define QUOTED_VALUE "\"(?:[^\\\\\"]+|\\\\.)*\""
// A value that may have been enclosed in quotes. The first group is the value,
// including quotes (if present).
#define MAY_BE_QUOTED_VALUE "(" UNQUOTED_VALUE "|" QUOTED_VALUE ")"
// A key, equals sign, and a value. The first group is the key, and the second
// group is the value, including quotes (if present).
#define KEY_VALUE  "(" KEY_NAME ")" SPACES "=" SPACES MAY_BE_QUOTED_VALUE
// A pattern to consume a key value pair, up to the end of the line or
// semicolon. The first group is the key, the second group is the value,
// including quotes (if present), and the third group is the semicolon that
// might be present.
#define KEY_VALUE_PATTERN KEY_VALUE SPACES "(?:," SPACES "|(;)|$)"
    static const pcrecpp::RE keyValueRe(KEY_VALUE_PATTERN);
    assert(keyValueRe.NumberOfCapturingGroups() == 3);
#undef KEY_VALUE_PATTERN
#undef KEY_VALUE
#undef MAY_BE_QUOTED_VALUE
#undef QUOTED_VALUE
#undef UNQUOTED_VALUE
#undef KEY_NAME

#undef SPACES

    // A replacement pattern to remove slashes from quoted values.
    // Each match should be replaced with the first group, which is the
    // character that was escaped (a quote).
    static const pcrecpp::RE unescapeQuotedRe("\\\\(\")");
    assert(unescapeQuotedRe.NumberOfCapturingGroups() == 1);

    // A replacement pattern to remove slashes from unquoted value.
    // Each match should be replaced with the first group, which is the
    // character that was escaped (a quote, comma, or semicolon).
    static const pcrecpp::RE unescapeUnquotedRe("\\\\([\",;])");
    assert(unescapeUnquotedRe.NumberOfCapturingGroups() == 1);

    // A replacement pattern to remove leading and trailing whitespace.
    // Each match should be replaced with the empty string.
    static const pcrecpp::RE stripRe("(?:^ *| *$)");
    assert(stripRe.NumberOfCapturingGroups() == 0);


    pcrecpp::StringPiece originalRemaining(*remainingServiceLocator);

    // Stop parsing if this is non-empty. The ';' is stored here.
    string sentinel;

    // Parse out the protocol.
    bool r = protocolRe.Consume(remainingServiceLocator, &protocol, &sentinel);
    if (!r) {
        throw BadServiceLocatorException(HERE, originalRemaining.as_string(),
                                         remainingServiceLocator->as_string());
    }

    // Each iteration through the following loop parses one key-value pair.
    while (sentinel.empty() && !remainingServiceLocator->empty()) {
        string key;
        string value;
        bool r = keyValueRe.Consume(remainingServiceLocator, &key, &value,
                                    &sentinel);
        if (!r) {
            throw BadServiceLocatorException(HERE,
                                             originalRemaining.as_string(),
                                         remainingServiceLocator->as_string());
        }
        if (!value.empty()) {
            if (value[0] == '"') {
                // The value was quoted. Strip the surrounding quotes and
                // remove escape characters from internal quotes.
                value.erase(0, 1);
                value.erase(value.size() - 1, 1);
                unescapeQuotedRe.GlobalReplace("\\1", &value);
            } else {
                // The value was not quoted. Remove escape characters from
                // quotes, commas, and semicolons.
                unescapeUnquotedRe.GlobalReplace("\\1", &value);
            }
        }
        options[key] = value;
    }

    // Set originalString from the first part of originalRemaining.
    originalRemaining.remove_suffix(remainingServiceLocator->size() +
                                    downCast<uint32_t>(sentinel.size()));
    originalRemaining.CopyToString(&originalString);

    // Strip leading and trailing whitespace from originalString.
    stripRe.GlobalReplace("", &originalString);
}

// Simpler form without a type template parameter, identical to:
// T getOption<T=const string&>(const string& key)
// (This isn't a doxygen comment because doxygen concatenates this
// documentation with that of getOption<> for getOption<>).
const string&
ServiceLocator::getOption(const string& key) const
{
    std::map<string, string>::const_iterator i = options.find(key);
    if (i == options.end())
        throw NoSuchKeyException(HERE, key);
    return i->second;
}

// Simpler form without a type template parameter, identical to:
// T getOption<T=const string&>(const string& key, T defaultValue)
const string&
ServiceLocator::getOption(const string& key,
                          const string& defaultValue) const
{
    std::map<string, string>::const_iterator i = options.find(key);
    if (i == options.end())
        return defaultValue;
    return i->second;
}

// Specializations for const char* because lexical_cast doesn't handle it well.
template<> const char*
ServiceLocator::getOption(const string& key) const
{
    return getOption(key).c_str();
}

template<> const char*
ServiceLocator::getOption(const string& key, const char* defaultValue)
const
{
    std::map<string, string>::const_iterator i = options.find(key);
    if (i == options.end())
        return defaultValue;
    return i->second.c_str();
}


} // namespace RAMCloud
