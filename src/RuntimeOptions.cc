/* Copyright (c) 2012 Stanford University
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

#include <sstream>

#include "RuntimeOptions.h"

namespace RAMCloud {

namespace {
/**
 * Catch-all template class which is intentionally undefined.
 * Specializations provide implementations which parse configuration option
 * string into fields of the chosen type.
 */
template <typename T>
struct Parser;

/**
 * Specialization which parses strings of form "a b c" to std::queue<T>
 * where 'a', 'b', 'c' are tokens which convert in a straightforward way
 * to type T.
 * If istream_iterator cannot parse some token in the given string it
 * simply stop adding elements to the queue.
 */
template <typename T>
struct Parser<std::queue<T>> : public RuntimeOptions::Parseable {
    explicit Parser(std::queue<T> & target)
        : target(target)
    {}

    void
    parse(const char* value)
    {
        while (!target.empty())
            target.pop();
        std::istringstream iss(value);
        auto begin = std::istream_iterator<T>(iss);
        auto end = std::istream_iterator<T>();
        for (auto it = begin; it != end; ++it)
            target.push(*it);
    }

    std::queue<T>& target;
};

/// Helper function to make declaring a new option easier.
template <typename T>
Parser<T>*
newParser(T& obj)
{
    return new Parser<T>(obj);
}
}

/**
 * Constructor. All runtime options must be registered in the constructor.
 * To create a runtime option simply add a field then add a line below:
 * REGISTER(myNewOption). If there isn't a existing parser for the
 * type of that field then you'll have to define a new specialiation
 * for Parser above.
 */
RuntimeOptions::RuntimeOptions()
    : parsers()
    , mutex()
    , failRecoveryMasters()
{
#define REGISTER(field) registerOption(#field, newParser(field))
    REGISTER(failRecoveryMasters);
#undef REGISTER
}

/// Free all parsers created in the constructor.
RuntimeOptions::~RuntimeOptions()
{
    foreach (const auto& item, parsers)
        delete item.second;
}

/**
 * Sets a runtime option field to the indicated value.
 *
 * \param option
 *      String name which corresponds to a member field in this class (e.g.
 *      "failRecoveryMasters") whose value should be replaced with the given
 *      value.
 * \param value
 *      String which can be parsed into the type of the field indicated by
 *      \a option. The format is specific to the type of each field but is
 *      generally either a single value (e.g. "10", "word") or a collection
 *      separated by spaces (e.g. "1 2 3", "first second"). See the type of
 *      the field of interest as well the specializations for Parser for more
 *      information.
 */
void
RuntimeOptions::set(const char* option, const char* value)
{
    Lock _(mutex);
    parsers.at(option)->parse(value);
}

// - Option-specific methods -

/**
 * Pop and return the next element of #failRecoveryMasters. If
 * #failRecoveryMaster is empty then return 0.
 */
uint32_t
RuntimeOptions::popFailRecoveryMasters()
{
    Lock _(mutex);
    uint32_t result = 0;
    if (!failRecoveryMasters.empty()) {
        result = failRecoveryMasters.front();
        failRecoveryMasters.pop();
    }
    return result;
}

// - private -

/**
 * Register a parser for a specific field. Generally not called directly;
 * see REGISTER() in RuntimeOptions().
 *
 * \param option
 *      String name this field should be exposed to clients as. Preferrably
 *      the same as the identifier used in the C++ code.
 * \param parser
 *      Parser which, given a string, populates the field name associcated
 *      with \a option.
 */
void
RuntimeOptions::registerOption(const char* option, Parseable* parser)
{
    Lock _(mutex);
    parsers[option] = parser;
}

} // namespace RAMCloud
