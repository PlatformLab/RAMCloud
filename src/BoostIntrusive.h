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
 * Header file for conveniences for using boost::intrusive.
 */

#ifndef RAMCLOUD_BOOSTINTRUSIVE_H
#define RAMCLOUD_BOOSTINTRUSIVE_H

#include <boost/intrusive/list.hpp>

namespace RAMCloud {

// Intrusive doubly-linked lists:

/**
 * The type that you should put into your class to add instances of that class
 * to an intrusive list.
 * For a usage example, see BoostIntrusiveTest::test_list_example().
 */
typedef boost::intrusive::list_member_hook<> IntrusiveListHook;

/**
 * Create a name for the type of an intrusive list.
 * For a usage example, see BoostIntrusiveTest::test_list_example().
 * \param entryType
 *      The name of the class whose instances you intend to add to intrusive
 *      lists of this type.
 * \param hookName
 *      The name of the member of type IntrusiveListHook that you want to use
 *      for this intrusive list.
 * \return
 *      C++ code following which you should put a name for the type of your
 *      intrusive list and a semicolon.
 */
#define INTRUSIVE_LIST_TYPEDEF(entryType, hookName) \
    typedef boost::intrusive::list < entryType, \
                boost::intrusive::member_hook < entryType, \
                    IntrusiveListHook, \
                    &entryType::hookName> >

} // end RAMCloud

#endif  // RAMCLOUD_BOOSTINTRUSIVE_H
