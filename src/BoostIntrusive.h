/* Copyright (c) 2010-2017 Stanford University
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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Weffc++"
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#pragma GCC diagnostic pop

namespace RAMCloud {

// Intrusive doubly-linked lists and sets (set, multiset, and rbtree):

/**
 * The type that you should put into your class to add instances of that class
 * to an intrusive list.
 * For a usage example, see BoostIntrusiveTest::test_list_example().
 */
typedef boost::intrusive::list_member_hook<> IntrusiveListHook;

/**
 * The type that you should put into your class to add instances of that class
 * to an intrusive set (which includes sets, multisets, and rbtrees).
 */
typedef boost::intrusive::set_member_hook<> IntrusiveSetHook;

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

/**
 * Create a name for the type of an intrusive set.
 * \param entryType
 *      The name of the class whose instances you intend to add to intrusive
 *      sets of this type.
 * \param hookName
 *      The name of the member of type IntrusiveSetHook that you want to use
 *      for this intrusive set.
 * \param comparerType
 *      The name of the class whose operator() is used to compare two instances
 *      of entryTypes to figure out if one is greater than the other. This is
 *      the functor used to sort elements in the set.
 * \return
 *      C++ code following which you should put a name for the type of your
 *      intrusive set and a semicolon.
 */
#define INTRUSIVE_SET_TYPEDEF(entryType, hookName, comparerType) \
    typedef boost::intrusive::set < entryType, \
                boost::intrusive::member_hook < entryType, \
                    IntrusiveSetHook, \
                    &entryType::hookName>, \
                boost::intrusive::compare < comparerType > >

/**
 * Create a name for the type of an intrusive multiset.
 * \param entryType
 *      The name of the class whose instances you intend to add to intrusive
 *      multisets of this type.
 * \param hookName
 *      The name of the member of type IntrusiveSetHook that you want to use
 *      for this intrusive multiset.
 * \param comparerType
 *      The name of the class whose operator() is used to compare two instances
 *      of entryTypes to figure out if one is greater than the other. This is
 *      the functor used to sort elements in the multiset.
 * \return
 *      C++ code following which you should put a name for the type of your
 *      intrusive multiset and a semicolon.
 */
#define INTRUSIVE_MULTISET_TYPEDEF(entryType, hookName, comparerType) \
    typedef boost::intrusive::multiset < entryType, \
                boost::intrusive::member_hook < entryType, \
                    IntrusiveSetHook, \
                    &entryType::hookName>, \
                boost::intrusive::compare < comparerType > >

/**
 * Create a name for the type of an intrusive rbtree.
 * \param entryType
 *      The name of the class whose instances you intend to add to intrusive
 *      rbtree of this type.
 * \param hookName
 *      The name of the member of type IntrusiveSetHook that you want to use
 *      for this intrusive rbtree.
 * \param comparerType
 *      The name of the class whose operator() is used to compare two instances
 *      of entryTypes to figure out if one is greater than the other. This is
 *      the functor used to sort elements in the rbtree.
 * \return
 *      C++ code following which you should put a name for the type of your
 *      intrusive rbtree and a semicolon.
 */
#define INTRUSIVE_RBTREE_TYPEDEF(entryType, hookName, comparerType) \
    typedef boost::intrusive::rbtree < entryType, \
                boost::intrusive::member_hook < entryType, \
                    IntrusiveSetHook, \
                    &entryType::hookName>, \
                boost::intrusive::compare < comparerType > >

template<typename List, typename Node>
void
insertBefore(List& list, Node& newNode, const Node& nextNode) {
    list.insert(list.iterator_to(nextNode), newNode);
}

template<typename Container, typename Node>
void
erase(Container& container, Node& node) {
    container.erase(container.iterator_to(node));
}

} // end RAMCloud

#endif  // RAMCLOUD_BOOSTINTRUSIVE_H
