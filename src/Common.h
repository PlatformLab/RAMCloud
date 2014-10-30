/* Copyright (c) 2009-2011 Stanford University
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
 * A header file that is included everywhere.
 * TODO(ongaro): A lot of this stuff should probably move elsewhere.
 */

#ifndef RAMCLOUD_COMMON_H
#define RAMCLOUD_COMMON_H

// Unfortunately, unit tests based on gtest can't access private members
// of classes.  If the following uppercase versions of "private" and
// "protected" are used instead, it works around the problem:  when
// compiling unit test files (anything that includes TestUtil.h)
// everything becomes public.

#ifdef EXPOSE_PRIVATES
#define PRIVATE public
#define PROTECTED public
#define PUBLIC public
#else
#define PRIVATE private
#define PROTECTED protected
#define PUBLIC public
#endif

#include <sys/time.h>

#define __STDC_LIMIT_MACROS
#include <cstdint>

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <xmmintrin.h>

#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <cassert>
#include <string>
#include <typeinfo>
#include <vector>
#include <boost/foreach.hpp>

namespace RAMCloud {
using std::string;
using std::pair;
using std::vector;
}

#define foreach BOOST_FOREACH
#define reverse_foreach BOOST_REVERSE_FOREACH

// A macro to disallow the copy constructor and operator= functions
#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&) = delete;             \
    TypeName& operator=(const TypeName&) = delete;
#endif

#include "Context.h"
#include "Logger.h"
#include "Status.h"

namespace RAMCloud {

// htons, ntohs cause warnings
#define HTONS(x) \
    static_cast<uint16_t>((((x) >> 8) & 0xff) | (((x) & 0xff) << 8))
#define NTOHS HTONS

/**
 * Useful for ignoring the results of functions that emit a warning when their
 * results are ignored. Not to discourage anyone, but if you're using this
 * macro, you're probably doing something hacky.
 */
#define IGNORE_RESULT(x) if (x) {}

/**
 * Return the number of elements in a statically allocated array.
 * Although #arrayLength() should be used where possible, this macro can appear
 * in constant expressions and that function can not.
 * \warning
 *      This will return bogus results for anything that's not an array.
 *      Prefer #arrayLength().
 */
#define unsafeArrayLength(array) (sizeof(array) / sizeof(array[0]))

/**
 * Cast a bigger int down to a smaller one.
 * Asserts that no precision is lost at runtime.
 */
template<typename Small, typename Large>
Small
downCast(const Large& large)
{
    Small small = static_cast<Small>(large);
    // The following comparison (rather than "large==small") allows
    // this method to convert between signed and unsigned values.
    assert(large-small == 0);
    return small;
}

/// Return the number of elements in a statically allocated array.
template<typename T, size_t length>
uint32_t
arrayLength(const T (&array)[length])
{
    return length;
}

uint64_t _generateRandom();

#if TESTING
extern uint64_t mockPMCValue;
extern uint64_t mockRandomValue;

static inline uint64_t
generateRandom()
{
    if (mockRandomValue)
        return mockRandomValue++;
    return _generateRandom();
}

class MockRandom {
    uint64_t original;
  public:
    explicit MockRandom(uint64_t value)
        : original(mockRandomValue)
    {
        mockRandomValue = value;
    }
    ~MockRandom()
    {
        mockRandomValue = original;
    }
};
#else
#define generateRandom() RAMCloud::_generateRandom()
#endif

uint32_t randomNumberGenerator(uint32_t n);

#if TESTING
#define VIRTUAL_FOR_TESTING virtual
#else
#define VIRTUAL_FOR_TESTING
#endif

string format(const char* format, ...)
    __attribute__((format(printf, 1, 2)));
string vformat(const char* format, va_list ap)
    __attribute__((format(printf, 1, 0)));

string demangle(const char* name);

} // namespace RAMCloud

#include "CodeLocation.h"
#include "Exception.h"

namespace RAMCloud {

bool pinToCpu(uint32_t cpu);
uint64_t getTotalSystemMemory();

// conveniences for dealing with maps

/// Return whether a map contains a given key.
template<typename Map>
bool
contains(const Map& map, const typename Map::key_type& key)
{
    return (map.find(key) != map.end());
}

/// See #get below.
struct NoSuchKeyException : public Exception {
    explicit NoSuchKeyException(const CodeLocation& where)
        : Exception(where) {}
    NoSuchKeyException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
    NoSuchKeyException(const CodeLocation& where, int errNo)
        : Exception(where, errNo) {}
    NoSuchKeyException(const CodeLocation& where, string msg, int errNo)
        : Exception(where, msg, errNo) {}
};

/**
 * Return the value for a given key in a map.
 * \throw NoSuchKeyException
 *      The map does not contain the given key.
 */
template<typename Map>
typename Map::mapped_type
get(const Map& map, const typename Map::key_type& key)
{
    typename Map::const_iterator it(map.find(key));
    if (it == map.end())
        throw NoSuchKeyException(HERE);
    return it->second;
}

/**
 * Return the offset of a field in a structure. This is identical to the
 * "offsetof" macro except that it uses 100 as the base address instead of
 * 0 (g++ refuses to compile with a 0 base address).
 */
#define OFFSET_OF(type, field) (reinterpret_cast<size_t> \
        (reinterpret_cast<char*>(&(reinterpret_cast<type*>(100)->field))) \
        - 100)

/**
 * Return the size of the given type as a uint32_t. This convenience macro
 * tavoids having downcasts everywhere we take sizeof, which returns size_t,
 * but want a uint32_t instead. Stay tuned for a fancier templated version
 * by syang0 and ongaro...
 */
#define sizeof32(type) downCast<uint32_t>(sizeof(type))

/**
 * The length of a cache line in bytes (or upper-bound estimate). Used to
 * insert padding into structures in order to ensure that some fields are
 * on different cache lines than others.
 */
#define CACHE_LINE_SIZE 64

/**
 * This macro forces the compiler to align allocated memory to the cash line
 * size. This is necessary as a performance boost for the data portion of
 * packet buffers in driver codes like SolarFlareDriver.
 */
#define CACHE_ALIGN  __attribute__((aligned(CACHE_LINE_SIZE)))

/**
 * Prefetch the cache lines containing [object, object + numBytes) into the
 * processor's caches.
 * The best docs for this are in the Intel instruction set reference under
 * PREFETCH.
 * \param object
 *      The start of the region of memory to prefetch.
 * \param numBytes
 *      The size of the region of memory to prefetch.
 */
static inline void
prefetch(const void* object, uint64_t numBytes)
{
    uint64_t offset = reinterpret_cast<uint64_t>(object) & 0x3fUL;
    const char* p = reinterpret_cast<const char*>(object) - offset;
    for (uint64_t i = 0; i < offset + numBytes; i += 64)
        _mm_prefetch(p + i, _MM_HINT_T0);
}

/**
 * Prefetch the cache lines containing the given object into the
 * processor's caches.
 * The best docs for this are in the Intel instruction set reference under
 * PREFETCHh.
 * \param object
 *      A pointer to the object in memory to prefetch.
 */
template<typename T>
static inline void
prefetch(const T* object)
{
    prefetch(object, sizeof(*object));
}

void pinAllMemory();

/**
 * Convert the annoying unix timeval structure into a single 64-bit microsecond
 * value.
 */
static inline uint64_t
timevalToMicroseconds(struct timeval* tv)
{
    return static_cast<uint64_t>(tv->tv_sec) * 1000000 + tv->tv_usec;
}

/*
 * A macro that's always defined that indicates whether or not this is a debug
 * build. Allows use of conditionals and lets the compiler remove dead code,
 * rather than sprinkling gross #ifndefs around.
 */
#ifdef NDEBUG
#define DEBUG_BUILD false
#else
#define DEBUG_BUILD true
#endif

/*
 * The following two macros are used in highly optimized code paths to hint
 * to the compiler what the expected truth value of a given expression is.
 * For instance, an 'if (expr) { ... }' statement in a hot code path might
 * benefit from being coded 'if (expect_true(expr)) { ... }' if we know that
 * 'expr' is usually true. If, instead, 'expr' is almost always false, one may
 * use the expect_false macro instead.
 */
#define expect_true(expr)   __builtin_expect((expr), true)
#define expect_false(expr)   __builtin_expect((expr), false)

} // end RAMCloud
#endif // RAMCLOUD_COMMON_H
