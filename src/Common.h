/* Copyright (c) 2009-2015 Stanford University
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
 */

#ifndef RAMCLOUD_COMMON_H
#define RAMCLOUD_COMMON_H

#include <sys/time.h>

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <xmmintrin.h>

#include <cinttypes>
#include <typeinfo>
#include <boost/foreach.hpp>

#define foreach BOOST_FOREACH
#define reverse_foreach BOOST_REVERSE_FOREACH

#include "Minimal.h"
#include "Context.h"
#include "Exception.h"
#include "Logger.h"
#include "Status.h"

namespace RAMCloud {

/**
 * GCC 4.6 and above introduced the constexpr keyword, but it's not backward
 * compatible with earlier versions. The CONSTEXPR_VAR and CONSTEXPR_FUNC
 * macros allow us to support both newer and older versions of GCC by providing
 * backward compatible and sematicaically correct conversions of the constexpr
 * keyword for variables and fucntions respectively.  GCC 4.6 and above requires
 * some uses of constexpr (in the form of warnings); for instance const double's
 * must be constexpr double's.
 */
#if __cpp_constexpr
    #define CONSTEXPR_VAR constexpr
#else
    #define CONSTEXPR_VAR const
#endif
#if __cpp_constexpr
    #define CONSTEXPR_FUNC constexpr
#else
    #define CONSTEXPR_FUNC
#endif

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

string demangle(const char* name);

} // namespace RAMCloud

#include "CodeLocation.h"

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

/**
 * The following variables are used for convenience of debugging and
 * experimentation, while minimizing the need to recompile across runs.
 *
 * Their meanings are determined by the current experiment, and should never be
 * used in production.
 */
extern uint64_t debugXXX;
extern double debugYYY;

} // end RAMCloud
#endif // RAMCLOUD_COMMON_H
