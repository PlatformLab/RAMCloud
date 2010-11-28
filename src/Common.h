/* Copyright (c) 2009 Stanford University
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

#define __STDC_LIMIT_MACROS
#include <stdint.h> // requires 0x for cstdint

// #include <cinttypes> // this requires c++0x support because it's c99
// so we'll go ahead and use the C header
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#ifndef __cplusplus
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#else
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <cassert>
#include <string>
#include <typeinfo>
#include <vector>
#include <boost/foreach.hpp>
using std::string;
using std::pair;
using std::vector;
#define foreach BOOST_FOREACH
#endif

// A macro to disallow the copy constructor and operator= functions
// This should be used in the private: declarations for a class
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&);             \
    void operator=(const TypeName&)

#include "Logging.h"
#include "Status.h"

/**
 * Allocate a new memory area.
 * This works like malloc(3), except it will crash rather than return \c NULL
 * if the system is out of memory.
 * \param[in] _l
 *      The length for the memory area (a \c size_t).
 * \return
 *      A non-\c NULL pointer to the new memory area.
 */
#define xmalloc(_l)  _xmalloc(_l, __FILE__, __LINE__, __func__)
static inline void *
_xmalloc(size_t len, const char *file, const int line, const char *func)
{
    void *p = malloc(len > 0 ? len : 1);
    if (p == NULL) {
        fprintf(stderr, "malloc(%d) failed: %s:%d (%s)\n",
                len, file, line, func);
        exit(1);
    }

    return p;
}

/**
 * Allocate a new memory area with additional alignment requirements.
 * This works like posix_memalign(3) but returns the pointer to the allocated
 * memory area. It will crash rather if the system is out of memory or the
 * required alignment was invalid. You should free the pointer returned with
 * free() when you're done with it.
 * \param[in] _a
 *      The required alignment for the memory area, which must be a power of
 *      two and a multiple of the size of a void pointer. If you're passing 1,
 *      2, 4, or 8 here, you should probably be using xmalloc instead.
 * \param[in] _l
 *      The length for the memory area (a \c size_t).
 * \return
 *      A non-\c NULL pointer to the new memory area.
 */
#define xmemalign(_a, _l) _xmemalign(_a, _l, __FILE__, __LINE__, __func__)
static inline void *
_xmemalign(size_t alignment, size_t len,
           const char *file, const int line, const char *func)
{
    void *p;
    int r;

    // alignment must be a power of two
    if ((alignment & (alignment - 1)) != 0) {
        fprintf(stderr, "xmemalign alignment (%d) must be "
                        "a power of two: %s:%d (%s)\n",
                alignment, file, line, func);
        exit(1);
    }

    // alignment must be a multiple of sizeof(void*)
    if (alignment % sizeof(void*) != 0) { // NOLINT
        fprintf(stderr, "xmemalign alignment (%d) must be "
                        "a multiple of sizeof(void*): %s:%d (%s)\n",
                alignment, file, line, func);
        exit(1);
    }

    r = posix_memalign(&p, alignment, len > 0 ? len : 1);
    if (r != 0) {
        fprintf(stderr, "posix_memalign(%d, %d) failed: %s:%d (%s)\n",
                alignment, len, file, line, func);
        exit(1);
    }

    return p;
}

/**
 * Resize a previously allocated memory area.
 * This works like realloc(3), except it will crash rather than return \c NULL
 * if the system is out of memory.
 * \param[in] _p
 *      The pointer to the previously allocated memory area. This pointer is
 *      invalid after this function is called.
 * \param[in] _l
 *      The new length for the memory area (a \c size_t).
 * \return
 *      A non-\c NULL pointer to the new memory area.
 */
#define xrealloc(_p, _l) _xrealloc(_p, _l, __FILE__, __LINE__, __func__)
static inline void * _xrealloc(void *ptr, size_t len, const char* file,
                               const int line, const char* func) {
    void *p = realloc(ptr, len > 0 ? len : 1);
    if (p == NULL) {
        fprintf(stderr, "realloc(%d) failed: %s:%d (%s)\n",
                len, file, line, func);
        exit(1);
    }

    return p;
}

#define STATIC_ASSERT_CAT2(a, b) a##b
#define STATIC_ASSERT_CAT(a, b) STATIC_ASSERT_CAT2(a, b)
/**
 * Generate a compile-time error if \a x is false.
 * You can "call" this anywhere declaring an enum is allowed -- it doesn't
 * necessarily have to be inside a function.
 * \param x
 *      A condition that can be evaluated at compile-time.
 */
#define static_assert(x) enum { \
    STATIC_ASSERT_CAT(STATIC_ASSERT_FAILED_, __COUNTER__) = 1/(x) }

#ifdef __cplusplus
/**
 * Return the size in bytes of a struct, except consider the size of structs
 * with no members to be 0 bytes.
 */
#define sizeof0(x)  (__is_empty(x) ? 0 : sizeof(x))
#else
#define sizeof0(x) sizeof(x)
#endif

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

#ifdef __cplusplus

/// Return the number of elements in a statically allocated array.
template<typename T, size_t length>
uint32_t
arrayLength(const T (&array)[length])
{
    return length;
}

__inline __attribute__((always_inline, no_instrument_function))
uint64_t _rdtsc();
uint64_t
_rdtsc()
{
    uint32_t lo, hi;

#ifdef __GNUC__
    __asm__ __volatile__("rdtsc" : "=a" (lo), "=d" (hi));
#else
    asm("rdtsc" : "=a" (lo), "=d" (hi));
#endif

    return (((uint64_t)hi << 32) | lo);
}

__inline __attribute__((always_inline, no_instrument_function))
uint64_t _rdpmc(uint32_t counter);
uint64_t
_rdpmc(uint32_t counter)
{
    uint32_t hi, lo;
    __asm __volatile("rdpmc" : "=d" (hi), "=a" (lo) : "c" (counter));
    return ((uint64_t) lo) | (((uint64_t) hi) << 32);
}

namespace RAMCloud {
uint64_t _generateRandom();
}

/// Yield the current task to the scheduler.
static inline void
yield()
{
#if YIELD
    extern int sched_yield();
    sched_yield(); // always returns 0 on linux
#endif
}

#if TESTING
extern uint64_t mockTSCValue;
extern uint64_t mockPMCValue;
extern uint64_t mockRandomValue;
__inline __attribute__((always_inline, no_instrument_function))
uint64_t rdtsc();
uint64_t
rdtsc()
{
    if (mockTSCValue)
        return mockTSCValue;
    return _rdtsc();
}
__inline __attribute__((always_inline, no_instrument_function))
uint64_t rdpmc(uint32_t counter);
uint64_t
rdpmc(uint32_t counter)
{
    if (mockTSCValue)
        return mockPMCValue;
    return _rdpmc(counter);
}
class MockTSC {
    uint64_t original;
  public:
    explicit MockTSC(uint64_t value)
        : original(mockTSCValue)
    {
        mockTSCValue = value;
    }
    ~MockTSC()
    {
        mockTSCValue = original;
    }
};
__inline __attribute__((always_inline, no_instrument_function))
uint64_t generateRandom(void);
uint64_t
generateRandom()
{
    if (mockRandomValue)
        return mockRandomValue;
    return RAMCloud::_generateRandom();
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
#define rdtsc() _rdtsc()
#define rdpmc(c) _rdpmc(c)
#define generateRandom() RAMCloud::_generateRandom()
#endif

#if TESTING
#undef PRODUCTION
#else
#define PRODUCTION 1
#endif

#if TESTING
#define VIRTUAL_FOR_TESTING virtual
#else
#define VIRTUAL_FOR_TESTING
#endif

#if TESTING
#define CONST_FOR_PRODUCTION
#else
#define CONST_FOR_PRODUCTION const
#endif

#if PERF_COUNTERS
#define STAT_REF(pc) &(pc)
#define STAT_INC(pc) ++(pc)
#else
#define STAT_REF(pc) NULL
#define STAT_INC(pc) (void) 0
#endif

#endif

namespace RAMCloud {

string format(const char* format, ...)
    __attribute__((format(printf, 1, 2)));

string& format(string& s, const char* format, ...)
    __attribute__((format(printf, 2, 3)));

/**
 * Describes the location of a line of code.
 * You can get one of these with #HERE.
 */
struct CodeLocation {
    /// Called by #HERE only.
    CodeLocation(const char* file,
                 const uint32_t line,
                 const char* function,
                 const char* prettyFunction)
        : file(file)
        , line(line)
        , function(function)
        , prettyFunction(prettyFunction)
    {}
    string str() const {
        return format("%s at %s:%d", prettyFunction, file, line);
    }

    /// __FILE__
    const char* file;
    /// __LINE__
    uint32_t line;
    /// __func__
    const char* function;
    /// __PRETTY_FUNCTION__
    const char* prettyFunction;
};

/**
 * Constructs a #CodeLocation describing the line from where it is used.
 */
#define HERE \
    RAMCloud::CodeLocation(__FILE__, __LINE__, __func__, __PRETTY_FUNCTION__)

string demangle(const char* name);

/**
 * The base class for all RAMCloud exceptions.
 */
struct Exception {
    explicit Exception(const CodeLocation& where)
        : message(""), errNo(0), where(where) {}
    Exception(const CodeLocation& where, std::string msg)
        : message(msg), errNo(0), where(where) {}
    Exception(const CodeLocation& where, int errNo)
        : message(""), errNo(errNo), where(where) {
        message = strerror(errNo);
    }
    Exception(const CodeLocation& where, string msg, int errNo)
        : message(msg), errNo(errNo), where(where) {}
    string str() const {
        return (demangle(typeid(*this).name()) + ": " + message +
                " thrown at " + where.str());
    }
    virtual ~Exception() {}
    string message;
    int errNo;
    CodeLocation where;
};

/**
 * A fatal error that should exit the program.
 */
struct FatalError : public Exception {
    explicit FatalError(const CodeLocation& where)
        : Exception(where) {}
    FatalError(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
    FatalError(const CodeLocation& where, int errNo)
        : Exception(where, errNo) {}
    FatalError(const CodeLocation& where, string msg, int errNo)
        : Exception(where, msg, errNo) {}
};

void debug_dump64(const void *buf, uint64_t bytes);
class Buffer;
void debug_dump64(Buffer& buffer);
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
 * Return the first element of a pair.
 *
 * Useful for projection of pair elements using functions which take a type
 * that models UnaryFunction.
 */
template <typename T, typename _>
T first(pair<T, _> p)
{
    return p.first;
}

/**
 * Return the second element of a pair.
 *
 * Useful for projection of pair elements using functions which take a type
 * that models UnaryFunction.
 */
template <typename T, typename _>
T second(pair<_, T> p)
{
    return p.second;
}

} // end RAMCloud

#endif
