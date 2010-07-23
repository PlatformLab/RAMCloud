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

// requires 0x for cstdint
#include <stdint.h>

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
using std::string;
#endif

// cpplint thinks these are system headers since we're using angle brackets.
// Then it complains that they're not included first, but really it wants
// application headers last.
#include <config.h> // NOLINT
#include <rcrpc.h> // NOLINT

// A macro to disallow the copy constructor and operator= functions
// This should be used in the private: declarations for a class
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&);             \
    void operator=(const TypeName&)

#include <Logging.h> // NOLINT

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
void debug_dump64(const void *buf, uint64_t bytes);

#if PERF_COUNTERS
__inline __attribute__((always_inline, no_instrument_function))
uint64_t rdtsc();
uint64_t
rdtsc()
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
uint64_t rdpmc(uint32_t counter);
uint64_t
rdpmc(uint32_t counter)
{
    uint32_t hi, lo;
    __asm __volatile("rdpmc" : "=d" (hi), "=a" (lo) : "c" (counter));
    return ((uint64_t) lo) | (((uint64_t) hi) << 32);
}
#else
#define rdtsc() 0UL
#define rdpmc() 0UL;
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

/**
 * The base class for all RAMCloud exceptions.
 */
struct Exception {
    explicit Exception() : message(""), errNo(0) {}
    explicit Exception(std::string msg)
            : message(msg), errNo(0) {}
    explicit Exception(int errNo) : message(""), errNo(errNo) {
        message = strerror(errNo);
    }
    explicit Exception(std::string msg, int errNo)
            : message(msg), errNo(errNo) {}
    Exception(const Exception &e)
            : message(e.message), errNo(e.errNo) {}
    Exception &operator=(const Exception &e) {
        if (&e == this)
            return *this;
        message = e.message;
        errNo = e.errNo;
        return *this;
    }
    virtual ~Exception() {}
    std::string message;
    int errNo;
};

} // end RAMCloud

#endif
