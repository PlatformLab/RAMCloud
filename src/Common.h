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

// RAMCloud pragma [CPPLINT=0]

#ifndef RAMCLOUD_COMMON_H
#define RAMCLOUD_COMMON_H

#include <config.h>

#ifdef __cplusplus
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#else
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#endif

// requires 0x for cstdint
#include <stdint.h>

// #include <cinttypes> // this requires c++0x support because it's c99
// so we'll go ahead and use the C header
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <rcrpc.h>

// A macro to disallow the copy constructor and operator= functions
// This should be used in the private: declarations for a class
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&);             \
    void operator=(const TypeName&)

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
    if (alignment % sizeof(void*) != 0) {
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

/*
 * static_assert(x) will generate a compile-time error if 'x' is false.
 */
#define static_assert(x) do { \
        switch (x) { default: case 0: case (x): break; } \
    } while (0)

#ifdef __cplusplus
void debug_dump64(const void *buf, uint64_t bytes);
uint64_t rdtsc();

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

namespace RAMCloud {

class AssertionException {};

void assert(bool invariant);

}
#endif

#endif
