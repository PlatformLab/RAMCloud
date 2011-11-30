/* Copyright (c) 2011 Stanford University
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

#include "Memory.h"

namespace RAMCloud {
namespace Memory {

/**
 * Allocate a new memory area.
 * This works like malloc(3), except it will crash rather than return \c NULL
 * if the system is out of memory.
 * \param[in] where
 *      The result of #HERE.
 * \param[in] len
 *      The length for the memory area (a \c size_t).
 * \return
 *      A non-\c NULL pointer to the new memory area.
 */
void*
xmalloc(const CodeLocation& where, size_t len)
{
    void *p = malloc(len > 0 ? len : 1);
    if (p == NULL) {
        fprintf(stderr, "malloc(%lu) failed: %s:%d (%s)\n",
                len, where.file, where.line, where.function);
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
 * \param[in] where
 *      The result of #HERE.
 * \param[in] alignment
 *      The required alignment for the memory area, which must be a power of
 *      two and a multiple of the size of a void pointer. If you're passing 1,
 *      2, 4, or 8 here, you should probably be using xmalloc instead.
 * \param[in] len
 *      The length for the memory area (a \c size_t).
 * \return
 *      A non-\c NULL pointer to the new memory area.
 */
void*
xmemalign(const CodeLocation& where, size_t alignment, size_t len)
{
    void *p;
    int r;

    // alignment must be a power of two
    if ((alignment & (alignment - 1)) != 0) {
        fprintf(stderr, "xmemalign alignment (%lu) must be "
                        "a power of two: %s:%d (%s)\n",
                alignment, where.file, where.line, where.function);
        exit(1);
    }

    // alignment must be a multiple of sizeof(void*)
    if (alignment % sizeof(void*) != 0) { // NOLINT
        fprintf(stderr, "xmemalign alignment (%lu) must be "
                        "a multiple of sizeof(void*): %s:%d (%s)\n",
                alignment, where.file, where.line, where.function);
        exit(1);
    }

    r = posix_memalign(&p, alignment, len > 0 ? len : 1);
    if (r != 0) {
        fprintf(stderr, "posix_memalign(%lu, %lu) failed: %s:%d (%s)\n",
                alignment, len, where.file, where.line, where.function);
        exit(1);
    }

    return p;
}

char*
xstrdup(const CodeLocation& where, const char* str)
{
    char *p = strdup(str);
    if (p == NULL) {
        fprintf(stderr, "strdup(%s) failed: %s:%d (%s)\n",
                str, where.file, where.line, where.function);
        exit(1);
    }

    return p;
}

} // namespace Memory
} // namespace RAMCloud
