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

#include "Util.h"

namespace RAMCloud {
namespace Util {

/**
 * Returns true if timespec \c t1 refers to an earlier time than \c t2.
 *
 * \param t1
 *      First timespec to compare.
 * \param t2
 *      Second timespec to compare.
 */
bool
timespecLess(const struct timespec& t1, const struct timespec& t2)
{
    return (t1.tv_sec < t2.tv_sec) ||
            ((t1.tv_sec == t2.tv_sec) && (t1.tv_nsec < t2.tv_nsec));
}

/**
 * Returns true if timespec \c t1 refers to an earlier time than
 * \c t2 or the same time.
 *
 * \param t1
 *      First timespec to compare.
 * \param t2
 *      Second timespec to compare.
 */
bool
timespecLessEqual(const struct timespec& t1, const struct timespec& t2)
{
    return (t1.tv_sec < t2.tv_sec) ||
            ((t1.tv_sec == t2.tv_sec) && (t1.tv_nsec <= t2.tv_nsec));
}

/**
 * Return the sum of two timespecs.  The timespecs do not need to be
 * normalized (tv_nsec < 1e09), but the result will be.
 *
 * \param t1
 *      First timespec to add.
 * \param t2
 *      Second timespec to add.
 */
struct timespec
timespecAdd(const struct timespec& t1, const struct timespec& t2)
{
    struct timespec result;
    result.tv_sec = t1.tv_sec + t2.tv_sec;
    uint64_t nsec = t1.tv_nsec + t2.tv_nsec;
    result.tv_sec += nsec/1000000000;
    result.tv_nsec = nsec%1000000000;
    return result;
}

} // namespace Util
} // namespace RAMCloud
