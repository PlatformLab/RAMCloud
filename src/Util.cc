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

#include "Util.h"

namespace RAMCloud {
namespace Util {

/**
 * Generate a random string.
 *
 * \param str
 *      Pointer to location where the string generated will be stored.
 * \param length
 *      Length of the string to be generated in bytes.
 */
void
genRandomString(char* str, const int length) {
    static const char alphanum[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    for (int i = 0; i < length; ++i) {
        str[i] = alphanum[generateRandom() % (sizeof(alphanum) - 1)];
    }
}

/**
 * Return (potentially multi-line) string hex dump of a binary buffer in
 * 'hexdump -C' style.
 * Note that this exceeds 80 characters due to 64-bit offsets.
 */
string
hexDump(const void *buf, uint64_t bytes)
{
    const unsigned char *cbuf = reinterpret_cast<const unsigned char *>(buf);
    uint64_t i, j;

    std::ostringstream output;
    for (i = 0; i < bytes; i += 16) {
        char offset[17];
        char hex[16][3];
        char ascii[17];

        snprintf(offset, sizeof(offset), "%016" PRIx64, i);
        offset[sizeof(offset) - 1] = '\0';

        for (j = 0; j < 16; j++) {
            if ((i + j) >= bytes) {
                snprintf(hex[j], sizeof(hex[0]), "  ");
                ascii[j] = '\0';
            } else {
                snprintf(hex[j], sizeof(hex[0]), "%02x",
                    cbuf[i + j]);
                hex[j][sizeof(hex[0]) - 1] = '\0';
                if (isprint(static_cast<int>(cbuf[i + j])))
                    ascii[j] = cbuf[i + j];
                else
                    ascii[j] = '.';
            }
        }
        ascii[sizeof(ascii) - 1] = '\0';

        output <<
            format("%s  %s %s %s %s %s %s %s %s  %s %s %s %s %s %s %s %s  "
                   "|%s|\n", offset, hex[0], hex[1], hex[2], hex[3], hex[4],
                   hex[5], hex[6], hex[7], hex[8], hex[9], hex[10], hex[11],
                   hex[12], hex[13], hex[14], hex[15], ascii);
    }
    return output.str();
}

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

/** 
 * Used for testing: if nonzero then this will be returned as the result of the
 * next call to read_pmc().
 */
uint64_t mockPmcValue = 0;

} // namespace Util
} // namespace RAMCloud
