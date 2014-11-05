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

/**
 * \file
 * This file defines a class (BitOps) containing a collection of fast
 * bit-twiddling functions.
 */

#ifndef RAMCLOUD_BITOPS_H
#define RAMCLOUD_BITOPS_H

#include "Common.h"

namespace RAMCloud {

/**
 * An exception that is thrown when BitOps methods exceed type sizes
 * or would produce undefined or nonsensical results.
 */
struct BitOpsException : public Exception {
    BitOpsException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
};

class BitOps {
  PUBLIC:
    /**
     * Determine if the integer provided is a power of two or not.
     * 
     * \param n
     *      The number to test.
     * \return
     *      true if n is a power of 2, else false.
     */
    template<typename T>
    static bool
    isPowerOfTwo(T n)
    {
        return (n > 0 && (n & (n - 1)) == 0);
    }

    /**
     * Count the number of bits set in a word.
     *
     * \param n
     *      The number whose bits we're counting.
     *
     * \return
     *      The number of bits set.
     */
    template<typename T>
    static int
    countBitsSet(T n)
    {
        return countBitsSet_portable(n);
    }

    /**
     * Return the index of the first (least significant) bit set.
     *
     * \param n
     *      The integer we're searching.
     * \return
     *      The index of the first bit set, starting at 1. If no
     *      bits are set 0 is returned.
     */
    template<typename T>
    static int
    findFirstSet(T n)
    {
        static_assert(sizeof(T) <= 8, "type must be <= 64 bits");
        return ffsl(n);
    }

    /**
     * Return the index of the last (most significant) bit set.
     *
     * \param n
     *      The integer we're searching.
     * \return
     *      The index of the last bit set, starting at 1. If no
     *      bits are set 0 is returned.
     */
    template<typename T>
    static int
    findLastSet(T n)
    {
        static_assert(sizeof(T) <= 8, "type must be <= 64 bits");

        // flsl(3) doesn't appear to be in our distro (or glibc?), so just
        // use the x86_64 asm here. Note that bsr's destination operand is
        // undefined when given 0 as input (though it does set ZF). Just
        // check for zero first.
        if (n == 0)
            return 0;

        uint64_t index = 0;
        uint64_t input = n;
        __asm__("bsrq %1,%0" : "=r"(index) : "r"(input) : "cc");
        return downCast<int>(index + 1);
    }

    /**
     * Round up a given integer to the next power of two, or return it
     * if it's already a power of two. Complexity is constant time due
     * to the bsrq instruction in findLastSet().
     *
     * \param n
     *      The number we want to round up to the next power of two.
     * \return
     *      The smallest power of two integer greater than or equal to n.
     * \throw BitOpsException
     *      An exception is thrown if rounding up would exceed the size of
     *      the input type.
     */
    template<typename T>
    static T
    powerOfTwoGreaterOrEqual(T n)
    {
        static_assert(sizeof(T) <= 8, "type must be <= 64 bits");

        if (isPowerOfTwo(n))
            return n;

        // round up if possible
        int lastBitSet = findLastSet(static_cast<uint64_t>(n));
        if (lastBitSet >= static_cast<int>(8 * sizeof(T)))
            throw BitOpsException(HERE, "Rounding up exceeds size of type!");

        return (downCast<T>(1UL << lastBitSet));
    }

    /**
     * Round up a given integer to the next power of two, or return it
     * if it's already a power of two. Complexity is constant time due
     * to the bsrq instruction in findLastSet().
     *
     * \param n
     *      The number we want to round up to the next power of two.
     * \return
     *      The smallest power of two integer greater than or equal to n.
     * \throw BitOpsException
     *      An exception is thrown if rounding down is not possible (i.e. if
     *      the input was 0).
     */
    template<typename T>
    static T
    powerOfTwoLessOrEqual(T n)
    {
        static_assert(sizeof(T) <= 8, "type must be <= 64 bits");

        if (isPowerOfTwo(n))
            return n;

        // round down if possible
        if (n == 0)
            throw BitOpsException(HERE, "Cannot round down past zero!");

        return (downCast<T>(1UL << (findLastSet(static_cast<uint64_t>(n))-1)));
    }

  PRIVATE:
    /**
     * \copydetails countBitsSet
     *
     * SSE version for newer x86_64 chips.
     */
    template<typename T>
    static int
    countBitsSet_x86(T n)
    {
        int64_t result = 0;
        uint64_t input = n;
        __asm__("popcntq %0,%1" : "=r"(result) : "r"(input));
        return downCast<int>(result);
    }

    /**
     * \copydetails countBitsSet
     *
     * Naive portable approach. Insert more clever method for your caveman CPU.
     */
    template<typename T>
    static int
    countBitsSet_portable(T n)
    {
        int64_t result = 0;

        for (size_t i = 0; i < sizeof(n) * 8; i++) {
            if (n & (1ULL << i))
                result++;
        }

        return downCast<int>(result);
    }
};

} // namespace RAMCloud

#endif // RAMCLOUD_BITOPS_H
