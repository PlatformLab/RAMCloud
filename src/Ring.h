/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file
 * Implementation of #RAMCloud::Ring.
 */

#ifndef RAMCLOUD_RING_H
#define RAMCLOUD_RING_H

#include <Common.h>


namespace RAMCloud {

/**
 * A Ring provides a fixed-size sliding window into an array.
 * \tparam T
 *      The type of the elements in the array. The elements will be initialized
 *      by setting the raw memory to 0, so hopefully that's meaningful to your
 *      type.
 *  \tparam length
 *      The number of elements in the window.
 */
template<typename T, uint32_t length>
class Ring {
  public:
    Ring() : start(0) {
        clear();
    }

    /**
     * Reset the elements in the window.
     */
    void clear() {
        memset(array, 0, sizeof(array));
    }

    /**
     * Access an element in the window.
     * \param[in] index
     *      The offset into the window of the desired element.
     * \return
     *      The element at the given offset in the window.
     */
    T& operator[](uint32_t index) {
        assert(index < length);
        index += start;
        if (index >= length)
            index -= length;
        return array[index];
    }

    /**
     * Advance the window.
     * \param[in] distance
     *      The number of elements by which to advance the window.
     */
    void advance(uint32_t distance) {
        // TODO(ongaro): This loop could be optimized into some math and 2
        // memset calls.
        for (uint32_t i = 0; i < std::min(distance, length); i++)
            memset(&((*this)[i]), 0, sizeof(T));

        start += distance;
        // TODO(ongaro): assert(distance <= length) and change to if?
        while (start >= length)
            start -= length;
    }

  private:
    /**
     * See #array.
     */
    uint32_t start;

    /**
     * The window is logically
     * the last (\a length - #start) elements of \a array plus
     * the first #start elements of \a array.
     */
    T array[length];

    friend class RingTest;
    DISALLOW_COPY_AND_ASSIGN(Ring);
};

}  // namespace RAMCloud

#endif
