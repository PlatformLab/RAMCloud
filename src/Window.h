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

#ifndef RAMCLOUD_WINDOW_H
#define RAMCLOUD_WINDOW_H

#include "Common.h"

namespace RAMCloud {

/**
 * A Window provides a mechanism for storing and accessing a small contiguous
 * range ("window") within a much larger logical array.  The window is fixed
 * in size, but can slide forward down the length of the array; only the
 * elements in the window are stored. The index of the first element in the
 * window is called the window's "offset"; the offset changes when #advance
 * is invoked.
 * \tparam T
 *      The type of the elements in the array. The elements will be initialized
 *      by setting the raw memory to 0, so hopefully that's meaningful to your
 *      type.
 *  \tparam length
 *      The number of elements in the window.
 */
template<typename T, uint32_t length>
class Window {
  public:
    /**
     * Construct a Window object. Initially the offset of the window
     * will be 0 (i.e., the window will store elements 0..length-1)
     */
    Window() : offset(0), translation(0) {
        reset();
    }

    /**
     * Restore the window to its state just after construction:
     * window offset will be 0, all elements in the window will be
     * cleared to 0.
     */
    void reset() {
        offset = translation = 0;
        memset(array, 0, sizeof(array));
    }

    /**
     * Access an element in the window.
     * \param index
     *      The offset into the virtual array of the desired element.
     *      The caller must ensure that this element is in the window.
     * \return
     *      The element indicated by index.
     */
    T& operator[](uint32_t index) {
        assert((index >= offset) && (index < offset+length));
        index += translation;
        if (index >= length)
            index -= length;
        return array[index];
    }

    /**
     * Increase the offset of the window, so that the window now
     * contains later elements in the virtual array.
     * \param distance
     *      The number of elements by which to advance the window.
     */
    void advance(uint32_t distance = 1) {
        uint32_t index = offset + translation;
        while (distance > 0) {
            memset(&(array[index]), 0, sizeof(T));
            index++;
            offset++;
            distance--;
            if (index == length) {
                // We just wrapped around in the ring buffer: the first
                // element of the old window was the last element in the ring,
                // so the first element of the new window is now the first
                // element of the ring; because of this we need to adjust
                // translation.
                index = 0;
                translation -= length;
            }
        }
    }

    /**
     * Return the total number of elements that can be stored in this
     * Window at once.
     */
    uint32_t getLength() {
        return length;
    }

    /**
     * Return the current offset for this Window.
     */
    uint32_t getOffset() {
        return offset;
    }

  PRIVATE:
    uint32_t offset;              //!< Index in the virtual array of the first
                                  //!< element in the window.

    int32_t translation;          //!< Add this value to offset to produce
                                  //!< the index in the array of the first
                                  //!< element in the window (i.e.,
                                  //!< offset%length - offset).

    T array[length];              //!< Storage for the elements currently in
                                  //!< the window. This array is treated like
                                  //!< a ring buffer in order to avoid copying
                                  //!< data when the window advances: element
                                  //!< i of this array will hold virtual array
                                  //!< elements i, i+length, i+2*length, etc.

    DISALLOW_COPY_AND_ASSIGN(Window);
};

}  // namespace RAMCloud

#endif // RAMCLOUD_WINDOW_H
