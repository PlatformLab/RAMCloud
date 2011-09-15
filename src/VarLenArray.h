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

#ifndef RAMCLOUD_VARLENARRAY_H
#define RAMCLOUD_VARLENARRAY_H

#include "Common.h"

namespace RAMCloud {

/**
 * Manages an array of dynamic size in a convenient and exception-safe way.
 *
 * This class is only useful for reducing calls to malloc. The idea is to
 * combine two allocations into one, allocating space for an object O along
 * with space for a related array A. If the size of A is known at compile-time,
 * this can be easily accomplished by adding A as a member of O. This class
 * solves the problem when numElements is not known until run-time.
 *
 * To use this class, add an instance of this class as the last member of O. At
 * run-time, allocate space for sizeof(O) + sizeof(ElementType) * numElements.
 * Then, use placement new on that space to construct O. The constructor for O
 * should, in turn, construct the instance of this class, passing to it the
 * appropriate number of elements. Thereafter, this instance as a member of O
 * acts just like A would have.
 *
 * This class presents an interface that is intentionally very similar to
 * std::array ( see http://en.wikipedia.org/wiki/Array_%28C%2B%2B%29 ).
 * The key difference is that this class takes the size of the array as an
 * argument to the constructor at run-time, whereas the std::array class takes
 * it as a compile-time template parameter. Then, rather than dynamically
 * allocating its own memory for the element array, this class assumes that the
 * caller has allocated sizeof(ElementType) * numElements bytes of raw memory
 * immediately following the VarLenArray instance.
 */
template<typename ElementType, typename SizeType = uint32_t>
struct VarLenArray {

    typedef ElementType& reference;
    typedef const ElementType& const_reference;
    typedef ElementType* iterator;
    typedef const ElementType* const_iterator;
    typedef SizeType size_type;
    typedef ptrdiff_t difference_type;
    typedef ElementType value_type;
    typedef std::reverse_iterator<iterator> reverse_iterator;
    typedef std::reverse_iterator<const_iterator> const_reverse_iterator;

    /**
     * Constructor.
     * \param numElements
     *      The number of elements in the array. The caller must have allocated
     *      sizeof(ElementType) * numElements bytes of raw memory immediately
     *      following this instance.
     */
    explicit VarLenArray(size_type numElements)
        : numElements(numElements)
    {
        // Construct elements in order
        for (size_type i = 0; i < numElements; ++i) {
            try {
                new(&start[i]) ElementType();
            } catch (...) {
                // Destroy elements in reverse order
                for (size_type j = 0; j < i; ++j)
                    start[i - j - 1].~ElementType();
                throw;
            }
        }
    }

    ~VarLenArray() {
        // Destroy elements in reverse order
        for (size_type i = 0; i < numElements; ++i)
            start[numElements - i - 1].~ElementType();
    }

    iterator begin() {
        return start;
    }

    const_iterator begin() const {
        return start;
    }

    iterator end() {
        return start + numElements;
    }

    const_iterator end() const {
        return start + numElements;
    }

    reverse_iterator rbegin() {
      return reverse_iterator(end());
    }

    const_reverse_iterator rbegin() const {
        return const_reverse_iterator(end());
    }

    reverse_iterator rend() {
        return reverse_iterator(begin());
    }

    const_reverse_iterator rend() const {
        return const_reverse_iterator(begin());
    }

    const_iterator cbegin() const {
        return start;
    }

    const_iterator cend() const {
        return start + numElements;
    }

    const_reverse_iterator crbegin() const {
        return const_reverse_iterator(end());
    }

    const_reverse_iterator crend() const {
        return const_reverse_iterator(begin());
    }

    size_type size() const {
        return numElements;
    }

    size_type max_size() const {
        return size();
    }

    bool empty() const {
        return numElements == 0;
    }

    reference operator[](size_type i) {
        return start[i];
    }

    const_reference operator[](size_type i) const {
        return start[i];
    }

    reference at(size_type i) {
        if (i >= numElements)
            throw std::out_of_range("VarLenArray::at");
        return start[i];
    }

    const_reference at(size_type i) const {
        if (i >= numElements)
            throw std::out_of_range("VarLenArray::at");
        return start[i];
    }

    reference front() {
        return *begin();
    }

    const_reference front() const {
        return *begin();
    }

    reference back() {
        return *(end() - 1);
    }

    const_reference back() const {
        return *(end() - 1);
    }

    ElementType* data() {
        return start;
    }

    const ElementType* data() const {
        return start;
    }

    /**
     * The number of elements in the array, as passed to the constructor.
     */
    const size_type numElements;

    /**
     * The start of the array. This makes the method implementations quite
     * succinct, since they do not need to use pointer arithmetic or casts.
     * More importantly, it makes sure the elements are properly aligned
     * (gcc will add the necessary structure padding before this member).
     */
    ElementType start[0];
};

} // namespace RAMCloud

#endif  // RAMCLOUD_VARLENARRAY_H
