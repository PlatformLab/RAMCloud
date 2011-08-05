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

#ifndef RAMCLOUD_ATOMICINT_H
#define RAMCLOUD_ATOMICINT_H

namespace RAMCloud {

/**
 * This class implements integers with atomic operations that are safe
 * for inter-thread synchronization.  Note: these operations do not deal
 * with instruction reordering issues; they simply provide basic atomic
 * reading and writing.   Proper synchronization also requires the use of
 * facilities such as those provided by the \c Fence class.
 *
 * As of 6/2011 this class is significantly faster than the C++ atomic_int
 * class, because the C++ facilities incorporate expensive fence operations.
 */
class AtomicInt {
  public:
    /**
     * Construct an AtomicInt.
     *
     * \param value
     *      Initial value for the integer.
     */
    explicit AtomicInt(int value = 0) : value(value) { }

    /**
     * Atomically add to the value of the integer.
     *
     * \param increment
     *      How much to add to the value of the integer.
     */
    void add(int increment)
    {
        __asm__ __volatile__("lock; addl %1,%0" : "=m" (value) :
                "r" (increment));
    }

    /**
     * Atomically compare the value of the integer with a test value and,
     * if the values match, replace the value of the integer with a new
     * value.
     *
     * \param test
     *      Replace the value only if its current value equals this.
     * \param newValue
     *      This value will replace the current value of the integer.
     * \result
     *      The previous value of the integer.
     */
    int compareExchange(int test, int newValue)
    {
        __asm__ __volatile__("lock; cmpxchgl %0,%1" : "=r" (newValue),
                "=m" (value), "=a" (test) : "0" (newValue), "2" (test));
        return test;
    }

    /**
     * Atomically replace the value of the integer while returning its
     * old value.
     *
     * \param newValue
     *      This value will replace the current value of the integer.
     * \result
     *      The previous value of the integer.
     */
    int exchange(int newValue)
    {
        __asm__ __volatile__("xchg %0,%1" : "=r" (newValue), "=m" (value) :
                "0" (newValue));
        return newValue;
    }

    /**
     * Atomically increment the value of the integer.
     */
    void inc()
    {
        __asm__ __volatile__("lock; incl %0" : "=m" (value));
    }

    /**
     * Return the current value of the integer.
     */
    int load()
    {
        return value;
    }

    /**
     * Assign to an AtomicInt.
     *
     * \param newValue
     *      This value will replace the current value of the integer.
     * \return
     *      The new value.
     */
    AtomicInt& operator=(int newValue)
    {
        store(newValue);
        return *this;
    }

    /**
     * Return the current value of the integer.
     */
    operator int()
    {
        return load();
    }

    /**
     * Increment the current value of the integer.
     */
    AtomicInt& operator++()
    {
        inc();
        return *this;
    }
    AtomicInt operator++(int)              // NOLINT
    {
        inc();
        return *this;
    }

    /**
     * Decrement the current value of the integer.
     */
    AtomicInt& operator--()
    {
        add(-1);
        return *this;
    }
    AtomicInt operator--(int)              // NOLINT
    {
        add(-1);
        return *this;
    }

    /**
     * Set the value of the integer.
     *
     * \param newValue
     *      This value will replace the current value of the integer.
     */
    void store(int newValue)
    {
        value = newValue;
    }

  PRIVATE:
    // The integer value on which the atomic operations operate.
    volatile int value;
};

} // end RAMCloud

#endif  // RAMCLOUD_ATOMICINT_H
