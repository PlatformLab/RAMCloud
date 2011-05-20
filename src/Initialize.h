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

#ifndef RAMCLOUD_INITIALIZE_H
#define RAMCLOUD_INITIALIZE_H

namespace RAMCloud {

/**
 * This class is used to manage the initialization of static variables.
 * In normal use objects of this class are declared at a static level.
 * When an instance of the class is constructed it causes a particular
 * function to be invoked, or it causes a new object to be dynamically
 * allocated and assigned to a pointer. This makes it easier to control
 * the order of initialization (e.g., an initialization function can
 * initialize objects in a particular order, and explicitly invoke other
 * initialization functions to make sure dependent classes are properly
 * initialized).  The pointer form allows the creation of static objects
 * that are never destructed (thereby avoiding issues with the order of
 * destruction).
 */
class Initialize {
  public:
    /**
     * This form of constructor causes a function to be invoked when the
     * object is constructed.  Typically the function will create static
     * objects and/or invoke other initialization methods.  The function
     * should normally contain an internal guard so that it only performs
     * its initialization is the first time it is invoked.
     *
     * \param func
     *      This function is invoked with no arguments when the object is
     *      constructed.
     */
    Initialize(void (*func)()) {
        (*func)();
    }

    /**
     * This form of constructor causes a new object of a particular class
     * to be constructed with a no-argument constructor and assigned to a
     * given pointer.
     *
     * \param p
     *      Pointer to an object of any type. If the pointer is NULL then
     *      it is replaced with a pointer to a newly allocated object of
     *      the given type.
     */
    template<typename T>
    Initialize(T*& p) {
        if (p == NULL) {
            p = new T;
        }
    }
};

} // end RAMCloud

#endif  // RAMCLOUD_INITIALIZE_H
