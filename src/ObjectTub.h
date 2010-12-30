/* Copyright (c) 2010 Stanford University
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

#ifndef RAMCLOUD_OBJECTTUB_H
#define RAMCLOUD_OBJECTTUB_H

#include "Common.h"

namespace RAMCloud {

/**
 * An ObjectTub holds an object that may be uninitialized; it allows the
 * allocation of memory for objects to be separated from its construction
 * and destruction. When you initially create an ObjectTub its object
 * is uninitialized (and should not be used). You can call #construct and
 * #destroy to invoke the constructor and destructor of the embedded object,
 * and #get or -> will return the embedded object. The embedded object
 * is automatically destroyed when the ObjectTub is destroyed (if it was
 * ever constructed in the first place).
 *
 * ObjectTubs are useful in situations like the following:
 * - You want to create an array of objects, but the objects need
 *   complex constructors with multiple arguments.
 * - You want to create a collection of objects, only some of which
 *   will be used, and you don't want to pay the cost of constructing
 *   objects that will never be used.
 * - You want automatic destruction of an object but don't want to
 *   heap-allocate the object (as with std::auto_ptr).
 *
 * Another way of thinking about this class is as a special case of the
 * boost::object_pool interface that can allocate at most 1 element at a time.
 * Destroy and free methods that take no arguments are provided since there
 * is no ambiguity as to which object is meant.
 *
 * It can also serve as a more efficient implementation of boost::scoped_ptr
 * for certain uses.
 *
 * ObjectTub is CopyConstructible if and only if ElementType is
 * CopyConstructible, and
 * ObjectTub is Assignable if and only if ElementType is Assignable.
 */
template<typename ElementType>
class ObjectTub {
  public:
    typedef ElementType element_type;

    ObjectTub()
        : raw()
        , occupied(false)
    {}

    ObjectTub(const ObjectTub<ElementType>& other) // NOLINT
        : raw()
        , occupied(false)
    {
        if (other.occupied)
            construct(*other.object); // use ElementType's copy constructor
    }

    ~ObjectTub() {
        if (occupied)
            destroy();
    }

    ObjectTub<ElementType>&
    operator=(const ObjectTub<ElementType>& other) {
        if (this != &other) {
            if (occupied)
                destroy();
            occupied = false;
            if (other.occupied) {
                *object = *other.object; // use ElementType's assignment
                occupied = true;
            }
        }
        return *this;
    }

    // methods like a boost::object_pool

    ElementType*
    malloc() {
        if (occupied)
            return NULL;
        occupied = true;
        return object;
    }

    void
    free(ElementType* p) {
        assert(p == object);
        free();
    }

    /// Same as free(get())
    void
    free() {
        assert(occupied);
        occupied = false;
    }


    template<typename... Args>
    ElementType*
    construct(Args&&... args) {
        if (occupied)
            return NULL;
        new(object) ElementType(static_cast<Args&&>(args)...);
        occupied = true;
        return object;
    }

    void
    destroy(ElementType* p) {
        assert(p == object);
        destroy();
    }

    /// Same as destroy(get())
    void
    destroy() {
        assert(occupied);
        object->~ElementType();
        occupied = false;
    }

    bool
    is_from(ElementType* p) const {
        return (p == object);
    }

    // methods like a boost::scoped_ptr

    template<typename... Args>
    ElementType*
    reset(Args&&... args) {
        if (occupied)
            destroy();
        return construct(static_cast<Args&&>(args)...);
    }

    const ElementType&
    operator*() const {
        return *get();
    }

    ElementType&
    operator*() {
        return *get();
    }

    const ElementType*
    operator->() const {
        return get();
    }

    ElementType*
    operator->() {
        return get();
    }

    ElementType*
    get() {
        if (!occupied)
            return NULL;
        else
            return object;
    }

    const ElementType*
    get() const {
        if (!occupied)
            return NULL;
        else
            return object;
    }

    operator bool() const {
        return occupied;
    }

  private:
    ElementType object[0];
    char raw[sizeof(ElementType)];
    bool occupied;
};

} // end RAMCloud

#endif  // RAMCLOUD_OBJECTTUB_H
