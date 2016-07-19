/* Copyright (c) 2010,2011 Stanford University
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

#ifndef RAMCLOUD_OBJECTPOOL_H
#define RAMCLOUD_OBJECTPOOL_H

#include "Common.h"
#include "Memory.h"

/*
 * Notes on performance and efficiency:
 *
 * This class is currently extremely simple and has some notable
 * inefficiencies. It does not allocate in blocks of objects, so
 * each allocation that cannot be satisfied by the space of a
 * previously destroyed object results in a malloc. Furthermore,
 * the individual mallocs can cause poor cache locality when small
 * objects are used.
 *
 * Compared to the former boost-based ObjectPool, for very small
 * objects a fast allocation is about 70% slower (6.2ns vs 3.6ns;
 * due to cache effects) for 4-byte objects. For 64-byte objects
 * they're about the same. Slow allocation (no pool memory left)
 * is considerably slower.
 */

namespace RAMCloud {

struct ObjectPoolException : public Exception {
    ObjectPoolException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
};

/**
 * ObjectPool is a simple templated allocator that provides fast
 * allocation in cases where objects are frequently constructed and
 * then deleted. It initially allocates memory from malloc, but when
 * the pool is given an object to destroy it caches the backing
 * memory for more efficient future allocations.
 *
 * Use ObjectPool in cases where you want to be able to repeatedly
 * new and delete an relatively fixed set of objects very quickly.
 * For example, transports use ObjectPool to allocate short-lived rpc
 * objects that cannot be kept in a stack context.
 */
template <typename T>
class ObjectPool
{
  public:
    /**
     * Construct a new ObjectPool. The pool begins life with no allocated
     * memory. Objects are first allocated individually via malloc. Destroyed
     * objects have their backing memory stashed away to speed up future
     * allocations. For simplicity, no bulk allocations are performed.
     */
    ObjectPool()
        : outstandingObjects(0),
          pool()
    {
    }

    /**
     * Destroy the ObjectPool. The pool expects that all objects allocated
     * from it have already been destroyed and will not go about cleaning
     * up the caller's mess.
     */
    ~ObjectPool()
    {
        foreach(void* p, pool) {
            free(p);
        }

        // Catching this isn't intended, but could be done if the caller really
        // wants to, so make sure we free the pooled memory first.
        if (outstandingObjects > 0) {
            RAMCLOUD_LOG(ERROR,
                    "Pool destroyed with %lu objects still outstanding!",
                    outstandingObjects);
        }
    }

    /**
     * Construct a new object of templated type T. This method allocates memory
     * from the pool if possible. If the pool is empty, it mallocs more space.
     * If malloc fails, the process is terminated.
     *
     * \param args
     *      Arguments to provide to T's constructor.
     * \throw
     *      An exception is thrown if T's constructor throws.
     */
    template<typename... Args>
    T*
    construct(Args&&... args)
    {
        void* backing = NULL;
        if (pool.size() == 0) {
            backing = Memory::xmalloc(HERE, sizeof(T));
        } else {
            backing = pool.back();
            pool.pop_back();
        }

        T* object = NULL;
        try {
            object = new(backing) T(static_cast<Args&&>(args)...);
        } catch (...) {
            pool.push_back(backing);
            throw;
        }

        outstandingObjects++;
        return object;
    }

    /**
     * Destroy an object previously allocated by this pool.
     */
    void
    destroy(T* object)
    {
        assert(outstandingObjects > 0);
        object->~T();
        pool.push_back(static_cast<void*>(object));
        outstandingObjects--;
    }

  PRIVATE:
    /// Count of the number of objects for which construct() was called, but
    /// destroy() was not.
    uint64_t outstandingObjects;

    /// Pool of backing memory from previously destroyed objects.
    vector<void*> pool;
};

} // end RAMCloud

#endif  // RAMCLOUD_OBJECTPOOL_H
