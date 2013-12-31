/* Copyright (c) 2013 Stanford University
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

#ifndef RAMCLOUD_UNLOCK_H
#define RAMCLOUD_UNLOCK_H

namespace RAMCloud {

/**
 * This class is used to temporarily release lock in a safe fashion. Creating
 * an object of this class will unlock its associated mutex; when the object
 * is deleted, the mutex will be locked again. The template class T must be
 * a mutex-like class that supports lock and unlock operations.
 */
template<typename MutexType>
class Unlock {
  PUBLIC:
    explicit Unlock(MutexType& mutex) : mutex(mutex)
    {
        mutex.unlock();
    }
    ~Unlock() {
        mutex.lock();
    }

  PRIVATE:
    MutexType& mutex;
    DISALLOW_COPY_AND_ASSIGN(Unlock);
};

} // namespace RAMCloud

#endif // RAMCLOUD_UNLOCK_H

