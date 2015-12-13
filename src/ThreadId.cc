/* Copyright (c) 2011-2015 Stanford University
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

#include "Common.h"
#include "ThreadId.h"

namespace RAMCloud {

// Thread-specific data holds the identifier for each thread.  It starts
// off zero, but is set to a non-zero unique value the first time it is
// accessed.
__thread int ThreadId::id = 0;

// The highest-numbered thread identifier that has already been used.
int ThreadId::highestId = 0;

/// Used to serialize access to #highestId.
std::mutex ThreadId::mutex;

/**
 * Pick a unique value to use as the thread identifier for the current
 * thread. This value is saved in the thread-specific variable #Thread::id.
 *
 * \return
 *      The unique identifier for the current thread.
 */
int
ThreadId::assign()
{
    std::unique_lock<std::mutex> lock(mutex);
    if (id == 0) {
        highestId++;
        id = highestId;
    }
    return id;
}

/**
 * Return a unique identifier associated with this thread.  The
 * return value has two properties:
 * - It will never be zero.
 * - It will be unique for this thread (i.e., no other thread has ever
 *   been returned this value or ever will be returned this value)
 */
int
ThreadId::get()
{
    if (id != 0) {
        return id;
    }
    return assign();
}

} // namespace RAMCloud
