/* Copyright (c) 2011-2015 Stanford University
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

#ifndef RAMCLOUD_THREADID_H
#define RAMCLOUD_THREADID_H

#include <mutex>

#include "Common.h"

namespace RAMCloud {

/**
 * This class provides a single static method #get, which returns a unique
 * identifier for the current thread. This class is implemented using the
 * gcc "__thread" storage class, which makes it much faster than other
 * mechanisms such as Boost thread-specific variables. For example, here
 * are some approximate times for various approaches, as of 6/2011:
 *
 * boost::this_thread::get_id():  75ns
 * read boost thread-specific variable: 27ns
 * read gcc __thread variable: < 2ns
 */
class ThreadId {
  public:
    static int get();

  PRIVATE:
    explicit ThreadId();
    static int assign();
    static __thread int id;
    static int highestId;
    static std::mutex mutex;

    DISALLOW_COPY_AND_ASSIGN(ThreadId);
};

} // end RAMCloud

#endif  // RAMCLOUD_THREADID_H
