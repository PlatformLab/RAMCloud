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

#ifndef RAMCLOUD_CYCLECOUNTER_H
#define RAMCLOUD_CYCLECOUNTER_H

#include "Common.h"

namespace RAMCloud {

/**
 * An object that keeps track of the elapsed number of cycles since its
 * declaration.
 */
class CycleCounter {
  public:
#if PERF_COUNTERS
    explicit CycleCounter();
    explicit CycleCounter(uint64_t* total);
    ~CycleCounter();
    void cancel();
    uint64_t stop();
  private:
    uint64_t* total;
    uint64_t startTime;
#else
    explicit CycleCounter() {}
    explicit CycleCounter(uint64_t* total) {}
    void cancel() {}
    uint64_t stop() { return 0; }
#endif
  private:
    DISALLOW_COPY_AND_ASSIGN(CycleCounter);
};

} // namespace RAMCloud

#endif
