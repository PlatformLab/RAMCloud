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

#ifndef RAMCLOUD_DATABLOCK_H
#define RAMCLOUD_DATABLOCK_H

#include <thread>
#include <mutex>
#include "Buffer.h"

namespace RAMCloud {

/**
 * This class provides synchronized access (atomic read and write) to an
 * arbitrary-length block of data. This class is thread-safe (concurrent
 * accesses are serialized).
 */
class DataBlock {
  PUBLIC:
    DataBlock();
    ~DataBlock();
    void set(const void* block, size_t length);
    void get(Buffer* output);

  PRIVATE:
    // Provides monitor-style synchronization for this object.
    std::mutex mutex;

    // Storage space for the block's contents (dynamically allocated). NULL
    // means no space has been allocated.
    void* block;

    // Total amount of space allocated at block.
    size_t length;

    DISALLOW_COPY_AND_ASSIGN(DataBlock);
};

} // namespace RAMCloud

#endif // RAMCLOUD_DATABLOCK_H

