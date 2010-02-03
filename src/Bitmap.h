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

#include <shared/common.h>

#include <cstring>

#ifndef RAMCLOUD_BITMAP_H
#define RAMCLOUD_BITMAP_H

// size in bits
template <int64_t size>
class Bitmap {
  public:
    static uint32_t words() {
        return (size / 64) + 1;
    }
    explicit Bitmap(bool set) {
        memset(&bitmap[0], set ? 0xff : 0x00, size / 8);
    }
    void setAll() {
        memset(&bitmap[0], 0xff, size / 8);
    }
    void clearAll() {
        memset(&bitmap[0], 0x00, size / 8);
    }
    void set(int64_t num) {
        bitmap[num / 64] |= (1lu << (num % 64));
    }
    void clear(int64_t num) {
        bitmap[num / 64] &= ~(1lu << (num % 64));
    }
    bool get(int64_t num) {
        return (bitmap[num / 64] & (1lu << (num % 64))) != 0;
    }
    int64_t nextSet(int64_t start) {
        // TODO(stutsman) start ignored for now
        int r;
        for (int i = 0; i < size; i += 64) {
            r = ffsl(bitmap[i / 64]);
            if (r) {
                r = (r - 1) + i;
                if (r >= size)
                    return -1;
                return r;
            }
        }
        return -1;
    }
    void debugDump() {
        debug_dump64(&bitmap[0], size / 8);
    }
  private:
    // TODO(stutsman) ensure size is a power of 2
    uint64_t bitmap[size / 64 + 1];
    DISALLOW_COPY_AND_ASSIGN(Bitmap);
};

#endif
