/* Copyright (c) 2009 Stanford University
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

#ifndef RAMCLOUD_SHARED_OBJECT_H
#define RAMCLOUD_SHARED_OBJECT_H

#include <shared/common.h>

#include <inttypes.h>
#include <assert.h>
#include <stdio.h>

namespace RAMCloud {

enum storage_type {
    STORAGE_INVALID_TYPE = 0,
    STORAGE_CHUNK_HDR_TYPE = 0x0B1EC7B1750B1EC7ULL, // "OBJECT BITS OBJECT"
    ___STORAGE_TYPE_MAX___ = 0xFFFFFFFFFFFFFFFFULL
};

struct chunk_entry {
    uint64_t len;
    uint32_t index_id;   /* static_cast<uint32_t>(-1) for data */
    uint32_t index_type; /* static_cast<uint32_t>(-1) for data */
    char data[0];                       // Variable length, but contiguous

    chunk_entry *
    next() const {
        const char *this_ptr = reinterpret_cast<const char*>(this);
        char *next_ptr = const_cast<char*>(this_ptr + this->total_size());
        return reinterpret_cast<chunk_entry*>(next_ptr);
    }

    uint64_t
    total_size() const {
        return sizeof(*this) + this->len;
    }

    bool
    is_data() const {
        return this->index_id == static_cast<uint32_t>(-1) &&
               this->index_type == static_cast<uint32_t>(-1);
    }
};

struct chunk_hdr {
    // WARNING: The hashtable code (for the moment) assumes that the
    // object's key is the first 64 bits of the struct
    uint64_t key;
    uint64_t checksum;
    enum storage_type type;
    uint64_t entries_len;
    struct chunk_entry entries[0];
};

class ChunkIter {
  public:

    ChunkIter(chunk_hdr *hdr) : entry(NULL), hdr_(hdr) {
        if (hdr_->entries_len > 0) {
            entry = hdr_->entries;
        } else {
            entry = NULL;
        }
    }

    ChunkIter& operator++() {
        if (entry != NULL) {
            entry = entry->next();
            size_t moved = reinterpret_cast<char*>(entry) -
                reinterpret_cast<char*>(hdr_->entries);
            size_t total = static_cast<size_t>(hdr_->entries_len);
            if (moved == total) {
                entry = NULL;
            } else {
                assert(moved < total);
            }
        }
        return *this;
    }

    chunk_entry *entry;

  private:
    chunk_hdr *hdr_;
    DISALLOW_COPY_AND_ASSIGN(ChunkIter);
};

} // namespace RAMCloud

#endif
