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

/**
 * \file
 * Header file for the Buffer class.
 *
 * The Buffer class is an mbuf-type data structure which contains pointers to a
 * list of memory locations, which are logically viewed as one buffer. The class
 * contains functions to read and manipulate this logical buffer.
 */

#ifndef RAMCLOUD_BUFFER_H
#define RAMCLOUD_BUFFER_H

#include <Common.h>

#ifdef __cplusplus
namespace RAMCloud {

class Buffer {
  public:
    bool prepend (void* buf, size_t size);
    
    bool append (void* buf, size_t size);

    size_t read (off_t offset, size_t length, void** return_ptr);

    size_t copy (off_t offset, size_t length, void* dest);

    size_t overwrite(void *buf, size_t length, off_t offset);

    /**
     * Function to get the entire length of this Buffer object.
     *
     * \return Returns the total length of the logical Buffer represented by
     *         this object.
     */
    size_t totalLength() const { return total_len; }

    Buffer() : curr_chunk(-1), num_chunks(INITIAL_CHUNK_ARR_SIZE),
            chunk_arr(NULL), total_len(0) {
        chunk_arr = (chunk*) xmalloc(sizeof(chunk) * INITIAL_CHUNK_ARR_SIZE);
    }

  private:
    struct chunk {
        void *ptr;
        size_t size;
    };

    // The initial size of the chunk array (see below). 10 should cover the vast
    // majority of Buffers. If not, we can increate this later.
    enum { INITIAL_CHUNK_ARR_SIZE = 10 };

    // The pointers (and lengths) of various chunks represented by this
    // Buffer. Initially, we allocate INITIAL_CHUNK_ARR_SIZE of the above
    // chunks, since this would be faster than using a vector<chunk>.

    int curr_chunk;  // The index of the last chunk thats currently being used.
    int num_chunks;  // The total number of chunks that have been allocated so far.
    chunk* chunk_arr;  // The pointers and lengths of various chunks represented
                       // by this Buffer. Initially, we allocate
                       // INITIAL_CHUNK_ARR_SIZE of the above chunks, since this
                       // would be faster than using a vector<chunk>.
    size_t total_len;  // The total length of all the memory blocks represented
                       // by this Buffer.

    friend class BufferTest;
};

}  // namespace RAMCloud
#endif  // __cplusplus

#endif  // RAMCLOUD_BUFFERPTR_H
