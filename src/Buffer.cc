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
 * Contains the implementation for the Buffer class.
 */

#include <Buffer.h>

#include <string.h>

namespace RAMCloud {

/**
 * Prepends a new memory region to the Buffer. This new region is added to
 * the beginning of the chunk list instead of at the end (like in the append
 * function).
 *
 * TODO(aravindn): This is a really slow implementation. Fix this. Maybe use a
 * linked list representation for the chunk array?
 * 
 * \param[in] buf The memory region to be added.
 * \param[in] size The size of the memory region represented by buf.
 * \return Returns true or false depending on teh success of the prpepend op.
 */
bool Buffer::prepend (void* buf, size_t size) {
    if (size <= 0) return false;
    if (buf == NULL) return false;

    // Check max chunks
    if (curr_chunk + 1 == num_chunks) {
        // Resize the chunk array.
        chunk* new_arr = (chunk*) xmalloc(sizeof(chunk) * num_chunks * 2);
        memcpy(new_arr, chunk_arr, sizeof(chunk) * num_chunks);
        free(chunk_arr);
        chunk_arr = new_arr;
        num_chunks *= 2;
    }

    // Right shift the chunks.
    for (int i = curr_chunk+1; i > 0; --i) {
        chunk_arr[i].ptr = chunk_arr[i-1].ptr;
        chunk_arr[i].size = chunk_arr[i-1].size;
    }
    
    chunk* c = (chunk_arr);
    c->ptr = buf;
    c->size = size;
    ++curr_chunk;

    total_len += size;

    return true;
}

/**
 * Appends a new memory region to the Buffer.
 *
 * \param[in] buf The memory region to be added.
 * \param[in] size The size of the memory region represented by buf.
 * \return Returns true or false depending on the success of the append op.
 */
bool Buffer::append(void* buf, size_t size) {
    if (size <= 0) return false;
    if (buf == NULL) return false;

    // Check max chunks
    if (curr_chunk + 1 == num_chunks) {
        // Resize the chunk array.
        chunk* new_arr = (chunk*) xmalloc(sizeof(chunk) * num_chunks * 2);
        memcpy(new_arr, chunk_arr, sizeof(chunk) * num_chunks);
        free(chunk_arr);
        chunk_arr = new_arr;
        num_chunks *= 2;
    }

    ++curr_chunk;
    chunk* c = (chunk_arr + curr_chunk);
    c->ptr = buf;
    c->size = size;

    total_len += size;

    return true;
}

/**
 * Function to return the pointer to the buffer at the given offset. It does not
 * copy. The combination of the offset and length parameters represent a logical
 * block of memory that we return. If the block spans multiple chunks, we only
 * return the number of bytes upto the end of the first chunk.
 *
 * \param[in] offset The offset into the logical buffer represented by this
 *             Buffer
 * \param[in] length The length of the memory block to return.
 * \param[out] return_ptr The pointer to the memory block requested.
 * \return The number of bytes that region of memory pointed to by return_ptr
 *             has.
 */
size_t Buffer::read(off_t offset, size_t length, void** return_ptr) {
    if (offset < 0) return 0;
    if (length <= 0) return 0;

    off_t curr_off = 0;
    for (int i = 0; i <= curr_chunk; ++i) {
        if ((off_t) (curr_off + chunk_arr[i].size) > offset) {
            off_t chunk_off = offset - curr_off;
            size_t len = chunk_arr[i].size - chunk_off;

            *return_ptr = (void*) (((uint8_t *) chunk_arr[i].ptr) + chunk_off);
            return len;
        }

        curr_off += chunk_arr[i].size;
    }

    return 0;
}

/**
 * Copies the memory block identified by the <offset, length> tuple into the
 * region pointed to by dest. 
 *
 * If the <offset, length> memory block extends beyond the end of the logical
 * Buffer, we copy all the bytes from offset upto the end of the Buffer, and
 * returns the number of bytes copied. 
 *
 * \param[in] offset The offset at which the memory block we should copy starts.
 * \param[in] length The length of the memory block we should copy.
 * \param[in] dest The pointer to which we should copy the logical memory block.
 * \return The actual number of bytes copied.
 */
size_t Buffer::copy(off_t offset, size_t length, void* dest) {
    if (offset < 0) return 0;
    if (length <= 0) return 0;

    off_t curr_off = 0;  // Offset from the beginning of the Buffer.
    off_t chunk_off;  // Offset from the beginning of the current chunk.
    size_t curr_chunk_len;  // Number of bytes to copy from the current chunk,
                            // beginning at 'chunk_off'.
    size_t bytes_copied = 0;
    int curr_chunk_index;  // Index of the current chunk we are copying from.

    // Find the chunk which contains the offset byte.
    for (curr_chunk_index = 0; curr_chunk_index <= curr_chunk;
         ++curr_chunk_index) {
        if ((off_t) (curr_off + chunk_arr[curr_chunk_index].size) > offset)
            break;
        curr_off += chunk_arr[curr_chunk_index].size;
    }

    chunk_off = offset - curr_off;

    while (curr_off < (off_t) (offset + length) &&
           curr_chunk_index <= curr_chunk) {
        if (curr_off + chunk_arr[curr_chunk_index].size > offset + length)
            curr_chunk_len = length - bytes_copied;
        else
            curr_chunk_len = chunk_arr[curr_chunk_index].size - chunk_off;

        memcpy(dest, (uint8_t *) chunk_arr[curr_chunk_index].ptr + chunk_off,
               curr_chunk_len);
        bytes_copied += curr_chunk_len;
        dest = ((uint8_t*) dest) + curr_chunk_len;

        ++curr_chunk_index;
        curr_off += chunk_arr[curr_chunk_index].size;
        chunk_off = 0;
    }

    return bytes_copied;
}

/**
 * Overwrites data at 'offset' from the data in buf.
 *
 * \param[in] buf The data from which we copy into the Buffer.
 * \param[in] length The length of the data in buf.
 * \param[in] offset The offset in the Buffer at which to begin overwriting.
 * \return The number of bytes that were actually overwritten. This will be less
 *         than length if the region extends past the end of the logical Buffer.
 */
size_t Buffer::overwrite(void *buf, size_t length, off_t offset) {
    if (buf == NULL) return 0;
    if (length <= 0) return 0;

    off_t curr_off = 0;  // Offset from the beginning of the Buffer.
    off_t chunk_off;  // Offset from the beginning of the current chunk.
    size_t curr_chunk_len;  // Number of bytes to copy from the current chunk,
                            // beginning at 'chunk_off'.
    size_t bytes_copied = 0;
    int curr_chunk_index;  // Index of the current chunk we are copying from.

    // Find the chunk that contains the offset byte.
    for (curr_chunk_index = 0; curr_chunk_index <= curr_chunk;
         ++curr_chunk_index) {
        if ((off_t) (curr_off + chunk_arr[curr_chunk_index].size) > offset)
            break;
        curr_off += chunk_arr[curr_chunk_index].size;
    }

    chunk_off = offset - curr_off;

    while (bytes_copied < (off_t) (length) &&
           curr_chunk_index <= curr_chunk) {
        if (curr_off + chunk_arr[curr_chunk_index].size > offset + length)
            curr_chunk_len = length - bytes_copied;
        else
            curr_chunk_len = chunk_arr[curr_chunk_index].size - chunk_off;

        memcpy((uint8_t *) chunk_arr[curr_chunk_index].ptr + chunk_off,
               buf, curr_chunk_len);
        bytes_copied += curr_chunk_len;
        buf = ((uint8_t*) buf) + curr_chunk_len;

        ++curr_chunk_index;
        curr_off += chunk_arr[curr_chunk_index].size;
        chunk_off = 0;
    }

    return bytes_copied;
}

}  // namespace RAMCLoud
