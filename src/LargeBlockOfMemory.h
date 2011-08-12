/* Copyright (c) 2011 Stanford University
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

#ifndef RAMCLOUD_LARGEBLOCKOFMEMORY_H
#define RAMCLOUD_LARGEBLOCKOFMEMORY_H

#include <limits.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <boost/type_traits.hpp>
#include <boost/utility/enable_if.hpp>
#include "Common.h"

namespace RAMCloud {

/**
 * A wrapper for a large block of memory.
 * \tparam T
 *      The type of object that will be stored in this block of memory.
 */
template<typename T = void>
struct LargeBlockOfMemory {
    /**
     * Allocates anonymous backing pages for a block of memory, pins them,
     * and zeros them.
     * \param length
     *      The number of bytes of memory to allocate.
     * \throw FatalError
     *      If the memory could not be allocated.
     */
    explicit LargeBlockOfMemory(size_t length)
        : length(length)
        , block(static_cast<T*>(mmap(NULL, length, PROT_READ | PROT_WRITE,
                                     MAP_SHARED | MAP_ANONYMOUS |
                                     MAP_POPULATE, -1, 0)))
    {
        if (block == MAP_FAILED) {
            if (length == 0)
                return;
            throw FatalError(HERE,
                             format("Could not allocate %lu bytes", length),
                             errno);
        }
        // Force the OS to populate backing pages,
        // because MAP_POPULATE doesn't seem to do the trick.
        uint64_t pageSize = sysconf(_SC_PAGESIZE);
        for (uint64_t i = 0; i < length; i += pageSize)
            reinterpret_cast<uint8_t*>(block)[i] = 0;
    }

    /**
     * Creates a file of the desired length and mmaps pages from it, pins them,
     * and zeros them. This is intended to be used with hugetlbfs to get
     * superpage-backed memory.
     * \param filePath
     *      Path to the file to create and mmap. This file must not already
     *      exist.
     * \param length
     *      The number of bytes of memory to allocate.
     * \throw FatalError
     *      If the memory could not be allocated, i.e. if the file already
     *      existed, could not be created, could not be truncated, or could
     *      not be mmapped.
     */
    LargeBlockOfMemory(string filePath, size_t length)
        : length(length),
          block(NULL)
    {
        const char* path = filePath.c_str();

        // Open the file, being careful that it did not previously
        // exist, as we don't want to stomp on other processes.
        int fd = open(path, O_CREAT | O_EXCL | O_RDWR, 0600);
        if (fd == -1) {
            throw FatalError(HERE,
                format("Could not open file [%s]", path),
                errno);
        }

        // This isn't strictly necessary for hugetblfs, but it lets us
        // use this same code on any mmaped file.
        if (ftruncate(fd, length) != 0) {
            unlink(path);
            close(fd);
            throw FatalError(HERE,
                format("Could not ftruncate file [%s] to %lu bytes",
                  path, length),
                errno);
        }

        block = reinterpret_cast<T*>(mmap(NULL, length, PROT_READ | PROT_WRITE,
                                         MAP_SHARED | MAP_POPULATE,
                                         fd, 0));
        if (reinterpret_cast<void*>(block) == MAP_FAILED) {
            unlink(path);
            close(fd);
            throw FatalError(HERE,
                format("Could not mmap file [%s]", path),
                errno);
        }

        // Remove the file from the directory. Our memory will remain allocated,
        // however.
        unlink(path);
        close(fd);

        RAMCLOUD_LOG(NOTICE,
                     "Mmapped %lu-byte region from [%s] at %p\n",
                     length, path, reinterpret_cast<void*>(block));

        // Fault in each mapping.
        uint64_t pageSize = sysconf(_SC_PAGESIZE);
        for (uint64_t i = 0; i < length; i += pageSize)
            reinterpret_cast<uint8_t*>(block)[i] = 0;
    }

    ~LargeBlockOfMemory()
    {
        if (block != NULL && munmap(block, length) != 0)
            RAMCLOUD_LOG(WARNING, "munmap of large block failed with %d",
                         errno);
    }

    void swap(LargeBlockOfMemory<T>& other) {
        std::swap(this->length, other.length);
        std::swap(this->block, other.block);
    }

    /// Returns #block.
    T* operator*() { return block; }
    /// Returns #block.
    T* operator->() { return block; }
    /// Returns #block.
    T* get() { return block; }

    /// The number of bytes valid starting at #block.
    size_t length;

    /**
     * A page-aligned block of #length bytes of data.
     * May be NULL if length is 0.
     */
    T* block;

    DISALLOW_COPY_AND_ASSIGN(LargeBlockOfMemory);
};

} // end RAMCloud

#endif  // RAMCLOUD_LARGEBLOCKOFMEMORY_H
