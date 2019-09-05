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

#include <sys/file.h>
#include <sys/mman.h>
#include <climits>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Weffc++"
#include <boost/type_traits.hpp>
#include <boost/utility/enable_if.hpp>
#pragma GCC diagnostic pop

#include "Common.h"

namespace RAMCloud {

/**
 * We'd like to share class state across all instances of all templated
 * versions of #LargeBlockOfMemory. However, due to templating, using
 * static members won't work (LargeBlockOfMemory<x> is a different type
 * from LargeBlockOfMemory<y> and doesn't share static members). Thus
 * we hack around this with a simple namespace.
 */
namespace LargeBlockOfMemoryInternal {
    extern uint64_t nextProbeBase;
}

/**
 * A wrapper for a large block of memory. Returned memory is guaranteed to be
 * at least one gigabyte aligned (at least the first 30 address bits will be 0).
 * \tparam T
 *      The type of object that will be stored in this block of memory.
 */
template<typename T = void>
struct LargeBlockOfMemory {
    /**
     * Allocates anonymous backing pages for a block of memory, pins them,
     * and zeros them. The memory is aligned to a gigabyte boundary.
     * \param length
     *      The number of bytes of memory to allocate.
     * \param hugepage
     *      True if we are allowed to use hugepage memory.
     * \throw FatalError
     *      If the memory could not be allocated.
     */
    explicit LargeBlockOfMemory(size_t length, bool hugepage = false)
        : length(length)
        , block()
    {
        int flags = MAP_ANONYMOUS;
        if (hugepage) {
            flags |= MAP_HUGETLB;
        }
        block = static_cast<T*>(mmapGigabyteAligned(length, flags));
        if (block == MAP_FAILED) {
            if (length == 0)
                return;
            throw FatalError(HERE,
                             format("Could not allocate %lu bytes", length),
                             errno);
        }
    }

    /**
     * Creates a file of the desired length and mmaps pages from it, pins them,
     * and zeros them. This is intended to be used with hugetlbfs to get
     * superpage-backed memory. Memory is aligned to a gigabyte boundary.
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

        block = reinterpret_cast<T*>(mmapGigabyteAligned(length, 0, fd));
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

    /// Just for convenience.
    static const uint64_t GIGABYTE = (uint64_t)1 << 30;

    /**
     * A page-aligned block of #length bytes of data.
     * May be NULL if length is 0.
     */
    T* block;

  private:
    /**
     * Mmap the desired amount of space with gigabyte alignment (lower 30
     * bits of the address are 0). Also, ensure that all mappings are faulted
     * in by touching each page. This is used to give the log memory that's
     * well-aligned, which makes things like computing base addresses of
     * Segments from random pointers really easy if Segments are aligned as
     * well (to a power-of-two less than or equal to 1GB).
     * 
     * One gigabyte alignment should be enough for anybody. Come find me in 30
     * years and tell me how foolishly shortsighted I was.
     *
     * \param[in] length
     *      Length of the memory area to be mapped in bytes.
     * \param[in] extraFlags
     *      Extra flags to be passed to mmap(2).
     * \param[in] fd
     *      Optional file descriptor (if mmaping a file, for instance).
     */
    void*
    mmapGigabyteAligned(size_t length, int extraFlags, int fd = -1)
    {
        const int maxTries = 10000;
        int i;

        uint64_t tryBase = LargeBlockOfMemoryInternal::nextProbeBase;
        for (i = 0; i < maxTries; i++) {
            void *base = mmap(reinterpret_cast<void*>(tryBase),
                              length,
                              PROT_READ | PROT_WRITE,
                              MAP_SHARED | extraFlags,
                              fd,
                              0);

            if (base == reinterpret_cast<void*>(tryBase))
                break;

            if (base != MAP_FAILED) {
                if (munmap(base, length)) {
                    RAMCLOUD_LOG(ERROR, "couldn't munmap undesirable mapping!");
                    return MAP_FAILED;
                }
            }

            tryBase += GIGABYTE;
        }

        if (i == maxTries) {
            RAMCLOUD_LOG(ERROR, "Couldn't mmap gigabyte-aligned region");
            return MAP_FAILED;
        }

        void* block = reinterpret_cast<void*>(tryBase);

        // Do not pin and fault in pages if we're testing, since that just
        // slows things down considerably (we usually don't touch anywhere near
        // all of the memory we allocate).
#if !TESTING
#ifdef MLOCK_PAGES
        // Pin the pages. Don't do this with the mmap() MAP_LOCKED flag since
        // that slows down probing considerably (Linux might be locking down
        // pages before it knows that it can actually give us the entire
        // range?).
        if (mlock(block, length)) {
            munmap(block, length);
            RAMCLOUD_LOG(ERROR, "Couldn't pin down the memory!");
            return MAP_FAILED;
        }
#endif

        // Force the OS to populate backing pages.  MAP_POPULATE doesn't seem
        // to do the trick and using it makes polling mmap for aligned base
        // addresses much slower.
        uint64_t pageSize = sysconf(_SC_PAGESIZE);
        for (uint64_t i = 0; i < length; i += pageSize) {
            reinterpret_cast<uint8_t*>(block)[i] = 0;
            if (!(i & ((1 << 30) - 1))) {
                RAMCLOUD_LOG(NOTICE, "Populating pages; progress %lu of %lu MB",
                             i / (1 << 20), length / (1 << 20));
            }
        }
#endif // !TESTING

        // Cache last mapped address to avoid re-probing same addresses later.
        LargeBlockOfMemoryInternal::nextProbeBase =
            (tryBase + length + GIGABYTE - 1) & ~(GIGABYTE - 1);

        return block;
    }

    DISALLOW_COPY_AND_ASSIGN(LargeBlockOfMemory);
};

} // end RAMCloud

#endif  // RAMCLOUD_LARGEBLOCKOFMEMORY_H
