/* Copyright (c) 2009-2010 Stanford University
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

// RAMCloud pragma [CPPLINT=0]

/**
 * \file
 * A dumping ground for some memory allocation-related stuff. The issue RAM-48
 * will eventually get rid of this.
 */

#ifndef RAMCLOUD_UGLY_MEMORY_STUFF_H
#define RAMCLOUD_UGLY_MEMORY_STUFF_H

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

// TODO(ongaro): The memory regioin allocated is impossible free since the
// pointer from xmalloc is discarded.
static void *
xmalloc_aligned_xmalloc(uint64_t len)
{
    uintptr_t p = (uintptr_t)xmalloc(len + 64);
    p += (64 - (p & 63));
    return ((void *)p);
}

static void *
xmalloc_aligned_hugetlb(uint64_t len)
{
    // TODO(stutsman) need to protect this state once we're threaded -
    // it might be best to yank it out
    static uintptr_t p = 0;
    static uint64_t alloced = 0;
    const size_t maxmem = 1 * 1024 * 1024 * 1024;

    if (p == 0) {
        unlink("/mnt/hugetlbshit");
        int fd = open("/mnt/hugetlbshit", O_CREAT | O_TRUNC | O_RDWR, 0600);
        if (fd == -1) {
            perror("open");
            exit(1);
        }
        p = (uintptr_t)mmap(0, maxmem, PROT_READ | PROT_WRITE,
                            MAP_SHARED, fd, 0);
        if ((void *)p == MAP_FAILED) {
            perror("mmap");
            exit(1);
        }
        memset(reinterpret_cast<void *>(p), 0, maxmem);
        printf("Allocated hugetlb region at %p\n", (void *)p);
    }

    len += (64 - (len & 63));
    if (alloced + len > maxmem) {
        printf("ERROR: allocated over hugetlb space!\n");
        exit(1);
    }
    alloced += len;

    uintptr_t r = p;
    p += len;

    return ((void *)r);
}

#endif
