#include <server/hashtable.h>

#include <cstdio>
#include <cstdlib>
#include <stdint.h>
#include <cmath>
#include <cstring>

#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <inttypes.h>

namespace RAMCloud {

#define UNUSED                  ((uint64_t)0)
#define ADDR(x)                 (x & 0xfffffffffffeULL)
#define GETMINIKEY(x)           (x >> 48)
#define MKENTRY(mk, p)          (((uint64_t)mk << 48) | ADDR((uint64_t)p))
#define ISCHAIN(c,x)            (c->keys[x] & 0x1)
#define GETCHAINPTR(c,x)        (c->keys[x] & ~0x1)
#define MKCHAINPTR(x)           ((uint64_t)x | 0x1)

void
Hashtable::StoreSample(uint64_t ticks)
{
    if (ticks / 10 < NBUCKETS)
        buckets[ticks / 10]++;
    else
        oflowbucket++;

    if (ticks < min_ticks)
        min_ticks = ticks;
    if (ticks > max_ticks)
        max_ticks = ticks;
}

static void *
malloc_aligned_malloc(uint64_t len)
{
    uintptr_t p = (uintptr_t)malloc(len + 64);
    p += (64 - (p & 63));
    return ((void *)p);
}

static void *
malloc_aligned_hugetlb(uint64_t len)
{
    // TODO(stutsman) need to protect this state once we're threaded -
    // it might be best to yank it out
    static uintptr_t p = 0;
    static uint64_t alloced = 0;
    const size_t maxmem = 1 * 1024 * 1024 * 1024;

    if (p == 0) {
        unlink("/mnt/hugetlbshit");
        int fd = open("/mnt/hugetlbshit", O_CREAT | O_TRUNC | O_RDWR, 600);
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

void *
Hashtable::MallocAligned(uint64_t len)
{
    return (use_huge_tlb) ?
        malloc_aligned_hugetlb(len) : malloc_aligned_malloc(len);
}

static inline void
hash(uint64_t key, uint64_t *hash, uint16_t *mkhash)
{
    key = (~key) + (key << 21); // key = (key << 21) - key - 1;
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8); // key * 265
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4); // key * 21
    key = key ^ (key >> 28);
    key = key + (key << 31);

    *hash   = key & 0x0000ffffffffffffULL;
    *mkhash = static_cast<uint16_t>(key >> 48);
}

void
Hashtable::InitTable(uint64_t lines)
{
    uint64_t i, j;

    table_lines = lines;
    table = static_cast<cacheline *>(MallocAligned(table_lines *
                                                   sizeof(table[0])));

    for (i = 0; i < table_lines; i++) {
        for (j = 0; j < 8; j++)
            table[i].keys[j] = UNUSED;
    }
}

uint64_t *
Hashtable::LookupKeyPtr(uint64_t key)
{
    uint64_t b = rdtsc();
    uint64_t h;
    uint16_t mk;
    int i;

    hash(key, &h, &mk);
    cacheline *cl = &table[h % table_lines];

    while (1) {
        uint64_t *kp = cl->keys;
        for (i = 0; i < 8; i++, kp++) {
            if (i == 8 && !ISCHAIN(cl, 7))
                break;

            if (*kp != UNUSED && GETMINIKEY(*kp) == mk) {
                // could be it. assume object stores key in first 64 bits
                uint64_t *obj = (uint64_t *)ADDR(*kp);
                if (*obj == key) {
                    uint64_t diff = rdtsc() - b;
                    lup_total += diff;
                    StoreSample(diff);
                    return kp;
                } else {
                    lup_mkfails++;
                }
            }
        }

        // not found, try chaining
        if (!ISCHAIN(cl, 7)) {
            uint64_t diff = rdtsc() - b;
            lup_total += diff;
            StoreSample(diff);
            return NULL;
        }

        cl = (cacheline *)GETCHAINPTR(cl, 7);
        lup_nexts++;
    }
}

void *
Hashtable::Lookup(uint64_t key)
{
    uint64_t *kp = LookupKeyPtr(key);
    return kp ? (void *) ADDR(*kp) : NULL;
}

bool
Hashtable::Delete(uint64_t key) {
    uint64_t *kp = LookupKeyPtr(key);
    if (!kp)
        return false;
    *kp = UNUSED;
    return true;
}

bool
Hashtable::Replace(uint64_t key, void *ptr) {
    uint64_t h;
    uint16_t mk;
    uint64_t *kp = LookupKeyPtr(key);
    if (!kp)
        return false;
    hash(key, &h, &mk);
    *kp = MKENTRY(mk, ptr);
    return true;
}

void
Hashtable::Insert(uint64_t key, void *ptr)
{
    uint64_t b = rdtsc();
    uint64_t h;
    uint16_t mk;
    int i;

    hash(key, &h, &mk);
    cacheline *cl = &table[h % table_lines];

    while (1) {
        uint64_t *kp = cl->keys;
        for (i = 0; i < 8; i++, kp++) {
            if (*kp == UNUSED) {
                *kp = MKENTRY(mk, ptr);
                ins_total += (rdtsc() - b);
                return;
            }
        }

        // no empty space found, allocate a new cache line
        if (!ISCHAIN(cl, 7)) {
            cacheline *ncl =
                static_cast<cacheline *>(MallocAligned(sizeof(cacheline)));
            ncl->keys[0] = cl->keys[7];
            for (i = 1; i < 8; i++)
                ncl->keys[i] = UNUSED;
            cl->keys[7] = MKCHAINPTR(ncl);
        }

        cl = reinterpret_cast<cacheline *>(GETCHAINPTR(cl, 7));
        ins_nexts++;
    }
}

} // namespace RAMCloud
