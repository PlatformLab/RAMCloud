/* Copyright (c) 2012-2015 Stanford University
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

#include "Common.h"
#include "BitOps.h"
#include "LogSegment.h"
#include "SegletAllocator.h"
#include "Segment.h"
#include "ServerConfig.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Construct a new SegmentAllocator by allocating a large chunk of memory
 * and chopping it up into individual seglets of the specified size. All
 * seglets will be placed in the lowest priority "default" pool.
 *
 * \param config
 *      Server runtime configuration, specifying various parameters like
 *      seglet size and bytes to allocate for seglets.
 *
 */
SegletAllocator::SegletAllocator(const ServerConfig* config)
    : segletSize(config->segletSize),
      segletSizeShift(BitOps::findFirstSet(segletSize) - 1),
      lock("SegletAllocator::lock"),
      emergencyHeadPool(),
      emergencyHeadPoolReserve(0),
      cleanerPool(),
      cleanerPoolReserve(0),
      defaultPool(),
      segletToSegmentTable(),
      block(config->master.logBytes)
{
    assert(BitOps::isPowerOfTwo(segletSize));
    uint8_t* segletBlock = block.get();
    for (size_t i = 0; i < (block.length / segletSize); i++) {
        Seglet* seglet = new Seglet(*this, segletBlock, segletSize);
        segletToSegmentTable.push_back(NULL);
        defaultPool.push_back(seglet);
        segletBlock += segletSize;
    }
}

/**
 * Clean up by freeing all seglets and deallocating the block of memory they
 * came from.
 */
SegletAllocator::~SegletAllocator()
{
    size_t totalFree = emergencyHeadPool.size() +
                       cleanerPool.size() +
                       defaultPool.size();
    size_t expectedFree = block.length / segletSize;

    if (totalFree != expectedFree)
        LOG(WARNING, "Destructor called before all seglets freed!");

    foreach (Seglet* s, emergencyHeadPool)
        delete s;
    foreach (Seglet* s, cleanerPool)
        delete s;
    foreach (Seglet* s, defaultPool)
        delete s;
}

/**
 * Populate the given protocol buffer with various metrics about the segments
 * we're managing.
 */
void
SegletAllocator::getMetrics(ProtoBuf::LogMetrics_SegletMetrics& m)
{
    // .size() methods on vectors are not necessarily thread-safe.
    std::lock_guard<SpinLock> guard(lock);

    m.set_total_seglets(getTotalCount());
    m.set_total_usable_seglets(getTotalCount() -
                               emergencyHeadPoolReserve -
                               cleanerPoolReserve);
    m.set_emergency_head_pool_reserve(emergencyHeadPoolReserve);
    m.set_emergency_head_pool_count(emergencyHeadPool.size());
    m.set_cleaner_pool_reserve(cleanerPoolReserve);
    m.set_cleaner_pool_count(cleanerPool.size());
    m.set_default_pool_count(defaultPool.size());
}

/**
 * Allocate and return the given number of seglets from a pool designated by
 * the way purpose for which the seglets will be used.
 *
 * This method either successfully allocates all seglets requested, or none.
 *
 * \param type
 *      Type specifying the pool to allocate from.
 * \param count
 *      The number of seglets to allocate.
 * \param[out] outSeglets
 *      Vector in which allocated seglets will be returned.
 */
bool
SegletAllocator::alloc(AllocationType type,
                       uint32_t count,
                       vector<Seglet*>& outSeglets)
{
    std::lock_guard<SpinLock> guard(lock);

    if (type == EMERGENCY_HEAD)
        return allocFromPool(emergencyHeadPool, count, outSeglets);

    if (type == CLEANER)
        return allocFromPool(cleanerPool, count, outSeglets);

    return allocFromPool(defaultPool, count, outSeglets);
}

/**
 * Set up the initial pool of reserved emergency head seglets by allocating the
 * given quantity. Seglets allocated from this pool will return to it when they
 * are freed. The pool will never take on any other freed seglets.
 *
 * \param numSeglets
 *      The maximum number of seglets to keep in the cleaner reserve. Upon
 *      successful return, this many seglets will also have been reserved.
 * \return
 *      True if sufficient seglets were available to meet the request. False if
 *      there were insufficient seglets, or if this method had already been
 *      called previously.
 */
bool
SegletAllocator::initializeEmergencyHeadReserve(uint32_t numSeglets)
{
    std::lock_guard<SpinLock> guard(lock);

    if (emergencyHeadPoolReserve != 0)
        return false;

    if (!allocFromPool(defaultPool, numSeglets, emergencyHeadPool))
        return false;

    foreach (Seglet* seglet, emergencyHeadPool)
        seglet->setSourcePool(&emergencyHeadPool);

    LOG(NOTICE, "Reserved %u seglets for emergency head segments (%lu MB). "
        "%lu seglets (%lu MB) left in default pool.",
        numSeglets,
        static_cast<uint64_t>(numSeglets) * segletSize / 1024 / 1024,
        defaultPool.size(),
        defaultPool.size() * segletSize / 1024 / 1024);

    emergencyHeadPoolReserve = numSeglets;
    return true;
}

/**
 * Set up the initial pool of reserved cleaner seglets by allocating the given
 * quantity. In the future, when seglets are removed from the pool, freed
 * seglets will be added back to it until the given maximum is reached again.
 *
 * \param numSeglets
 *      The maximum number of seglets to keep in the cleaner reserve. Upon
 *      successful return, this many seglets will also have been reserved.
 * \return
 *      True if sufficient seglets were available to meet the request. False if
 *      there were insufficient seglets, or if this method had already been
 *      called previously.
 */
bool
SegletAllocator::initializeCleanerReserve(uint32_t numSeglets)
{
    std::lock_guard<SpinLock> guard(lock);

    if (cleanerPoolReserve != 0)
        return false;

    if (!allocFromPool(defaultPool, numSeglets, cleanerPool))
        return false;

    LOG(NOTICE, "Reserved %u seglets for the cleaner (%lu MB). %lu seglets "
        "(%lu MB) left in default pool.",
        numSeglets,
        static_cast<uint64_t>(numSeglets) * segletSize / 1024 / 1024,
        defaultPool.size(),
        defaultPool.size() * segletSize / 1024 / 1024);

    cleanerPoolReserve = numSeglets;
    return true;
}

/**
 * Return a seglet to this allocator. This is normally invoked within the
 * Seglet::free() method when a seglet returns itself.
 */
void
SegletAllocator::free(Seglet* seglet)
{
    if (DEBUG_BUILD)
        memset(seglet->get(), '!', seglet->getLength());

    std::lock_guard<SpinLock> guard(lock);

    // This seglet no longer belongs to any segment, so update that fact first.
    setOwnerSegment(seglet, NULL);

    // The emergency head pool is special. Seglets that came from it must be
    // returned to it. Futhermore, only segments that came from it should be
    // returned.
    //
    // The reason is a little subtle. The problem with always preferring to put
    // seglets into this pool if it's non-empty is that under high memory
    // utilization we could fail to re-fill the cleaner's pool, preventing it
    // from having enough space to work with and deadlocking the system.
    if (seglet->getSourcePool() == &emergencyHeadPool) {
        emergencyHeadPool.push_back(seglet);
        return;
    }

    // Any seglets not allocated to emergency heads should be used to fill empty
    // space in the cleaner reserve. The cleaner maintains the invariant that
    // after every pass it has consumed no more seglets than it has freed. Thus
    // this pool should never remain non-full for long.
    if (cleanerPool.size() < cleanerPoolReserve) {
        cleanerPool.push_back(seglet);
        return;
    }

    // If we're making forward progress, any excess clean seglets accumulate in
    // the default pool. New log heads can allocate from this to service new
    // log appends.
    defaultPool.push_back(seglet);
}

/**
 * Return the total number of seglets this allocator is managing. This includes
 * free seglets, reserved seglets, and currently allocated ones.
 */
size_t
SegletAllocator::getTotalCount()
{
    return block.length / segletSize;
}

/**
 * Return the number of free seglets available for the given allocation
 * type.
 */
size_t
SegletAllocator::getFreeCount(AllocationType type)
{
    std::lock_guard<SpinLock> guard(lock);

    if (type == EMERGENCY_HEAD)
        return emergencyHeadPool.size();
    if (type == CLEANER)
        return cleanerPool.size();
    assert(type == DEFAULT);
    return defaultPool.size();
}

size_t
SegletAllocator::getTotalCount(AllocationType type)
{
    if (type == EMERGENCY_HEAD)
        return emergencyHeadPoolReserve;
    if (type == CLEANER)
        return cleanerPoolReserve;
    assert(type == DEFAULT);
    return getTotalCount() - emergencyHeadPoolReserve - cleanerPoolReserve;
}

/**
 * Return the size of each seglet in bytes.
 */
uint32_t
SegletAllocator::getSegletSize()
{
    return segletSize;
}

/**
 * Return a pointer to the first byte of the contiguous buffer that all log
 * memory is allocated from. This is used by the transports to register
 * memory areas for zero-copy transmits.
 */
const void*
SegletAllocator::getBaseAddress()
{
    return block.get();
}

/**
 * Return the total number of bytes this allocator has for the log.
 */
uint64_t
SegletAllocator::getTotalBytes()
{
    return block.length;
}

/**
 * Return the percentage of unreserved seglets currently allocated. In other
 * words, the amount of space allocated in the log, not including seglets set
 * aside for cleaning or emergency head segments. The value returned is in
 * the range [0, 100].
 */
int
SegletAllocator::getMemoryUtilization()
{
    std::lock_guard<SpinLock> guard(lock);

    size_t maxDefaultPoolSize = getTotalCount() -
                                emergencyHeadPoolReserve -
                                cleanerPoolReserve;
    return downCast<int>(100 * (maxDefaultPoolSize - defaultPool.size()) /
                         maxDefaultPoolSize);
}

/**
 * XXX
 */
LogSegment*
SegletAllocator::getOwnerSegment(const void* p)
{
    return segletToSegmentTable[getSegletIndex(p)];
}

void
SegletAllocator::setOwnerSegment(Seglet* seglet, LogSegment* segment)
{
    size_t index = getSegletIndex(seglet->get());
    assert(segment == NULL || segletToSegmentTable[index] == NULL);
    segletToSegmentTable[index] = segment;

    // segletToSegmentTable is not protected by any locks, but may be accessed
    // by multiple threads. I believe this is safe because: 1) it is never
    // resized, and 2) only one thread should be able to modify a particular
    // index at any point in time (the thread that allocated or freed the
    // seglet).
    //
    // This fence should ensure that other threads see the proper pointer (for
    // example, threads servicing a read request on a discontiguous object that
    // starts in this seglet). It may be an impossible race, but I want to make
    // sure that the table is updated by the time anything is stored in this
    // seglet.
    Fence::sfence();
}

size_t
SegletAllocator::getSegletIndex(const void* p)
{
    uintptr_t i = reinterpret_cast<uintptr_t>(p);
    uintptr_t blockBase = reinterpret_cast<uintptr_t>(block.get());
    assert(i >= blockBase && i < blockBase + block.length);
    return (i - blockBase) >> segletSizeShift;
}

/**
 * Allocate the exact number of requested seglets from a specific pool. If
 * the full allocation cannot be met, allocate nothing and return false.
 * Otherwise, return true.
 *
 * This must be called with the monitor lock held.
 *
 * \param pool
 *      The pool to allocator from.
 * \param count
 *      The number of seglets to allocate.
 * \param outSeglets
 *      Vector to return allocated seglets in.
 * \return
 *      True if the full allocation succeeded, otherwise false.
 */
bool
SegletAllocator::allocFromPool(vector<Seglet*>& pool,
                               uint32_t count,
                               vector<Seglet*>& outSeglets)
{
    if (pool.size() < count)
        return false;

    outSeglets.insert(outSeglets.end(), pool.end() - count, pool.end());
    pool.erase(pool.end() - count, pool.end());
    return true;
}

} // end RAMCloud
