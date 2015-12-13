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

#ifndef RAMCLOUD_SEGLETALLOCATOR_H
#define RAMCLOUD_SEGLETALLOCATOR_H

#include "Common.h"
#include "LargeBlockOfMemory.h"
#include "Seglet.h"
#include "SpinLock.h"

#include "LogMetrics.pb.h"

namespace RAMCloud {

class ServerConfig;
class LogSegment;

/**
 * This class manages the allocation of all seglets in a server. For more
 * details on the purposes of seglets, see the Seglet and Segment classes.
 *
 * Beyond serving as a pool for unallocated seglets, this class also maintains
 * several special reserves of seglets that are needed to handle low memory
 * conditions without deadlocking.
 *
 * The first reserve pool is for seglets used in "emergency" head segments.
 * SegmentManager typically reserves two full segments' worth of seglets so
 * that it can always allocate a new head and write a log digest. Two suffice
 * because it guarantees that it never needs more than two at any given time.
 *
 * The second reserve pool is for seglets used in log cleaning. The log cleaner
 * specifies how many to reserve and guarantees that it always frees at least
 * as many seglets as it consumes during each cleaning pass. This means that
 * the pool is always completely re-filled.
 *
 * Finally, there is a "default" pool from which regular log heads are allocated
 * to service normal log appends.
 *
 * How seglets are returned to appropriate pools is somewhat subtle (and
 * annoyingly so). See the free() method's documentation if you're interested.
 *
 * Seglets are always allocated by the SegmentManager when it creates new log
 * segments and are freed by the Segment class they're assigned to, either at
 * destruction time, or when the segment is closed and told to free unused
 * seglets that have not had data appended to them.
 */
class SegletAllocator {
  public:
    /**
     * All seglets are allocated from a caller-specified pool. This allows us
     * to set aside seglets and reserve them for certain purposes. This enum
     * represents the possible pools.
     */
    enum AllocationType {
        EMERGENCY_HEAD,
        CLEANER,
        DEFAULT
    };

    explicit SegletAllocator(const ServerConfig* config);
    ~SegletAllocator();
    void getMetrics(ProtoBuf::LogMetrics_SegletMetrics& m);
    bool alloc(AllocationType type,
               uint32_t count,
               vector<Seglet*>& outSeglets);
    bool initializeEmergencyHeadReserve(uint32_t numSeglets);
    bool initializeCleanerReserve(uint32_t numSeglets);
    void free(Seglet* seglet);
    size_t getTotalCount();
    size_t getTotalCount(AllocationType type);
    size_t getFreeCount(AllocationType type);
    uint32_t getSegletSize();
    const void* getBaseAddress();
    uint64_t getTotalBytes();
    int getMemoryUtilization();
    LogSegment* getOwnerSegment(const void* p);
    void setOwnerSegment(Seglet* seglet, LogSegment* segment);

  PRIVATE:
    size_t getSegletIndex(const void* p);
    bool allocFromPool(vector<Seglet*>& pool,
                       uint32_t count,
                       vector<Seglet*>& outSeglets);

    /// Size of each seglet in bytes.
    const uint32_t segletSize;

    /// Log2(segletSize). Allows us to quickly find the index of a seglet
    /// (see getSegletIndex()).
    const uint32_t segletSizeShift;

    /// Monitor-style lock protecting the mutable class members. This allocator
    /// is mostly invoked under the SegmentManager's monitor lock, but the
    /// cleaner also requests segments to return unused seglets to the allocator
    /// outside of that lock.
    SpinLock lock;

    /// Pool holding seglets reserved for emergency head allocations.
    //
    /// There are two situations in which we must be able to allocate a new
    /// head, even if we're out of memory:
    ///
    /// 1) ReplicaManager detected a head replica failure and needs to close the
    ///    previous head immediately.
    /// 2) There are cleaned segments that could be freed up once we write a new
    ///    log digest that doesn't include them.
    ///
    /// In both of these cases, we need only be able to open a new head and
    /// write data that will be superceded by the next new head. So, it suffices
    /// to keep around enough seglets for two emergency head segments that we
    /// can open in such a situation and immediately reclaim once the next head
    /// is opened.
    vector<Seglet*> emergencyHeadPool;

    /// Maximum number of seglets to reserve in the emergencyHeadPool.
    uint32_t emergencyHeadPoolReserve;

    /// Pool holding seglets reserved for log cleaning.
    ///
    /// The cleaner needs to ensure that enough seglets are kept in reserve so
    /// that it can continue to clean. Otherwise, it might deadlock.
    vector<Seglet*> cleanerPool;

    /// Maximum number of seglets to reserve in the cleanerPool.
    uint32_t cleanerPoolReserve;

    /// Pool holding all other seglets not otherwise reserved.
    vector<Seglet*> defaultPool;

    /// Table mapping blocks of memory backing Seglets to their owner LogSegment
    /// objects. This allows getOwnerSegment() to look up a LogSegment object
    /// based on a pointer anywhere into ``block'' below.
    vector<LogSegment*> segletToSegmentTable;

    /// Single contiguous block of memory backing all of our seglets.
    LargeBlockOfMemory<uint8_t> block;

    DISALLOW_COPY_AND_ASSIGN(SegletAllocator);
};

} // end RAMCloud

#endif // RAMCLOUD_SEGLETALLOCATOR_H
