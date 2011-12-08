/* Copyright (c) 2009, 2010 Stanford University
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

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "Crc32C.h"
#include "CycleCounter.h"
#include "RawMetrics.h"
#include "Segment.h"
#include "SegmentIterator.h"
#include "ShortMacros.h"
#include "Log.h"
#include "LogTypes.h"
#include "WallTime.h"

namespace RAMCloud {

/**
 * Constructor for Segment.
 * \param[in] log
 *      Pointer to the Log this Segment is a part of.
 * \param[in] segmentId
 *      The unique identifier for this Segment.
 * \param[in] baseAddress
 *      A pointer to memory that will back this Segment. The memory must be at
 *      least #capacity bytes in length as well as power-of-two aligned.
 * \param[in] capacity
 *      The size of the backing memory pointed to by baseAddress in bytes. Must
 *      be a power of two.
 * \param[in] replicaManager
 *      The ReplicaManager responsible for this Segment's durability.
 * \param[in] type
 *      See #append. Used for transmitting a LogDigest atomically with the RPC
 *      that opens the segment.
 * \param[in] buffer
 *      See #append. Used for transmitting a LogDigest atomically with the RPC
 *      that opens the segment.
 * \param[in] length
 *      See #append. Used for transmitting a LogDigest atomically with the RPC
 *      that opens the segment.
 * \return
 *      The newly constructed Segment object.
 */
Segment::Segment(Log *log,
                 uint64_t segmentId,
                 void *baseAddress,
                 uint32_t capacity,
                 ReplicaManager *replicaManager,
                 LogEntryType type,
                 const void *buffer,
                 uint32_t length)
    : replicaManager(replicaManager),
      baseAddress(baseAddress),
      log(log),
      logId(log->getId()),
      id(segmentId),
      capacity(capacity),
      tail(0),
      bytesExplicitlyFreed(0),
      bytesImplicitlyFreed(0),
      spaceTimeSum(0),
      implicitlyFreedSpaceTimeSum(0),
      checksum(),
      prevChecksum(),
      canRollBack(false),
      closed(false),
      mutex(),
      entryCountsByType(),
      listEntries(),
      cleanedEpoch(-1),
      replicatedSegment(NULL)
{
    commonConstructor(type, buffer, length);
}

/**
 * Constructor for Segment.
 * \param[in] logId
 *      The unique identifier for the Log to which this Segment belongs.
 * \param[in] segmentId
 *      The unique identifier for this Segment.
 * \param[in] baseAddress
 *      A pointer to memory that will back this Segment. The memory must be at
 *      least #capacity bytes in length as well as power-of-two aligned.
 * \param[in] capacity
 *      The size of the backing memory pointed to by baseAddress in bytes. Must
 *      be a power of two.
 * \param[in] replicaManager
 *      The ReplicaManager responsible for this Segment's durability.
 * \return
 *      The newly constructed Segment object.
 */
Segment::Segment(uint64_t logId,
                 uint64_t segmentId,
                 void *baseAddress,
                 uint32_t capacity,
                 ReplicaManager *replicaManager)
    : replicaManager(replicaManager),
      baseAddress(baseAddress),
      log(NULL),
      logId(logId),
      id(segmentId),
      capacity(capacity),
      tail(0),
      bytesExplicitlyFreed(0),
      bytesImplicitlyFreed(0),
      spaceTimeSum(0),
      implicitlyFreedSpaceTimeSum(0),
      checksum(),
      prevChecksum(),
      canRollBack(false),
      closed(false),
      mutex(),
      entryCountsByType(),
      listEntries(),
      cleanedEpoch(-1),
      replicatedSegment(NULL)
{
    commonConstructor(LOG_ENTRY_TYPE_INVALID, NULL, 0);
}

/**
 * Perform actions common to all Segment constructors, including writing
 * the header and opening the replica on backups.
 * \param[in] type
 *      See #append. Used for transmitting a LogDigest atomically with the RPC
 *      that opens the segment.
 * \param[in] buffer
 *      See #append. Used for transmitting a LogDigest atomically with the RPC
 *      that opens the segment.
 * \param[in] length
 *      See #append. Used for transmitting a LogDigest atomically with the RPC
 *      that opens the segment.
 */
void
Segment::commonConstructor(LogEntryType type,
                           const void *buffer, uint32_t length)
{
    if (!BitOps::isPowerOfTwo(capacity))
        throw SegmentException(HERE, "segment capacity must be a power of two");

    if (capacity < (sizeof(SegmentEntry) + sizeof(SegmentHeader) +
                    sizeof(SegmentEntry) + sizeof(SegmentFooter))) {
        throw SegmentException(HERE, "segment capacity is too small");
    }

    if (ffs(capacity) == 0 || ffs(capacity) > 31) {
        throw SegmentException(HERE, "segment capacity must be power of two"
            "between 1 and 2^31");
    }

    if ((reinterpret_cast<uintptr_t>(baseAddress) % capacity) != 0)
        throw SegmentException(HERE, "segment memory not aligned to capacity");

    SegmentHeader segHdr = { logId, id, capacity };
    SegmentEntryHandle h = forceAppendWithEntry(LOG_ENTRY_TYPE_SEGHEADER,
                                                &segHdr, sizeof(segHdr), false);
    assert(h != NULL);
    if (length) {
        SegmentEntryHandle h = forceAppendWithEntry(type,
                                                    buffer, length, false);
        assert(h != NULL);
    }
    if (replicaManager)
        replicatedSegment = replicaManager->openSegment(id, baseAddress, tail);

    // Even if we're not backing up synchronously, we shouldn't be able to roll
    // back this metadata.
    canRollBack = false;

    memset(entryCountsByType, 0, sizeof(entryCountsByType));
}

Segment::~Segment()
{
}

/**
 * \copydoc Segment::locklessAppend
 */
SegmentEntryHandle
Segment::append(LogEntryType type, const void *buffer, uint32_t length,
    bool sync, Tub<SegmentChecksum::ResultType> expectedChecksum)
{
    boost::lock_guard<SpinLock> lock(mutex);
    return locklessAppend(type, buffer, length, sync, expectedChecksum);
}

/**
 * Append an entry described by a given SegmentEntryHandle. This method is used
 * exclusively by the cleaner as a simple way to write existing entries to a
 * new Segment. This operation also ensures that the checksum of the new entry
 * matches the old.
 *
 * \param[in] handle
 *      Handle to the object we want to append to this Segment.
 * \param[in] sync
 *      If true then this write to replicated to backups before return,
 *      otherwise the replication will happen on a subsequent append()
 *      where sync is true or when the segment is closed.  This defaults
 *      to true.
 */
SegmentEntryHandle
Segment::append(SegmentEntryHandle handle, bool sync)
{
    // NB: no need to take the mutex since append() will
    return append(handle->type(),
                  handle->userData<void*>(),
                  handle->length(),
                  sync,
                  Tub<SegmentChecksum::ResultType>(handle->checksum()));
}

SegmentEntryHandleVector
Segment::multiAppend(SegmentMultiAppendVector& appends, bool sync)
{
    CycleCounter<RawMetric> _(&metrics->master.segmentAppendTicks);
    boost::lock_guard<SpinLock> lock(mutex);
    SegmentEntryHandleVector handles;

    if (closed)
        return {};

    size_t totalEntryBytes = 0;
    for (size_t i = 0; i < appends.size(); i++) {
        if (appends[i].type == LOG_ENTRY_TYPE_SEGFOOTER)
            return {};
        totalEntryBytes += appends[i].length;
    }

    if (!locklessCanAppendEntries(appends.size(), totalEntryBytes))
        return {};

    for (size_t i = 0; i < appends.size(); i++) {
        handles.push_back(locklessAppend(appends[i].type,
                                         appends[i].buffer,
                                         appends[i].length,
                                         false,
                                         appends[i].expectedChecksum));

        // This should never fail. It was up to this method to ensure
        // success before starting the appends.
        assert(handles[i] != NULL);
    }

    // Sync once, if needed, in order to write everything atomically.
    if (replicaManager) {
        replicatedSegment->write(tail);
        if (sync) {
            replicaManager->sync();
        }
    }

    return handles;
}

/**
 * Undo the previous append operation. This function exists for the cleaning
 * code: we need to store an entry in a new Segment before we know whether it
 * will actually be used (it may have been overwritten/deleted since). If its
 * not used, it must be rolled back, otherwise we could recover a dead object
 * on recovery.
 *
 * \param[in] handle
 *      Handle to the entry returned by the previous #append() operation.
 *      The previous append must not have synced the data to backups.
 * \throw SegmentException
 *      A SegmentException is thrown if rolling back is not possible This
 *      can be due to several reasons: the Segment has since been closed,
 *      the handle is invalid or does not refer to the last entry in the
 *      Segment (other appends may have taken place), or the append
 *      operation that wrote the entry was synchronous (i.e. replicated
 *      the data to backups). Currently only asynchronous appends may be
 *      rolled back. 
 */
void
Segment::rollBack(SegmentEntryHandle handle)
{
    boost::lock_guard<SpinLock> lock(mutex);

    if (closed)
        throw SegmentException(HERE, "Cannot roll back on closed Segment");

    if (!canRollBack)
        throw SegmentException(HERE, "Cannot roll back any further!");

    uintptr_t handleBase  = reinterpret_cast<uintptr_t>(handle->userData());
    uintptr_t currentBase = reinterpret_cast<uintptr_t>(baseAddress);
    if (handleBase + handle->length() != currentBase + tail)
        throw SegmentException(HERE, "Invalid handle to last appended entry");

    checksum = prevChecksum;
    canRollBack = false;
    tail -= handle->totalLength();
    decrementSpaceTimeSum(handle);
    entryCountsByType[downCast<uint8_t>(handle->type())]--;
}

/**
 * Mark bytes used by a single entry in this Segment as freed. This simply
 * maintains counters that can be used to compute utilisation of the Segment,
 * as well as a space-weighted age of entries (for computing a mean age of
 * data in the segment).
 *
 * \param[in] entry
 *      A SegmentEntryHandle as returned by an #append call.
 */
void
Segment::free(SegmentEntryHandle entry)
{
    boost::lock_guard<SpinLock> lock(mutex);

    assert((uintptr_t)entry >= ((uintptr_t)baseAddress + sizeof(SegmentEntry)));
    assert((uintptr_t)entry <  ((uintptr_t)baseAddress + capacity));

    // be sure to account for SegmentEntry structs before each append
    uint32_t length = entry->totalLength();

    assert((bytesExplicitlyFreed + bytesImplicitlyFreed + length) <= tail);

    bytesExplicitlyFreed += length;

    decrementSpaceTimeSum(entry);
}

/**
 * Set the counts for entries that have been implicitly freed (i.e. for
 * entry types that the user will not explicitly mark as free). The
 * cleaner determines when these entries are no longer live and updates
 * the counts with this method. As such, this method is only ever intended
 * for invocation by the LogCleaner class.
 *
 * \param freeByteSum
 *      Total number of bytes to be marked free. This should include all
 *      overhead (e.g. metadata), as returned by the LogEntryHandle's
 *      totalLength() method.
 *
 * \param freeSpaceTimeSum
 *      Sum of the product of byte count and timestamps from all free
 *      entries reflected in the freeByteSm count. If any entries do not
 *      have timestamps (there is no timestamp callback associated with the
 *      type, then they should contribute nothing to this sum.
 */
void
Segment::setImplicitlyFreedCounts(uint32_t freeByteSum,
                                  uint64_t freeSpaceTimeSum)
{
    boost::lock_guard<SpinLock> lock(mutex);

    // Count should never decrease subsequently.
    assert(bytesImplicitlyFreed <= freeByteSum);
    assert(implicitlyFreedSpaceTimeSum <= freeSpaceTimeSum);

    bytesImplicitlyFreed = freeByteSum;
    implicitlyFreedSpaceTimeSum = freeSpaceTimeSum;

    // Sanity check.
    assert(bytesImplicitlyFreed + bytesExplicitlyFreed <= capacity);
    assert(implicitlyFreedSpaceTimeSum <= spaceTimeSum);
}

/**
 * Close the Segment. Once a Segment has been closed, it is considered
 * closed, i.e. it cannot be appended to. Calling #free on a closed
 * Segment to maintain utilisation counts is still permitted. 
 * \param nextHead
 *      For a normal log segment this is a pointer to the Segment
 *      which logically will follow this segment in the log.  Used to check
 *      ordering constraints of backup replication operations.
 *      Pass NULL for log cleaning or during unit testing to bypass the
 *      ordering constraints.  See ReplicatedSegment::close for details
 *      about the contraints which are critical for log integrity.
 * \param sync
 *      Whether to wait for the replicas to acknowledge that the segment is
 *      closed.
 * \throw SegmentException
 *      An exception is thrown if the Segment has already been closed.
 */
void
Segment::close(Segment* nextHead, bool sync)
{
    boost::lock_guard<SpinLock> lock(mutex);

    if (closed)
        throw SegmentException(HERE, "Segment has already been closed");

    SegmentFooter footer = { checksum.getResult() };

    const void *p = forceAppendWithEntry(LOG_ENTRY_TYPE_SEGFOOTER, &footer,
        sizeof(SegmentFooter), false, false);
    assert(p != NULL);

    // ensure that any future append() will fail
    closed = true;

    if (replicaManager) {
        replicatedSegment->close(nextHead ?
                                    nextHead->replicatedSegment : NULL);
        if (sync) // sync determines whether to wait for the acks
            replicaManager->sync();
    }
}

/**
 * Wait for the segment to be fully replicated.
 */
void
Segment::sync()
{
    boost::lock_guard<SpinLock> lock(mutex);
    if (replicaManager)
        replicaManager->sync();
}

/**
 * Request the eventual freeing all known replicas of a segment from its
 * backups.  Requires that the segment has been closed.
 */
void
Segment::freeReplicas()
{
    assert(closed);
    assert(!replicaManager || replicatedSegment);
    if (replicaManager) {
        replicatedSegment->free();
        replicatedSegment = NULL;
    }
}


/**
 * Obtain a const pointer to the first byte of backing memory for this Segment.
 */
const void *
Segment::getBaseAddress() const
{
    // NB: constant - no need for lock
    return baseAddress;
}

/**
 * Obtain the Segment's Id, which was originally specified in the constructor.
 */
uint64_t
Segment::getId() const
{
    // NB: constant - no need for lock
    return id;
}

/**
 * Obtain the number of bytes of backing memory that this Segment represents.
 */
uint32_t
Segment::getCapacity() const
{
    // NB: constant - no need for lock
    return capacity;
}

/**
 * \copydoc Segment::locklessAppendableBytes
 */
uint32_t
Segment::appendableBytes()
{
    boost::lock_guard<SpinLock> lock(mutex);
    return locklessAppendableBytes();
}

/**
 * Return the Segment's utilisation as an integer percentage. This is
 * calculated by taking into account the number of live bytes written to
 * the Segment minus the freed bytes in proportion to its capacity.
 */
int
Segment::getUtilisation()
{
    boost::lock_guard<SpinLock> lock(mutex);
    return static_cast<int>((100UL * locklessGetLiveBytes()) / capacity);
}

/**
 * \copydoc Segment::locklessGetLiveBytes
 */
uint32_t
Segment::getLiveBytes()
{
    boost::lock_guard<SpinLock> lock(mutex);
    return locklessGetLiveBytes();
}

/**
 * Return the number of bytes that aren't being used in the Segment. This is
 * the number of bytes that could be freed up if this Segment were to be
 * cleaned.
 */
uint32_t
Segment::getFreeBytes()
{
    // NB: capacity is constant, getLiveBytes will lock
    return capacity - getLiveBytes();
}

/**
 * Return a RAMCloud timestamp that's indicates the average time each byte
 * was written in this Segment. This can be used to tell the average age
 * of data in the Segment.
 *
 * \throw SegmentException
 *      A SegmentException is thrown if this method is called on a Segment
 *      that isn't part of a Log (i.e. if an alternate constructor was used
 *      that did not take in a Log pointer). Access to Log callbacks is
 *      needed to obtain ages for registered types.
 */
uint64_t
Segment::getAverageTimestamp()
{
    boost::lock_guard<SpinLock> lock(mutex);

    if (!log) {
        throw SegmentException(HERE, format("%s() is only valid "
                                     "if Segment is part of a log", __func__));
    }

    uint64_t liveBytes = locklessGetLiveBytes();
    if (liveBytes == 0)
        return 0;

    // Take into account entries determined to be free by the LogCleaner.
    uint64_t adjustedSpaceTimeSum = spaceTimeSum - implicitlyFreedSpaceTimeSum;

    return adjustedSpaceTimeSum / liveBytes;
}

////////////////////////////////////////
/// Private Methods
////////////////////////////////////////

/**
 * Append an entry to this Segment. Entries consist of a typed header, followed
 * by the user-specified contents. Note that this operation makes no guarantees
 * about data alignment.
 * \param[in] type
 *      The type of entry to append. All types except LOG_ENTRY_TYPE_SEGFOOTER
 *      are permitted.
 * \param[in] buffer
 *      Data to be appended to this Segment.
 * \param[in] length
 *      Length of the data to be appended in bytes.
 * \param[in] sync
 *      If true then this write to replicated to backups before return,
 *      otherwise the replication will happen on a subsequent append()
 *      where sync is true or when the segment is closed.  This defaults
 *      to true.
 * \param[in] expectedChecksum
 *      The checksum we expect this entry to have once appended. If the
 *      actual calculated checksum does not match, an exception is
 *      thrown and nothing is appended. This parameter is optional.
 * \return
 *      On success, a SegmentEntryHandle is returned, which points to the
 *      ``buffer'' written. On failure, the handle is NULL. We avoid using
 *      slow exceptions since this can be on the fast path.
 */
SegmentEntryHandle
Segment::locklessAppend(LogEntryType type, const void *buffer, uint32_t length,
    bool sync, Tub<SegmentChecksum::ResultType> expectedChecksum)
{
    CycleCounter<RawMetric> _(&metrics->master.segmentAppendTicks);

    if (closed || type == LOG_ENTRY_TYPE_SEGFOOTER ||
      !locklessCanAppendEntries(1, length))
        return NULL;

    return forceAppendWithEntry(type, buffer, length,
        sync, true, expectedChecksum);
}

/**
 * Return the number of bytes that are being used in the Segment. This is
 * the total amount of live data.
 */
uint32_t
Segment::locklessGetLiveBytes() const
{
    return tail - bytesExplicitlyFreed - bytesImplicitlyFreed;
}

/**
 * Obtain the maximum number of bytes that can be appended to this Segment,
 * including space that will need to be used for metadata.
 */
uint32_t
Segment::locklessAppendableBytes() const
{
    if (closed)
        return 0;

    uint32_t headRoom = downCast<uint32_t>(sizeof(SegmentEntry) +
                                           sizeof(SegmentFooter));
    uint32_t freeBytes = capacity - tail;

    assert(freeBytes >= headRoom);

    return freeBytes - headRoom;
}

/**
 * Given a number of entries and the total length of the entries in
 * bytes, return whether or not they can be written into the free
 * space left in this Segment.
 * 
 * \param numberOfEntries
 *      The number of entries we want to see if we can append.
 *
 * \param numberOfBytesInEntries
 *      The total number of bytes in each of the entries we want to
 *      see if we can append.
 */
bool
Segment::locklessCanAppendEntries(size_t numberOfEntries,
                                  size_t numberOfBytesInEntries) const
{
    assert(numberOfBytesInEntries == 0 || numberOfEntries != 0);

    if (closed)
        return 0;

    uint32_t bytesNeeded = downCast<uint32_t>(numberOfEntries) *
                           downCast<uint32_t>(sizeof(SegmentEntry)) +
                           downCast<uint32_t>(numberOfBytesInEntries);

    uint32_t headRoom = downCast<uint32_t>(sizeof(SegmentEntry) +
                                           sizeof(SegmentFooter));
    uint32_t freeBytes = capacity - tail - headRoom;

    return (freeBytes >= bytesNeeded);
}

/**
 * Increment the spaceTime product sum in light of a new entry.
 *
 * \param[in] handle
 *      Handle of the entry appended.
 */
void
Segment::incrementSpaceTimeSum(SegmentEntryHandle handle)
{
    adjustSpaceTimeSum(handle, false);
}

/**
 * Decrement the spaceTime product sum in light of a dead entry.
 *
 * \param[in] handle
 *      Handle of the entry freed.
 */
void
Segment::decrementSpaceTimeSum(SegmentEntryHandle handle)
{
    adjustSpaceTimeSum(handle, true);
}

/**
 * Helper for #incrementSpaceTimeSum and #decrementSpaceTimeSum.
 */
void
Segment::adjustSpaceTimeSum(SegmentEntryHandle handle, bool subtract)
{
    if (log) {
        const LogTypeInfo *cb = log->getTypeInfo(handle->type());
        if (cb != NULL && cb->timestampCB != NULL) {
            // XXX should the timestamp callback be mandatory for externally-
            //     defined types? If someone defines a commonly-used type with
            //     no timestamp callback, age calculations will not be accurate.
            uint32_t timestamp = cb->timestampCB(handle);
            uint64_t product = (uint64_t)timestamp * handle->totalLength();
            if (subtract) {
                assert(product <= spaceTimeSum);
                spaceTimeSum -= product;
            } else {
                spaceTimeSum += product;
            }
        }
    }
}

/**
 * Append exactly the provided raw bytes to the memory backing this Segment.
 * Note that no SegmentEntry is written and the only sanity check is to ensure
 * that the backing memory is not overrun.
 * \param[in] buffer
 *      Pointer to the data to be appended to the Segment's backing memory.
 * \param[in] length
 *      Length of the buffer to be appended in bytes.
 * \return
 *      A pointer into the Segment corresponding to the first byte that was
 *      copied in to.
 */
const void *
Segment::forceAppendBlob(const void *buffer, uint32_t length)
{
    assert((tail + length) <= capacity);
    assert(!closed);

    const uint8_t *src = reinterpret_cast<const uint8_t *>(buffer);
    uint8_t       *dst = reinterpret_cast<uint8_t *>(baseAddress) + tail;

    memcpy(dst, src, length);

    tail += length;
    return reinterpret_cast<void *>(dst);
}

/**
 * Append an entry of any type to the Segment. This function will always
 * succeed so long as there is sufficient room left in the tail of the Segment.
 * \param[in] type
 *      The type of entry to append.
 * \param[in] buffer
 *      Data to be appended to this Segment.
 * \param[in] length
 *      Length of the data to be appended in bytes.
 * \param[in] sync
 *      If true then this write to replicated to backups before return,
 *      otherwise the replication will happen on a subsequent append()
 *      where sync is true or when the segment is closed.  This defaults
 *      to true.
 * \param[in] updateChecksum
 *      Optional boolean to disable updates to the Segment checksum. The
 *      default is to update the running checksum while appending data, but
 *      this can be stopped when appending the SegmentFooter, for instance.
 * \param[in] expectedChecksum
 *      The expected checksum this new entry should have. If the calculated
 *      checksum does not match, an exception is thrown. This exists mainly
 *      for recovery to avoid calculating the checksum twice (once to check
 *      the recovered object, and again when adding to the log). This
 *      parameter is optional and is not normally used.
 * \return
 *      A SegmentEntryHandle corresponding to the data just written. 
 */
SegmentEntryHandle
Segment::forceAppendWithEntry(LogEntryType type, const void *buffer,
    uint32_t length, bool sync, bool updateChecksum,
    Tub<SegmentChecksum::ResultType> expectedChecksum)
{
    assert(!closed);

    uint64_t freeBytes = capacity - tail;
    uint64_t needBytes = sizeof(SegmentEntry) + length;
    if (freeBytes < needBytes)
        return NULL;

    SegmentEntry entry(type, length);

    if (updateChecksum) {
        CycleCounter<RawMetric> _(&metrics->master.segmentAppendChecksumTicks);
        SegmentChecksum entryChecksum;
        entryChecksum.update(&entry, sizeof(entry));
        entryChecksum.update(buffer, length);

        // The incoming checksum will have had the mutableFields checksum
        // XORed back out, so compare it now.
        if (expectedChecksum) {
            if (*expectedChecksum != entryChecksum.getResult()) {
                throw SegmentException(HERE, format("checksum didn't match "
                    "expected (wanted: 0x%08x, got 0x%08x)", *expectedChecksum,
                    entryChecksum.getResult()));
            }
        }

        // The Segment checksum in the footer is computed without the
        // mutableFields contents. When we check an existing Segment,
        // we back out the mutableFields from the recorded checksum
        // using XOR and recompute.
        prevChecksum = checksum;
        SegmentChecksum::ResultType r = entryChecksum.getResult();
        checksum.update(&r, sizeof(r));

        SegmentChecksum mutableFieldsChecksum;
        entry.mutableFields.segmentCapacityExponent =
            downCast<uint8_t>(ffs(capacity) - 1);
        mutableFieldsChecksum.update(&entry.mutableFields,
            sizeof(entry.mutableFields));

        // XOR the mutableFields checksum in so that we can rip it back out
        // if necessary.
        entry.checksum = entryChecksum.getResult() ^
                         mutableFieldsChecksum.getResult();
    }

    const void* entryPointer;
    {
        CycleCounter<RawMetric> _(&metrics->master.segmentAppendCopyTicks);
        entryPointer = forceAppendBlob(&entry, sizeof(entry));
        forceAppendBlob(buffer, length);
    }

    if (replicaManager && replicatedSegment) {
        // replicatedSegment can be NULL while initial opening entries for the
        // segment header are appended but before openSegment is called.
        replicatedSegment->write(tail);
        if (sync) {
            replicaManager->sync();
        }
    }

    SegmentEntryHandle handle =
        reinterpret_cast<SegmentEntryHandle>(entryPointer);

    incrementSpaceTimeSum(handle);

    // If we haven't synced it yet, we can roll it back.
    canRollBack = !sync;

    entryCountsByType[downCast<uint8_t>(type)]++;

    return handle;
}

} // namespace
