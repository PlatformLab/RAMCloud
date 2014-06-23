/* Copyright (c) 2012-2014 Stanford University
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
#include "Object.h"
#include "LogDigest.h"
#include "LogMetadata.h"
#include "ShortMacros.h"
#include "SegletAllocator.h"
#include "SegmentManager.h"
#include "ServerConfig.h"
#include "ServerRpcPool.h"
#include "TableStats.h"
#include "MasterTableMetadata.h"

namespace RAMCloud {

/**
 * Construct a new segment manager.
 *
 * \param context
 *      The RAMCloud context this will run under.
 * \param config
 *      Server runtime configuration options, including the size of each segment
 *      and the disk expansion factor.
 * \param logId
 *      Identifier of the log this object will manage. Used to stamp segments
 *      so they can be later identified on backups. Since there is one log per
 *      master server, the log id is always equal to the server id.
 * \param allocator
 *      SegletAllocator that will supply memory for new log segments.
 * \param replicaManager
 *      The replica manager that will handle replication of segments this class
 *      allocates.
 * \param masterTableMetadata
 *      MasterTableMetadata container that contains the table stats information
 *      to be serialized during log head rollover.
 */
SegmentManager::SegmentManager(Context* context,
                               const ServerConfig* config,
                               ServerId* logId,
                               SegletAllocator& allocator,
                               ReplicaManager& replicaManager,
                               MasterTableMetadata* masterTableMetadata)
    : context(context),
      segmentSize(config->segmentSize),
      logId(logId),
      allocator(allocator),
      replicaManager(replicaManager),
      masterTableMetadata(masterTableMetadata),
      segletsPerSegment(segmentSize / allocator.getSegletSize()),
      maxSegments(static_cast<uint32_t>(static_cast<double>(
        allocator.getTotalCount() / segletsPerSegment)
          * config->master.diskExpansionFactor)),
      segments(NULL),
      states(NULL),
      freeEmergencyHeadSlots(),
      freeSurvivorSlots(),
      freeSlots(),
      emergencyHeadSlotsReserved(2),
      survivorSlotsReserved(0),
      nextSegmentId(1),
      idToSlotMap(),
      allSegments(),
      segmentsByState(),
      lock("SegmentManager::lock"),
      logIteratorCount(0),
      segmentsOnDisk(0),
      segmentsOnDiskHistogram(maxSegments, 1),
      safeVersion(1)
{
    if ((segmentSize % allocator.getSegletSize()) != 0)
        throw SegmentManagerException(HERE, "segmentSize % segletSize != 0");

    if (config->master.diskExpansionFactor < 1.0)
        throw SegmentManagerException(HERE, "diskExpansionFactor not >= 1.0");

    if (!allocator.initializeEmergencyHeadReserve(2 * segletsPerSegment))
        throw SegmentManagerException(HERE, "must have at least two segments");

    assert(maxSegments >= (allocator.getTotalCount() / segletsPerSegment));

    segments = new Tub<LogSegment>[maxSegments];
    states = new State[maxSegments];

    for (uint32_t i = 0; i < emergencyHeadSlotsReserved; i++)
        freeEmergencyHeadSlots.push_back(i);
    for (uint32_t i = emergencyHeadSlotsReserved; i < maxSegments; i++)
        freeSlots.push_back(i);

    // TODO(anyone): Get this hack out of here.
    context->transportManager->registerMemory(
        const_cast<void*>(allocator.getBaseAddress()),
        allocator.getTotalBytes());
}

/**
 * Destroy the segment manager, freeing all segments that were allocated. Once
 * called, it is no longer safe to deference pointers to any segments that were
 * allocated by this class.
 */
SegmentManager::~SegmentManager()
{
    for (size_t i = 0; i < maxSegments; i++) {
        if (segments[i]) {
            removeFromLists(*segments[i]);
            segments[i].destroy();
        }
    }
    delete[] segments;
    delete[] states;
}

/**
 * Populate the given protocol buffer with various metrics about the segments
 * we're managing.
 */
void
SegmentManager::getMetrics(ProtoBuf::LogMetrics_SegmentMetrics& m)
{
    Lock guard(lock);

    segmentsOnDiskHistogram.serialize(
                *m.mutable_segments_on_disk_histogram());

    m.set_current_segments_on_disk(segmentsOnDisk);

    // Compile stats on the entries that exist in our segments (whether dead
    // or alive).
    uint64_t entryCounts[TOTAL_LOG_ENTRY_TYPES] = { 0 };
    uint64_t entryLengths[TOTAL_LOG_ENTRY_TYPES] = { 0 };
    foreach (LogSegment& s, allSegments) {
        for (int i = 0; i < TOTAL_LOG_ENTRY_TYPES; i++) {
            entryCounts[i] += s.getEntryCount(static_cast<LogEntryType>(i));
            entryLengths[i] += s.getEntryLengths(static_cast<LogEntryType>(i));
        }
    }
    for (int i = 0; i < TOTAL_LOG_ENTRY_TYPES; i++) {
        m.add_total_entry_counts(entryCounts[i]);
        m.add_total_entry_lengths(entryLengths[i]);
    }
}

/**
 * Return the allocator that is being used to provide backing memory to segments
 * this module is managing.
 */
SegletAllocator&
SegmentManager::getAllocator() const
{
    return allocator;
}

/**
 * Allocate a new segment that will serve as the head of the log. The segment
 * manager will handle the transition between the previous and next log head,
 * as well as write a segment header and the log digest. These entries will also
 * be synced to backups before the function returns. In short, the caller need
 * not do anything special to maintain log integrity.
 *
 * \param flags
 *      If out of memory and the MUST_NOT_FAIL flag is provided, an emergency
 *      head segment is allocated and returned. Otherwise, NULL is returned
 *      when out of memory and the flag is not provided.
 *
 * \return
 *      NULL if out of memory, otherwise the new head segment. If NULL is
 *      returned, then no transition took place and the previous head remains
 *      as the head of the log.
 */
LogSegment*
SegmentManager::allocHeadSegment(uint32_t flags)
{
    Lock guard(lock);

    LogSegment* prevHead = getHeadSegment();
    LogSegment* newHead = alloc(ALLOC_HEAD,
                                nextSegmentId,
                                WallTime::secondsTimestamp());
    if (newHead == NULL) {
        /// Even if out of memory, we may need to allocate an emergency head
        /// segment to let cleaning free resources. We used also to do this as
        /// part of handling segment replica failures, but that is now done
        /// entirely in the replication layer.
        if ((flags & MUST_NOT_FAIL) ||
          segmentsByState[FREEABLE_PENDING_DIGEST_AND_REFERENCES].size() > 0) {
            newHead = alloc(ALLOC_EMERGENCY_HEAD,
                            nextSegmentId,
                            WallTime::secondsTimestamp());
        } else {
            return NULL;
        }
    }

    nextSegmentId++;

    writeHeader(newHead);
    if (prevHead != NULL && !prevHead->isEmergencyHead)
        writeDigest(newHead, prevHead);
    else
        writeDigest(newHead, NULL);
    writeTableStatsDigest(newHead);
    writeSafeVersion(newHead);

    // Make the head immutable if it's an emergency head. This will prevent the
    // log from adding anything to it and let us reclaim it without cleaning
    // when the next head is allocated. Note that closure in the Segment class
    // is soft-state only and not propagated to backups. Backups close segments
    // when the ReplicatedSegment is closed.
    if (newHead->isEmergencyHead)
        newHead->close();

    ReplicatedSegment* prevReplicatedSegment = NULL;
    if (prevHead != NULL)
        prevReplicatedSegment = prevHead->replicatedSegment;

    // Allocate a new ReplicatedSegment to handle backing up the new head. This
    // call will also sync the initial data (header, digest, etc) to the needed
    // number of replicas before returning.
    newHead->replicatedSegment = replicaManager.allocateHead(
        newHead->id, newHead, prevReplicatedSegment);
    segmentsOnDiskHistogram.storeSample(++segmentsOnDisk);

    // Close the old head after we've opened up the new head. This ensures that
    // we always have an open segment on backups (unless, of course, there is a
    // coordinated failure of all replicas, in which case we can then
    // unambiguously detect the loss of data).
    if (prevHead != NULL) {
        prevHead->close();
        prevHead->replicatedSegment->close();

        if (prevHead->isEmergencyHead)
            free(prevHead);
        else
            changeState(*prevHead, NEWLY_CLEANABLE);
    }

    return newHead;
}

/**
 * Allocate a replacement segment for the cleaner to write survivor data into.
 *
 * This method is used both for allocating segments during disk cleaning, and
 * for allocating a segment during memory compaction.
 *
 * If ``replacing'' is NULL, a survivor for disk cleaning is allocated with a
 * fresh segment id. If it is non-NULL, the survivor segment assumes the
 * identity of the given segment (a compacted segment is logically the same
 * segment, just with less dead data).
 *
 * This method will never return NULL. It will, however, block forever if the
 * cleaner fails to keep its promise of not using more than it frees.
 *
 * \param flags
 *      If the FOR_CLEANING flag is provided, the allocation will be attempted
 *      from a pool specially reserved for the cleaner. If MUST_NOT_FAIL is
 *      provided, the method will block until a segment is free. Otherwise, it
 *      will return immediately with NULL if no segment is available.
 *
 * \param replacing
 *      If memory compaction is being performed, this must point to the current
 *      segment that is being compacted. The allocated segment will then be
 *      created with the same segment identifier and creation timestamp.
 *
 *      If a survivor is being allocated for disk cleaning instead, this must be
 *      NULL (the default).
 */
LogSegment*
SegmentManager::allocSideSegment(uint32_t flags, LogSegment* replacing)
{
    assert(replacing == NULL || states[replacing->slot] == CLEANABLE);

    Tub<Lock> guard;
    LogSegment* s = NULL;

    uint32_t reportSeconds = 5;
    uint64_t start = Cycles::rdtsc();
    while (1) {
        guard.construct(lock);

        uint64_t id = nextSegmentId;
        uint32_t creationTimestamp = WallTime::secondsTimestamp();
        if (replacing != NULL) {
            id = replacing->id;
            creationTimestamp = replacing->creationTimestamp;
        }

        if (flags & FOR_CLEANING)
            s = alloc(ALLOC_CLEANER_SIDELOG, id, creationTimestamp);
        else
            s = alloc(ALLOC_REGULAR_SIDELOG, id, creationTimestamp);

        if (s != NULL)
            break;

        if ((flags & MUST_NOT_FAIL) == 0)
            return NULL;

        guard.destroy();

        // This message is likely benign (it can happen if there are no writes
        // coming in to roll over the log head and free up previously-cleaned
        // disk segments. It exists, however, in case this isn't the case and
        // we've managed to deadlock.
        if (Cycles::toSeconds(Cycles::rdtsc() - start) >= reportSeconds) {
            LOG(NOTICE, "Haven't made progress in %us (likely just waiting for "
                "a write to roll the log head over, but could be deadlocked)",
                reportSeconds);
            reportSeconds *= 2;
        }

        // Reduce monitor lock contention.
        usleep(100);
    }
    assert(guard);

    writeHeader(s);

    if (replacing != NULL) {
        // This survivor will inherit the replicatedSegment of the one it
        // replaces when memoryCleaningComplete() is invoked.
    } else {
        s->replicatedSegment = replicaManager.allocateNonHead(s->id, s);
        segmentsOnDiskHistogram.storeSample(++segmentsOnDisk);
        nextSegmentId++;
    }

    TEST_LOG("id = %lu", s->id);

    return s;
}

/**
 * This method is invoked by the log cleaner when it has finished a cleaning
 * pass. A list of cleaned segments is passed in, as well as a list of new
 * survivor segments that contain the cleaned segments' live entries. These
 * new segments will be added to the log and the cleaned ones freed.
 *
 * \param clean
 *      List of segments that were cleaned and whose live objects, if any, have
 *      already been relocated to survivor segments.
 * \param survivors
 *      List of new survivor segments containing the live entries from the
 *      cleaned segments.
 */
void
SegmentManager::cleaningComplete(LogSegmentVector& clean,
                                 LogSegmentVector& survivors)
{
    Lock guard(lock);

    // Sanity check: the cleaner must not have used more seglets than it freed.
    uint32_t segletsUsed = 0;
    uint32_t segletsFreed = 0;
    foreach (LogSegment* s, survivors)
        segletsUsed += s->getSegletsAllocated();
    foreach (LogSegment* s, clean)
        segletsFreed += s->getSegletsAllocated();
    assert(segletsUsed <= segletsFreed);

    // Mark the new segments for insertion into the log when the next digest is
    // written and mark the cleaned segments for removal at the same point.
    foreach (LogSegment* s, survivors)
        injectSideSegment(s, CLEANABLE_PENDING_DIGEST, guard);
    foreach (LogSegment* s, clean)
        freeSegment(s, true, guard);

    LOG(DEBUG, "Cleaning used %u seglets to free %u seglets",
        segletsUsed, segletsFreed);
}

/**
 * This method is invoked by the log cleaner when it has finished compacting
 * a single segment in memory. The old segment will be freed and the new
 * version will take its place. The on-disk contents remain unchanged until a
 * backup failure occurs that forces a copy of the new compacted segment to be
 * replicated.
 *
 * \param oldSegment
 *      The segment that was compacted and will be superceded by newSegment.
 * \param newSegment
 *      The compacted version of oldSegment. It contains all of the same live
 *      data, but less dead data. This must be different from oldSegment.
 */
void
SegmentManager::compactionComplete(LogSegment* oldSegment,
                                   LogSegment* newSegment)
{
    Lock guard(lock);

    // Update the previous version's ReplicatedSegment to use the new, compacted
    // segment in the event of a backup failure.
    assert(newSegment->replicatedSegment == NULL);
    newSegment->replicatedSegment = oldSegment->replicatedSegment;
    oldSegment->replicatedSegment = NULL;
    newSegment->replicatedSegment->swapSegment(newSegment);

    injectSideSegment(newSegment, NEWLY_CLEANABLE, guard);
    freeSegment(oldSegment, false, guard);

    LOG(DEBUG, "Compaction used %u seglets to free %u seglets",
        newSegment->getSegletsAllocated(), oldSegment->getSegletsAllocated());
}

/**
 * Mark the given segments for inclusion into the log. They will not be made
 * part of the on-disk log until the next head segment is allocated and a
 * digest is written.
 *
 * This method is used by the SideLog class when committing appended entries.
 *
 * \param segments
 *      The segments to add to the log. They must have been allocated via the
 *      allocSideSegment method.
 */
void
SegmentManager::injectSideSegments(LogSegmentVector& segments)
{
    Lock guard(lock);
    foreach (LogSegment* segment, segments)
        injectSideSegment(segment, CLEANABLE_PENDING_DIGEST, guard);
}

/**
 * Free the given segments that were allocated in the allocSideSegment method,
 * but never added to the log (injectSideSegments was never invoked on them).
 *
 * This method is used by the SideLog class when aborting and returning all
 * previously-allocated segments since the last commit.
 *
 * \param segments
 *      The segments to free. Upon return, these segments are no longer valid.
 *      The system will reuse the memory only after any outstanding RPCs that
 *      could have referenced them have completed.
 */
void
SegmentManager::freeUnusedSideSegments(LogSegmentVector& segments)
{
    Lock guard(lock);
    foreach (LogSegment* segment, segments)
        freeSegment(segment, false, guard);
}

/**
 * Obtain a list of new segments that may now be cleaned. This is used by the
 * cleaner to learn of closed segments recently added to the log. Each call will
 * return the list of segments added since the previous call.
 *
 * \param[out] out
 *      List to append the newly cleanable segments to.
 */
void
SegmentManager::cleanableSegments(LogSegmentVector& out)
{
    Lock guard(lock);

    SegmentList& newlyCleanable = segmentsByState[NEWLY_CLEANABLE];
    while (!newlyCleanable.empty()) {
        LogSegment& s = newlyCleanable.front();
        out.push_back(&s);
        changeState(s, CLEANABLE);
    }
}

/**
 * Called whenever a LogIterator is created. The point is that the segment
 * manager keeps track of when iterators exist and ensures that cleaning does
 * not permute the log until after all iteration has completed.
 */
void
SegmentManager::logIteratorCreated()
{
    Lock guard(lock);
    logIteratorCount++;
}

/**
 * Called whenever a LogIterator is destroyed.
 */
void
SegmentManager::logIteratorDestroyed()
{
    Lock guard(lock);
    logIteratorCount--;
}

/**
 * Get a list of active segments (segments that are currently part of the log)
 * that have an identifier greater than or equal to the given value. This is
 * used by the log iterator to discover segments to iterate over.
 *
 * Note that segments are not returned in any particular order.
 *
 * \param minSegmentId
 *      Return segments with identifiers greater than or equal to this value.
 *      This allows the caller to avoid revisiting a segment it learned about
 *      previously.
 * \param[out] outList
 *      Appropriate segments, if any, are returned here.
 */
void
SegmentManager::getActiveSegments(uint64_t minSegmentId,
                                  LogSegmentVector& outList)
{
    Lock guard(lock);

    if (logIteratorCount == 0)
        throw SegmentManagerException(HERE, "cannot call outside of iteration");

    // Walk the lists to collect the closed Segments. Since the cleaner
    // is locked out of inserting survivor segments and freeing cleaned
    // ones so long as any iterators exist, we need only consider what
    // is presently part of the log.
    State activeSegmentStates[] = {
        NEWLY_CLEANABLE,
        CLEANABLE,
        FREEABLE_PENDING_DIGEST_AND_REFERENCES,
        HEAD
    };

    for (size_t i = 0; i < arrayLength(activeSegmentStates); i++) {
        foreach (LogSegment &s, segmentsByState[activeSegmentStates[i]]) {
            if (s.id >= minSegmentId)
                outList.push_back(&s);
        }
    }
}

/**
 * Called by the cleaner once at construction time to specify how many segments
 * to reserve for it for cleaning. These segments will not be allocated for
 * heads of the log. The SegmentManager will ensure that if the reserve is not
 * full, any freed segments are returned to the reserve, rather than used as log
 * heads.
 *
 * \param numSegments
 *      The total number of full segments to reserve for the cleaner.
 * \return
 *      True if the number of requested segments were reserved, false if the
 *      request was too large and the reserve was not initialized.
 */
bool
SegmentManager::initializeSurvivorReserve(uint32_t numSegments)
{
    Lock guard(lock);

    if (survivorSlotsReserved != 0)
        return false;

    if (numSegments > freeSlots.size())
        return false;

    if (!allocator.initializeCleanerReserve(numSegments * segletsPerSegment))
        return false;

    for (uint32_t i = 0; i < numSegments; i++) {
        freeSurvivorSlots.push_back(freeSlots.back());
        freeSlots.pop_back();
    }

    survivorSlotsReserved = numSegments;
    return true;
}

/**
 * Given a slot number (see the SegmentSlot documentation), return a reference
 * to the associated segment. This is used by clients of the log to gain access
 * to data that was previously appended. For instance, Log::getEntry is the
 * primary user of this method.
 *
 * \param slot
 *      Index into the array of segments specifying which segment to return.
 */
LogSegment&
SegmentManager::operator[](SegmentSlot slot)
{
    // There's no need to lock here. The 'segments' array is static and
    // we assume that callers are well-behaved and won't use an old
    // slot number. Even if we did take a lock, it still wouldn't prevent
    // them from doing what I just said.
    if (slot >= maxSegments || !segments[slot])
        throw SegmentManagerException(HERE, "invalid segment slot");
    return *segments[slot];
}

/**
 * Check if a segment with a particular id exists in the system. This is mainly
 * used in conjunction with tombstones to determine when its safe to garbage
 * collect them.
 *
 * \return
 *      True if the segment exists, otherwise false.
 */
bool
SegmentManager::doesIdExist(uint64_t id)
{
    Lock guard(lock);
    return contains(idToSlotMap, id);
}

#ifdef TESTING
/// Set to non-0 to mock the reported utilization of backup segments.
int SegmentManager::mockSegmentUtilization = 0;
#endif

/**
 * Return the percentage of segments in use. The value returned is in the range
 * [0, 100].
 *
 * The SegmentManager may allocate a limited number of segments, regardless of
 * how much server memory is allocated to each one. This limits the number of
 * backup segments and therefore the total space used on backups.
 */
int
SegmentManager::getSegmentUtilization()
{
    Lock guard(lock);

#ifdef TESTING
    if (mockSegmentUtilization)
        return mockSegmentUtilization;
#endif

    uint32_t maxUsableSegments = maxSegments -
                                 emergencyHeadSlotsReserved -
                                 survivorSlotsReserved;
    return downCast<int>(100 * (maxUsableSegments - freeSlots.size()) /
        maxUsableSegments);
}

int
SegmentManager::getMemoryUtilization()
{
    Lock guard(lock);

    size_t freeSeglets = allocator.getFreeCount(SegletAllocator::DEFAULT);
    size_t totalSeglets = allocator.getTotalCount(SegletAllocator::DEFAULT);

    // Count seglets that will soon be freed as free. This is important when
    // the server is run with a small amount of memory and these segments are
    // a non-trivial percentage of total space. If we don't include them in
    // such cases, we may run the cleaner earlier than we otherwise should and
    // can clean at substantially higher utilizations (and consequently with
    // much higher overhead).
    //
    // It's perfectly reasonable to count these as free, since they'll be
    // available shortly after the next head segment is created. The more the
    // system needs the memory, the faster the log head will roll.
    State freeableStates[2] = {
        FREEABLE_PENDING_DIGEST_AND_REFERENCES,
        FREEABLE_PENDING_REFERENCES
    };
    foreach (State state, freeableStates) {
        foreach (LogSegment& s, segmentsByState[state])
            freeSeglets += s.getSegletsAllocated();
    }

    return downCast<int>(100 * (totalSeglets - freeSeglets) / totalSeglets);
}

/**
 * Return safeVersion for newly allocated object and increment safeVersion for
 * next assignment. \see #safeVersion
 * semantics of version number and the purpose of the safeVersion.
 *
 * In a case, a object is repeatedly removed, recreated, removed, recreated.
 * safeVersion needs to be incremented in allocateVersion().
 *
 * \return
 *      The next version available from the master vector clock.
 */
uint64_t
SegmentManager::allocateVersion() {
    return safeVersion++;
}

/**
 * Ensure the safeVersion is larger than given number.
 * Return true if safeVersion is revised.
 * \param minimum
 *      The version number to be compared against safeVersion.
 * \see #safeVersion
 */
bool
SegmentManager::raiseSafeVersion(uint64_t minimum) {
    if (minimum > safeVersion) {
        safeVersion = minimum;
        return true;
    }
    return false;
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

/**
 * Mark a previously-allocated side segment (see allocSideSegment()) to be added
 * to the log when the next log digest is written. This is used to add survivor
 * segments generated by the cleaner to the log, as well as SideLog segments
 * that have been committed.
 *
 * Note that the segment given will not be part of the replicated log until the
 * next head is allocated and a digest is written out. allocHeadSegment() can be
 * called to ensure this, if necessary.
 *
 * \param segment
 *      The segment to add to the log. It must be in the SIDELOG state.
 * \param nextState
 *      The next state to place the injected segment into. This must be either
 *      CLEANABLE_PENDING_DIGEST or NEWLY_CLEANABLE. No other value is allowed.
 * \param lock
 *      This internal method assumes that the monitor lock is held. Hopefully
 *      this parameter will remind you to obey that constraint.
 */
void
SegmentManager::injectSideSegment(LogSegment* segment,
                                  State nextState,
                                  Lock& lock)
{
    assert(states[segment->slot] == SIDELOG);
    assert(nextState == CLEANABLE_PENDING_DIGEST ||
           nextState == NEWLY_CLEANABLE);

    // The segment we're adding will become part of the on-disk log
    // when the next head is written.
    changeState(*segment, nextState);
}

/**
 * Free a segment. This method will ensure that the segment given is freed
 * when it is safe to do so (for example, when no more RPCs could reference
 * data within it). Segments are freed in one of two ways: either the cleaner
 * relocates live data and frees a segment previously in the log, or an
 * instance of the SideLog class aborts and any segments that were allocated
 * to it since the last commit are freed.
 *
 * \param segment
 *      The segment to free. When this method returns, the pointer is not longer
 *      valid. The segment must be in either the CLEANABLE or SIDELOG state.
 * \param waitForDigest
 *      Indicates whether or not freeing of the segment must be delayed until
 *      the next log digest is written (so that it will not appear in any
 *      backup copy of the log). This is set for cleaned segments, but not for
 *      compacted segments or aborted SideLog segments.
 * \param lock
 *      This internal method assumes that the monitor lock is held. Hopefully
 *      this parameter will remind you to obey that constraint.
 */
void
SegmentManager::freeSegment(LogSegment* segment, bool waitForDigest, Lock& lock)
{
    assert(states[segment->slot] == CLEANABLE ||
           states[segment->slot] == SIDELOG);

    State state = (waitForDigest) ? FREEABLE_PENDING_DIGEST_AND_REFERENCES :
                                    FREEABLE_PENDING_REFERENCES;

    // Increment the current epoch and save the last epoch any
    // RPC could have been a part of so we can store it with
    // the segment to be freed.
    uint64_t epoch = ServerRpcPool<>::incrementCurrentEpoch() - 1;

    // Cleaned segments must wait until the next digest has been
    // written (that is, the new segments we cleaned into are part
    // of the Log and the cleaned ones are not) and no outstanding
    // RPCs could reference the data in those segments before they
    // may be cleaned.
    //
    // This method may also be invoked for segments that were never
    // actually part of the log (an aborted SideLog, for example).
    // The above restriction does not need to apply in that case,
    // but for simplicity we'll be conservative.
    segment->cleanedEpoch = epoch;
    changeState(*segment, state);
}

/**
 * Write the SegmentHeader to a segment that's presumably the new head of the
 * log. This method is only really useful in allocHeadSegment() and
 * allocSideSegment().
 *
 * \param segment
 *      Pointer to the segment the header should be written to.
 */
void
SegmentManager::writeHeader(LogSegment* segment)
{
    SegmentHeader header(**logId, segment->id, segmentSize);
    bool success = segment->append(LOG_ENTRY_TYPE_SEGHEADER,
                                   &header, sizeof(header));
    if (!success)
        throw FatalError(HERE, "Could not append segment header");
}

/**
 * Write the LogDigest to the new head of the log. This method should only be
 * called by allocHeadSEgment(), since it will modify segment states in a way
 * that is not idempotent.
 *
 * \param newHead
 *      Pointer to the segment the digest should be written into. Generally
 *      this is the new head segment.
 * \param prevHead
 *      Pointer to the segment that was, or will soon be, the previous head of
 *      the log. If NULL, it will not be included. This parameter exists so
 *      that the caller may exclude the previous head if it happened to be
 *      an emergency head segment that will be immediately reclaimed.
 */
void
SegmentManager::writeDigest(LogSegment* newHead, LogSegment* prevHead)
{
    LogDigest digest;

    // Only include new survivor segments if no log iteration in progress.
    if (logIteratorCount == 0) {
        while (!segmentsByState[CLEANABLE_PENDING_DIGEST].empty()) {
            LogSegment& s = segmentsByState[CLEANABLE_PENDING_DIGEST].front();
            changeState(s, NEWLY_CLEANABLE);
        }
    }

    foreach (LogSegment& s, segmentsByState[CLEANABLE])
        digest.addSegmentId(s.id);

    foreach (LogSegment& s, segmentsByState[NEWLY_CLEANABLE])
        digest.addSegmentId(s.id);

    if (prevHead != NULL)
        digest.addSegmentId(prevHead->id);

    digest.addSegmentId(newHead->id);

    // Only preclude/free cleaned segments if no log iteration in progress.
    if (logIteratorCount == 0) {
        SegmentList& list = segmentsByState[
            FREEABLE_PENDING_DIGEST_AND_REFERENCES];
        while (!list.empty()) {
            LogSegment& s = list.front();
            changeState(s, FREEABLE_PENDING_REFERENCES);
        }
    } else {
        SegmentList& list = segmentsByState[
            FREEABLE_PENDING_DIGEST_AND_REFERENCES];
        foreach (LogSegment& s, list)
            digest.addSegmentId(s.id);
    }

    Buffer buffer;
    digest.appendToBuffer(buffer);
    bool success = newHead->append(LOG_ENTRY_TYPE_LOGDIGEST, buffer);
    if (!success) {
        throw FatalError(HERE, format("Could not append log digest of %u bytes "
            "to segment", buffer.size()));
    }
}

/**
 * Write the ObjectSafeVersion to the new head of the log.
 * This method should only be called by allocHeadSegment(), since it will
 * modify segment states in a way that is not idempotent.
 *
 * \param head
 *      Pointer to the segment the ObjectSafeVersion should be written into.
 *      Generally this is the new head segment.
 */
void
SegmentManager::writeSafeVersion(LogSegment* head)
{
    // Create Object with segmentManager::safeVersion.
    ObjectSafeVersion objSafeVer(safeVersion);

    Buffer buffer;
    buffer.append(&objSafeVer, sizeof(objSafeVer));
    bool success = head->append(LOG_ENTRY_TYPE_SAFEVERSION, buffer);
    if (!success) {
        throw FatalError(HERE,
                 format("Could not append safeVersion of %u bytes "
                        "to head segment", buffer.size()));
    }
}

/**
 * Writes the table stats digest to the head segment so that it can be found
 * and used during recovery.
 *
 * \param head
 *      Pointer to the segment the TableStatsDigest should be written into.
 *      Generally this is the new head segment.
 */
void
SegmentManager::writeTableStatsDigest(LogSegment* head)
{
    Buffer buffer;
    TableStats::serialize(&buffer, masterTableMetadata);

    bool success = head->append(LOG_ENTRY_TYPE_TABLESTATS, buffer);
    if (!success) {
        throw FatalError(HERE,
                 format("Could not append TableStats of %u bytes "
                        "to head segment", buffer.size()));
    }
}

/**
 * Return a pointer to the current head segment. If there is no head segment
 * yet, NULL is returned.
 */
LogSegment*
SegmentManager::getHeadSegment()
{
    if (segmentsByState[HEAD].size() == 0)
        return NULL;

    assert(segmentsByState[HEAD].size() == 1);
    return &segmentsByState[HEAD].front();
}

/**
 * Transition a segment from one state to another. This involves changing the
 * state associated with the segment and moving it to the appropriate new list.
 *
 * \param s
 *      Segment whose state is to be changed.
 * \param newState
 *      New state of the segment.
 */
void
SegmentManager::changeState(LogSegment& s, State newState)
{
    removeFromLists(s);
    states[s.slot] = newState;
    addToLists(s);
}

/**
 * Allocate a new segment, if possible, and set its initial state appropriately.
 *
 * \param purpose
 *      Purpose for which this segment being is allocated. Either it is for an
 *      emergency head, the cleaner, or a new regular head segment.
 * \param segmentId
 *      Identifier given to the segment. This is the log-unique value that will
 *      be placed in the log digest.
 * \param creationTimestamp
 *      WallTime seconds timestamp when this segment was created. Normally set
 *      to the current time, but when segments are compacted in memory this will
 *      be set to the prior segments' timestamp.
 * \return
 *      NULL if the allocation failed, otherwise a pointer to the newly
 *      allocated segment.
 */
LogSegment*
SegmentManager::alloc(AllocPurpose purpose,
                      uint64_t segmentId,
                      uint32_t creationTimestamp)
{
    TEST_LOG("purpose: %d", static_cast<int>(purpose));

    // Use this opportunity to see if we can free segments that have been
    // cleaned but were previously waiting for readers to finish.
    freeUnreferencedSegments();

    SegmentSlot slot = allocSlot(purpose);
    if (slot == INVALID_SEGMENT_SLOT)
        return NULL;

    uint32_t segletSize = allocator.getSegletSize();
    vector<Seglet*> seglets;

    SegletAllocator::AllocationType type = SegletAllocator::DEFAULT;
    if (purpose == ALLOC_CLEANER_SIDELOG)
        type = SegletAllocator::CLEANER;
    else if (purpose == ALLOC_EMERGENCY_HEAD)
        type = SegletAllocator::EMERGENCY_HEAD;

    if (!allocator.alloc(type, segmentSize / segletSize, seglets)) {
        assert(purpose != ALLOC_EMERGENCY_HEAD);
        freeSlot(slot, false);
        return NULL;
    }

    State state = HEAD;
    if (purpose == ALLOC_REGULAR_SIDELOG || purpose == ALLOC_CLEANER_SIDELOG)
        state = SIDELOG;

    segments[slot].construct(seglets,
                             segletSize,
                             segmentSize,
                             segmentId,
                             slot,
                             creationTimestamp,
                             (purpose == ALLOC_EMERGENCY_HEAD));

    foreach (Seglet* seglet, seglets)
        allocator.setOwnerSegment(seglet, segments[slot].get());

    states[slot] = state;
    idToSlotMap[segmentId] = slot;

    LogSegment& s = *segments[slot];
    addToLists(s);

    return &s;
}

/**
 * Allocate an unused segment slot. Returns -1 if there is no
 * slot available.
 *
 * \param purpose
 *      The purpose for which the segment slot is being allocated. Some slots
 *      are reserved for special purposes like cleaning and emergency head
 *      segments and can only be allocated if the appropriate purpose is given.
 */
SegmentSlot
SegmentManager::allocSlot(AllocPurpose purpose)
{
    vector<uint32_t>* source = NULL;

    switch (purpose) {
    case ALLOC_HEAD:
    case ALLOC_REGULAR_SIDELOG:
        source = &freeSlots;
        break;
    case ALLOC_EMERGENCY_HEAD:
        source = &freeEmergencyHeadSlots;
        break;
    case ALLOC_CLEANER_SIDELOG:
        source = &freeSurvivorSlots;
        break;
    }

    if (source->empty())
        return -1;

    SegmentSlot slot = source->back();
    source->pop_back();
    return slot;
}

/**
 * Return a slot number from a previously-allocated segment to the
 * appropriate free list.
 *
 * \param slot
 *      The slot number that's now free.
 * \param wasEmergencyHead
 *      If the segment slot being freed belonged to an emergency head
 *      segment, this must be set to true so that the slot is kept in
 *      reserve for a future emergency head segment.
 */
void
SegmentManager::freeSlot(SegmentSlot slot, bool wasEmergencyHead)
{
    // TODO(Steve): Have the slot # imply whether or not it was an emergency
    //              head (for example, define the first N slots to always be
    //              emergency heads)?
    if (wasEmergencyHead)
        freeEmergencyHeadSlots.push_back(slot);
    else if (freeSurvivorSlots.size() < survivorSlotsReserved)
        freeSurvivorSlots.push_back(slot);
    else
        freeSlots.push_back(slot);
}

/**
 * Free the given segment, returning its memory to the allocator and removing
 * it from all lists. This should only be called once the segment manager
 * determines that its safe to destroy a segment and reuse the memory. This
 * means that the segment must no longer part of the log and no references to
 * it may still exist.
 *
 * This method is invoked by us (SegmentManager) once cleaned/unneeded segments
 * are no longer being accessed by any part of the system.
 *
 * \param s
 *      The segment to be freed. The segment should either have been an
 *      emergency head segment, or have been in the FREEABLE_PENDING_REFERENCES
 *      state at the time this is called.
 */
void
SegmentManager::free(LogSegment* s)
{
    SegmentSlot slot = s->slot;
    assert(segments[slot].get() == s);

    freeSlot(slot, s->isEmergencyHead);

    // In-memory cleaning may have replaced this segment in the map, so do not
    // unconditionally remove it.
    if (idToSlotMap[s->id] == slot)
        idToSlotMap.erase(s->id);

    // Free any backup replicas. Segments cleaned in memory will have had their
    // replicatedSegment stolen and the pointer set to NULL on the old cleaned
    // version of segment.
    if (s->replicatedSegment != NULL) {
        s->replicatedSegment->free();
        segmentsOnDiskHistogram.storeSample(--segmentsOnDisk);
    }

    removeFromLists(*s);
    states[slot] = INVALID_STATE;
    segments[slot].destroy();
}

/**
 * Add the given segment to the lists appropriate for its current state. This
 * assumes the state field has already been assigned in the 'states' array
 * associated with the segment's slot. The segment must also not be on any
 * list already.
 *
 * \param s
 *      Segment to be added to appropriate lists.
 */
void
SegmentManager::addToLists(LogSegment& s)
{
    allSegments.push_back(s);
    segmentsByState[states[s.slot]].push_back(s);
}

/**
 * Remove the given segment from all lists.
 *
 * \param s
 *      The segment being removed from lists.
 */
void
SegmentManager::removeFromLists(LogSegment& s)
{
    State state = states[s.slot];
    segmentsByState[state].erase(
        segmentsByState[state].iterator_to(s));
    allSegments.erase(allSegments.iterator_to(s));
}

/**
 * Scan potentially freeable segments and check whether any RPCs could still
 * refer to them. Any segments with no outstanding possible references are
 * immediately freed.
 *
 * This method is lazily invoked when new segments are allocated.
 */
void
SegmentManager::freeUnreferencedSegments()
{
    SegmentList& freeablePending = segmentsByState[FREEABLE_PENDING_REFERENCES];
    if (freeablePending.empty())
        return;

    uint64_t earliestEpoch =
        ServerRpcPool<>::getEarliestOutstandingEpoch(context);
    SegmentList::iterator it = freeablePending.begin();

    while (it != freeablePending.end()) {
        LogSegment& s = *it;
        ++it;
        if (s.cleanedEpoch < earliestEpoch)
            free(&s);
    }
}

} // namespace
