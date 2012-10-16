/* Copyright (c) 2012 Stanford University
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
 *      SegletAllocator to use to allocate memory from when constructing new log
 *      segments.
 * \param replicaManager
 *      The replica manager that will handle replication of segments this class
 *      allocates.
 */
SegmentManager::SegmentManager(Context* context,
                               const ServerConfig& config,
                               ServerId& logId,
                               SegletAllocator& allocator,
                               ReplicaManager& replicaManager)
    : context(context),
      segmentSize(config.segmentSize),
      logId(logId),
      allocator(allocator),
      replicaManager(replicaManager),
      segletsPerSegment(segmentSize / allocator.getSegletSize()),
      maxSegments(static_cast<uint32_t>(static_cast<double>(
        allocator.getTotalCount() / segletsPerSegment)
          * config.master.diskExpansionFactor)),
      segments(NULL),
      states(NULL),
      freeEmergencyHeadSlots(),
      freeSurvivorSlots(),
      freeSlots(),
      emergencyHeadSlotsReserved(2),
      survivorSlotsReserved(0),
      nextSegmentId(0),
      idToSlotMap(),
      allSegments(),
      segmentsByState(),
      lock(),
      logIteratorCount(0),
      segmentsOnDisk(0),
      segmentsOnDiskHistogram(maxSegments, 1),
      safeVersion(1),
      testing_allocSurvivorMustNotBlock(false)
{
    if ((segmentSize % allocator.getSegletSize()) != 0)
        throw SegmentManagerException(HERE, "segmentSize % segletSize != 0");

    if (config.master.diskExpansionFactor < 1.0)
        throw SegmentManagerException(HERE, "diskExpansionFactor not >= 1.0");

    if (!allocator.initializeEmergencyHeadReserve(2 * segletsPerSegment))
        throw SegmentManagerException(HERE, "must have at least two segments");

    assert(maxSegments >= (allocator.getTotalCount() / segletsPerSegment));

    segments = new Tub<LogSegment>[maxSegments];
    states = new Tub<State>[maxSegments];

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
    segmentsOnDiskHistogram.serialize(
                *m.mutable_segments_on_disk_histogram());
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
 * not do anything special.
 *
 * \param mustNotFail
 *      If true, the allocation of a new head must not fail. By specifying this,
 *      the caller may be returned an immutable (closed) head segment if memory
 *      has been exhausted.
 *
 * \return
 *      NULL if out of memory, otherwise the new head segment. If NULL is
 *      returned, then no transition took place and the previous head remains
 *      as the head of the log.
 */
LogSegment*
SegmentManager::allocHead(bool mustNotFail)
{
    Lock guard(lock);

    LogSegment* prevHead = getHeadSegment();
    LogSegment* newHead = alloc(SegletAllocator::DEFAULT, nextSegmentId);
    if (newHead == NULL) {
        /// Even if out of memory, we may need to allocate an emergency head
        /// segment to deal with replica failure or to let cleaning free
        /// resources.
        if (mustNotFail ||
          segmentsByState[FREEABLE_PENDING_DIGEST_AND_REFERENCES].size() > 0) {
            newHead = alloc(SegletAllocator::EMERGENCY_HEAD, nextSegmentId);
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
    writeSafeVersion(newHead);
    //TODO(syang0) writeWill(newHead);

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
    segmentsOnDiskHistogram.storeSample(segmentsOnDisk++);

    // Close the old head after we've opened up the new head. This ensures that
    // we always have an open segment on backups, unless of course there was a
    // coordinated failure, in which case we can unambiguously detect it.
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
 * \param replacing
 *      If memory compaction is being performed, this must point to the current
 *      segment that is being compacted. If a survivor is being allocated for
 *      disk cleaning instead, this must be NULL (the default).
 */
LogSegment*
SegmentManager::allocSurvivor(LogSegment* replacing)
{
    Tub<Lock> guard;
    LogSegment* s = NULL;

    while (1) {
        guard.construct(lock);

        uint64_t id = (replacing != NULL) ? replacing->id : nextSegmentId;
        s = alloc(SegletAllocator::CLEANER, id);
        if (s != NULL)
            break;

        if (testing_allocSurvivorMustNotBlock)
            return NULL;

        guard.destroy();
        usleep(100);
    }
    assert(guard);

    writeHeader(s);

    if (replacing != NULL) {
        // This survivor will inherit the replicatedSegment of the one it
        // replaces when memoryCleaningComplete() is invoked.
    } else {
        s->replicatedSegment = replicaManager.allocateNonHead(s->id, s);
        segmentsOnDiskHistogram.storeSample(segmentsOnDisk++);
        nextSegmentId++;
    }

    TEST_LOG("id = %lu", s->id);

    return s;
}

/**
 * Notify the segment manager when cleaning has completed. A list of cleaned
 * segments is pass in and the segment manager assumes that all survivor
 * segments allocated since the last call to this method will be added to the
 * log.
 *
 * \param clean
 *      List of segments that were cleaned and whose live objects, if any, have
 *      already been relocated to survivor segments.
 */
void
SegmentManager::cleaningComplete(LogSegmentVector& clean)
{
    Lock guard(lock);

    uint32_t segletsUsed = 0;
    uint32_t segletsFreed = 0;

    // New Segments we've added during cleaning need to wait
    // until the next head is written before they become part
    // of the Log.
    SegmentList& cleaningInto = segmentsByState[CLEANING_INTO];
    while (!cleaningInto.empty()) {
        LogSegment& s = cleaningInto.front();
        segletsUsed += s.getSegletsAllocated();
        changeState(s, CLEANABLE_PENDING_DIGEST);
    }

    // Increment the current epoch and save the last epoch any
    // RPC could have been a part of so we can store it with
    // the Segments to be freed.
    uint64_t epoch = ServerRpcPool<>::incrementCurrentEpoch() - 1;

    // Cleaned Segments must wait until the next digest has been
    // written (that is, the new Segments we cleaned into are part
    // of the Log and the cleaned ones are not) and no outstanding
    // RPCs could reference the data in those Segments before they
    // may be cleaned.
    foreach (LogSegment* s, clean) {
        segletsFreed += s->getSegletsAllocated();
        s->cleanedEpoch = epoch;
        changeState(*s, FREEABLE_PENDING_DIGEST_AND_REFERENCES);
    }

    LOG(DEBUG, "Cleaning used %u seglets to free %u seglets",
        segletsUsed, segletsFreed);
    assert(segletsUsed <= segletsFreed);
}

void
SegmentManager::memoryCleaningComplete(LogSegment* cleaned)
{
    Lock guard(lock);

    assert(segmentsByState[CLEANING_INTO].size() == 1);
    LogSegment& survivor = segmentsByState[CLEANING_INTO].front();
    changeState(survivor, NEWLY_CLEANABLE);

    uint64_t epoch = ServerRpcPool<>::incrementCurrentEpoch() - 1;
    cleaned->cleanedEpoch = epoch;
    changeState(*cleaned, FREEABLE_PENDING_REFERENCES);

    // Update the previous version's ReplicatedSegment to use the new, compacted
    // segment in the event of a backup failure.
    survivor.replicatedSegment = cleaned->replicatedSegment;
    cleaned->replicatedSegment = NULL;
    survivor.replicatedSegment->swapSegment(&survivor);

    LOG(DEBUG, "Compaction used %u seglets to free %u seglets",
        survivor.getSegletsAllocated(), cleaned->getSegletsAllocated());
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
        FREEABLE_PENDING_DIGEST_AND_REFERENCES
    };

    for (size_t i = 0; i < arrayLength(activeSegmentStates); i++) {
        foreach (LogSegment &s, segmentsByState[activeSegmentStates[i]]) {
            if (s.id >= minSegmentId)
                outList.push_back(&s);
        }
    }

    if (getHeadSegment() != NULL && getHeadSegment()->id >= minSegmentId)
        outList.push_back(getHeadSegment());
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
 * Given a slot number (an index into the segment manager's table of segments),
 * return a reference to the associated segment. This is used by clients of the
 * log to gain access to data that was previously appended. See Log::getEntry,
 * which is the primary user of this method.
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

/**
 * Return the number of free survivor segments left in the reserve. The caller
 * may subsequently call allocSurvivor() this many times with a guarantee that
 * a survivor segment will be returned.
 */
size_t
SegmentManager::getFreeSurvivorCount()
{
    Lock guard(lock);
    return allocator.getFreeCount(SegletAllocator::CLEANER) /
           (segmentSize / allocator.getSegletSize());
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
 * Write the SegmentHeader to a segment that's presumably the new head of the
 * log. This method is only really useful in allocHead() and allocSurvivor().
 *
 * \param segment
 *      Pointer to the segment the header should be written to.
 */
void
SegmentManager::writeHeader(LogSegment* segment)
{
    SegmentHeader header(*logId, segment->id, segmentSize);
    bool success = segment->append(LOG_ENTRY_TYPE_SEGHEADER,
                                   &header, sizeof(header));
    if (!success)
        throw FatalError(HERE, "Could not append segment header");
}

/**
 * Write the LogDigest to the new head of the log. This method should only be
 * called by allocHead(), since it will modify segment states in a way that
 * is not idempotent.
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
            "to segment", buffer.getTotalLength()));
    }
}

/**
 * Write the ObjectSafeVersion to the new head of the log.
 * This method should only be called by allocHead(), since it will
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
                        "to head segment", buffer.getTotalLength()));
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
    *states[s.slot] = newState;
    addToLists(s);
}

/**
 * Allocate a new segment, if possible, and set its initial state appropriately.
 *
 * \param type 
 *      Type of segment being allocated. Either it is for an emergency head,
 *      the cleaner, or a new regular head segment.
 * \param segmentId
 *      Identifier given to the segment. This is the log-unique value that will
 *      be placed in the log digest.
 * \return
 *      NULL if the allocation failed, otherwise a pointer to the newly
 *      allocated segment.
 */
LogSegment*
SegmentManager::alloc(SegletAllocator::AllocationType type,
                      uint64_t segmentId)
{
    TEST_LOG((type == SegletAllocator::CLEANER) ? "for cleaner" :
        ((type == SegletAllocator::DEFAULT) ?  "for head of log" :
        "for emergency head of log"));

    // Use this opportunity to see if we can free segments that have been
    // cleaned but were previously waiting for readers to finish.
    freeUnreferencedSegments();

    SegmentSlot slot = allocSlot(type);
    if (slot == INVALID_SEGMENT_SLOT)
        return NULL;

    uint32_t segletSize = allocator.getSegletSize();
    vector<Seglet*> seglets;

    if (!allocator.alloc(type, segmentSize / segletSize, seglets)) {
        assert(type == SegletAllocator::DEFAULT);
        freeSlot(slot, false);
        return NULL;
    }

    State state = (type == SegletAllocator::CLEANER) ? CLEANING_INTO : HEAD;
    segments[slot].construct(seglets,
                             segletSize,
                             segmentSize,
                             segmentId,
                             slot,
                             (type == SegletAllocator::EMERGENCY_HEAD));
    states[slot].construct(state);
    idToSlotMap[segmentId] = slot;

    LogSegment& s = *segments[slot];
    addToLists(s);

    return &s;
}

/**
 * Allocate an unused segment slot. Returns -1 if there is no
 * slot available.
 *
 * \param type
 *      The type of segment we're allocating a slot for. Some slots
 *      are reserved for special purposes like cleaning and emergency
 *      head segments and can only be allocated if the appropriate
 *      type is provided.
 */
SegmentSlot
SegmentManager::allocSlot(SegletAllocator::AllocationType type)
{
    vector<uint32_t>* source = NULL;

    if (type == SegletAllocator::DEFAULT)
        source = &freeSlots;
    else if (type == SegletAllocator::CLEANER)
        source = &freeSurvivorSlots;
    else if (type == SegletAllocator::EMERGENCY_HEAD)
        source = &freeEmergencyHeadSlots;

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
 * \param s
 *      The segment to be freed. The segment should either have been an
 *      emergency head segment, or have been in the FREEABLE_PENDING_REFERENCES
 *      state at the time this is called.
 */
void
SegmentManager::free(LogSegment* s)
{
    SegmentSlot slot = s->slot;
    uint64_t id = segments[slot]->id;

    freeSlot(slot, s->isEmergencyHead);

    // In-memory cleaning may have replaced this segment in the map, so do not
    // unconditionally remove it.
    if (idToSlotMap[id] == slot)
        idToSlotMap.erase(id);

    // Free any backup replicas. Segments cleaned in memory will have had their
    // replicatedSegment stolen and the pointer set to NULL on the old cleaned
    // version of segment.
    if (s->replicatedSegment != NULL) {
        s->replicatedSegment->free();
        segmentsOnDiskHistogram.storeSample(--segmentsOnDisk);
    }

    removeFromLists(*s);
    states[slot].destroy();
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
    segmentsByState[*states[s.slot]].push_back(s);
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
    State state = *states[s.slot];
    segmentsByState[state].erase(
        segmentsByState[state].iterator_to(s));
    allSegments.erase(allSegments.iterator_to(s));
}

/**
 * Scan potentially freeable segments and check whether any RPCs could still
 * refer to them. Any segments with no outstanding possible references are
 * immediately freed.
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
