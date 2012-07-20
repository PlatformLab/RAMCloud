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
#include "LogMetadata.h"
#include "ShortMacros.h"
#include "SegmentManager.h"
#include "ServerRpcPool.h"

namespace RAMCloud {

SegmentManager::SegmentManager(Context& context,
                               ServerId& logId,
                               Allocator& allocator,
                               ReplicaManager& replicaManager,
                               double diskExpansionFactor)
    : context(context),
      logId(logId),
      allocator(allocator),
      replicaManager(replicaManager),
      maxSegments(static_cast<uint32_t>(static_cast<double>(
        allocator.getFreeSegmentCount()) * diskExpansionFactor)),
      numSurvivorSegments(0),
      numSurvivorSegmentsAlloced(0),
      segments(NULL),
      states(NULL),
      freeSlots(), 
      nextSegmentId(0),
      idToSlotMap(),
      allSegments(),
      segmentsByState(),
      lock(),
      logIteratorCount(0)
{
    if (diskExpansionFactor < 1.0)
        throw SegmentManagerException(HERE, "diskExpansionFactor not >= 1.0");

    segments = new Tub<LogSegment>[maxSegments];
    states = new Tub<State>[maxSegments];

    for (uint32_t i = 0; i < maxSegments; i++)
        freeSlots.push_back(i);

    // TODO(anyone): Get this hack out of here.
    context.transportManager->registerMemory(
        const_cast<void*>(allocator.getBaseAddress()),
        allocator.getTotalBytes());
}

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

LogSegment*
SegmentManager::allocHead()
{
    Lock guard(lock);

    LogSegment* prevHead = getHeadSegment();
    LogSegment* newHead = alloc(false);
    if (newHead == NULL)
        return NULL;

    writeHeader(newHead, Segment::INVALID_SEGMENT_ID);
    writeDigest(newHead);
    //writeMaximumVersionNumber(newHead);
    //writeWill(newHead);


// XXX- replica manager open/sync hook here!

    // Only close the old head _after_ we've opened up the new head!
    if (prevHead != NULL) {
        // An exception here would be problematic.
// XXX- replica manager close hook here!
        changeState(*prevHead, NEWLY_CLEANABLE);
    }

    return newHead;
}

LogSegment*
SegmentManager::allocSurvivor()
{
    Lock guard(lock);

// XXX- writeHeader(...)
    return alloc(true);
}

void
SegmentManager::cleaningComplete(LogSegmentVector& clean)
{
    Lock guard(lock);

    // New Segments we've added during cleaning need to wait
    // until the next head is written before they become part
    // of the Log.
    SegmentList& cleaningInto = segmentsByState[CLEANING_INTO];
    while (!cleaningInto.empty()) {
        LogSegment& s = cleaningInto.front();
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
        s->cleanedEpoch = epoch;
        changeState(*s, FREEABLE_PENDING_DIGEST_AND_REFERENCES);
    }
}

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
 * Called whenever a LogIterator is created. The point is that the log
 * keeps track of when iterators exist and ensures that cleaning does
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

void
SegmentManager::getActiveSegments(uint64_t nextSegmentId, LogSegmentVector& outList)
{
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
            if (s.id >= nextSegmentId)
                outList.push_back(&s);
        }
    }

    if (getHeadSegment() != NULL && getHeadSegment()->id >= nextSegmentId)
        outList.push_back(getHeadSegment());
}

/**
 * Called by the cleaner, typically once at construction time, to specify
 * how many segments to reserve for it for cleaning. These segments will
 * not be allocated for heads of the log. The SegmentManager will ensure
 * that if the reserve is not full, any freed segments are returned to the
 * reserve, rather than used as log heads.
 */
void
SegmentManager::setSurvivorSegmentReserve(uint32_t numSegments)
{
    if (numSegments > maxSegments)
        throw SegmentManagerException(HERE, "numSegments exceeds system total");

    numSurvivorSegments = numSegments;
}

LogSegment&
SegmentManager::operator[](uint32_t slot)
{
    Lock guard(lock);
    if (slot >= maxSegments || !segments[slot])
        throw SegmentManagerException(HERE, "invalid segment slot");
    return *segments[slot];
}

bool
SegmentManager::doesIdExist(uint64_t id)
{
    Lock guard(lock);
    return contains(idToSlotMap, id);
}

uint32_t
SegmentManager::getAllocatedSegmentCount()
{
    Lock guard(lock);
    size_t total = 0;
    for (int i = 0; i < TOTAL_STATES; i++)
        total += segmentsByState[i].size();
    return downCast<uint32_t>(total);
}

uint32_t
SegmentManager::getFreeSegmentCount()
{
    Lock guard(lock);
    return allocator.getFreeSegmentCount();
}

uint32_t
SegmentManager::getMaximumSegmentCount()
{
    return maxSegments;
}

uint32_t
SegmentManager::getSegletSize()
{
    return allocator.getSegletSize();
}

uint32_t
SegmentManager::getSegmentSize()
{
    return allocator.getSegmentSize();
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

/**
 * Write the SegmentHeader to a segment that's presumably the new head of the
 * log. This method is only really useful in allocHead().
 */
void
SegmentManager::writeHeader(LogSegment* segment, uint64_t headSegmentIdDuringCleaning)
{
    SegmentHeader header(*logId,
                         segment->id,
                         allocator.getSegmentSize(),
                         headSegmentIdDuringCleaning);
    Buffer buffer;
    Buffer::Chunk::appendToBuffer(&buffer, &header, sizeof(header));
    bool success = segment->append(LOG_ENTRY_TYPE_SEGHEADER, buffer);
    if (!success)
        throw FatalError(HERE, "Could not append segment header");
}

/**
 * Write the LogDigest to the new head of the log. This method should only be
 * called by allocHead(), since it will modify segment states in a way that
 * is not idempotent.
 */
void
SegmentManager::writeDigest(LogSegment* head)
{
    // Just allocate enough stack space for the max possible number of segments.
    uint32_t digestBytes = LogDigest::getBytesFromCount(maxSegments);
    char digestBuf[digestBytes];
    LogDigest digest(maxSegments, digestBuf, digestBytes);

    // Only include new survivor segments if no log iteration in progress.
    if (logIteratorCount == 0) {
        while (!segmentsByState[CLEANABLE_PENDING_DIGEST].empty()) {
            LogSegment& s = segmentsByState[CLEANABLE_PENDING_DIGEST].front();
            changeState(s, NEWLY_CLEANABLE);
        }
    }

    foreach (LogSegment& s, segmentsByState[CLEANABLE])
        digest.addSegment(s.id);

    foreach (LogSegment& s, segmentsByState[NEWLY_CLEANABLE])
        digest.addSegment(s.id);

    digest.addSegment(head->id);

    // Only preclude/free cleaned segments if no log iteration in progress.
    if (logIteratorCount == 0) {
        while (!segmentsByState[FREEABLE_PENDING_DIGEST_AND_REFERENCES].empty()) {
            LogSegment& s = segmentsByState[FREEABLE_PENDING_DIGEST_AND_REFERENCES].front();
            changeState(s, FREEABLE_PENDING_REFERENCES);
        }
    } else {
        foreach (LogSegment& s, segmentsByState[FREEABLE_PENDING_DIGEST_AND_REFERENCES])
            digest.addSegment(s.id);
    }

    Buffer buffer;
    Buffer::Chunk::appendToBuffer(&buffer, digestBuf, digest.getBytes());
    bool success = head->append(LOG_ENTRY_TYPE_LOGDIGEST, buffer);
    if (!success) {
        throw FatalError(HERE, format("Could not append log digest of %u bytes to head segment",
            digest.getBytes()));
    }
}

LogSegment*
SegmentManager::getHeadSegment()
{
    if (segmentsByState[HEAD].size() == 0)
        return NULL;

    assert(segmentsByState[HEAD].size() == 1);
    return &segmentsByState[HEAD].front();
}

void
SegmentManager::changeState(LogSegment& s, State newState)
{
    removeFromLists(s);
    *states[s.slot] = newState; 
    addToLists(s);
}

bool
SegmentManager::mayAlloc(bool forCleaner)
{
    if (forCleaner) {
        if (numSurvivorSegmentsAlloced >= numSurvivorSegments)
            return false;
    } else {
        size_t numReserved = 0;
        if (numSurvivorSegments >= numSurvivorSegmentsAlloced)
            numReserved = numSurvivorSegments - numSurvivorSegmentsAlloced;
        if (allocator.getFreeSegmentCount() <= numReserved)
            return false;
    }

    if (allocator.getFreeSegmentCount() == 0)
        return false;

    if (freeSlots.size() == 0)
        return false;

    return true;
}

LogSegment*
SegmentManager::alloc(bool forCleaner)
{
    if (!mayAlloc(forCleaner))
        return NULL;

    uint64_t id = nextSegmentId++;
    uint32_t slot = freeSlots.back();
    freeSlots.pop_back();
    assert(!segments[slot]);
    
    State state = (forCleaner) ? CLEANING_INTO : HEAD;
    segments[slot].construct(allocator, id, slot);
    states[slot].construct(state);
    idToSlotMap[id] = slot;

    LogSegment& s = *segments[slot];
    addToLists(s);

    if (forCleaner)
        numSurvivorSegmentsAlloced++;

    return &s;
}

void
SegmentManager::free(LogSegment* s)
{
    uint32_t slot = s->slot;
    uint64_t id = segments[slot]->id;
    freeSlots.push_back(slot);
    idToSlotMap.erase(id);

    if (numSurvivorSegmentsAlloced > 0)
        numSurvivorSegmentsAlloced--;

    removeFromLists(*s);
    states[slot].destroy();
    segments[slot].destroy();
}

void
SegmentManager::addToLists(LogSegment& s)
{
    allSegments.push_back(s);
    segmentsByState[*states[s.slot]].push_back(s);
}

void
SegmentManager::removeFromLists(LogSegment& s)
{
    State state = *states[s.slot];
    segmentsByState[state].erase(
        segmentsByState[state].iterator_to(s));
    allSegments.erase(allSegments.iterator_to(s));
}

void
SegmentManager::freeUnreferencedSegments()
{
    uint64_t earliestEpoch = ServerRpcPool<>::getEarliestOutstandingEpoch(context);
    SegmentList& freeablePending = segmentsByState[FREEABLE_PENDING_REFERENCES];
    SegmentList::iterator it = freeablePending.begin();

    while (it != freeablePending.end()) {
        LogSegment& s = *it;
        ++it;
        if (s.cleanedEpoch < earliestEpoch)
            free(&s);
    }
}

} // namespace
