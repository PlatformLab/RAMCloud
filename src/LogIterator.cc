/* Copyright (c) 2011-2012 Stanford University
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
#include "LogIterator.h"
#include "Segment.h"

/*
 * NOTES:
 *  - appendLock can screw with concurrent iteration of the head segment.
 *    Perhaps we want a reader-writer lock.
 */

namespace RAMCloud {

//
// PUBLIC METHODS
//

/**
 * Construct a new LogIterator on a particular Log. The iterator may be used to
 * walk the entire log in order (i.e. every entry can be iterated over in the
 * order in which it was added to the log). However, this is not be particularly
 * meaningful if cleaning is enabled, since entries could have been moved within
 * the log.
 *
 * Since additional appends to the log may occur during iteration, locks have to
 * sometimes be briefly acquired when stepping between Segments (since the list
 * of active Segments may be changing). Furthermore, once the Log head is
 * reached, it is locked to avoid any modifications during iteration and not
 * unlocked until the iterator is destroyed). Locking is not done for any of the
 * closed Segments, however, as they are immutable. Finally, although cleaning
 * may occur during iteration, any cleaned segments will have been pinned (they
 * are not permitted to be reclaimed so long as any iterator exists) so as to
 * avoid any races. This effectively delays the return of any freed memory to
 * the system until all iterators are either destroyed or have finished walking
 * the entire log. (We can relax this in the future to unpin segments after all
 * iterators have passed over them).
 *
 * Locking the head once we reach it and keeping it locked until the iterator
 * has been destroyed allows the caller to ensure that once they've iterated
 * the entire log no new modifications can be made until they decide to permit
 * them again.
 *
 * Do note that running in parallel with cleaning means that the same entry may
 * be iterated over multiple times (i.e. if the cleaner has relocated it).
 *
 * \param log
 *      The log to iterate over.
 * \param lockHead
 *      When set, we will take the log's appendLock upon reaching the head.
 */
LogIterator::LogIterator(Log& log, bool lockHead)
    : log(log),
      segmentList(),
      currentIterator(),
      currentSegmentId(Segment::INVALID_SEGMENT_ID),
      headLocked(false),
      lockHead(lockHead),
      headReached(false)
{
    log.segmentManager->logIteratorCreated();

    if (lockHead) {
        // If there's no log head yet we need to preclude any appends.
        log.appendLock.lock();
        if (log.head == NULL)
            headLocked = true;
        else
            log.appendLock.unlock();
    }
    if (log.head == NULL)
        headReached = true;

    next();
}

/**
 * Destroy the iterator. Once the last iterator on a log has been destroyed,
 * it may resume completing garbage collection. If the head segment was locked
 * for iteration, appends will also be re-enabled.
 */
LogIterator::~LogIterator()
{
    if (headLocked)
        log.appendLock.unlock();
    log.segmentManager->logIteratorDestroyed();
}

/**
 * Test whether or not the iterator has finished stepping through the entire
 * log. More concretely, if the current entry is valid, this will return false.
 * After next() has been called on the last valid entry, this will return true.
 *
 * \return
 *      True if all entries have been iterated over, else false if there are
 *      more.
 */
bool
LogIterator::isDone()
{
    if (currentIterator && !currentIterator->isDone())
        return false;

    if (segmentList.size() > 0)
        return false;

    return true;
}

/**
 * Advance the iterator forward by one entry, if possible.
 */
void
LogIterator::next()
{
    // This variable ensures that headReached is updated exactly once in this
    // function.
    bool localHeadReached = false;
    if (currentIterator && !currentIterator->isDone()) {
        currentIterator->next();
        return;
    }

    // We've exhausted the current segment. Now try the next one, if there
    // is one.

    if (currentIterator)
        currentIterator.destroy();

    if (segmentList.size() == 0)
        populateSegmentList(currentSegmentId + 1);

    if (segmentList.size() == 0)
        return;

    if (segmentList.back() == log.head) {
        if (lockHead && !headLocked) {
            log.appendLock.lock();
            headLocked = true;
        }
        localHeadReached = true;
    }

    currentIterator.construct(*segmentList.back());
    currentSegmentId = segmentList.back()->id;

    // If we lock the head and then discover that it got ahead of us, we
    // release the lock and continue iteration until we catch up to the current
    // head. This is necessary to prevent deadlock in any iteration that takes
    // a HashTableBucketLock.
    if (headLocked && log.head != segmentList.back())
    {
        // Try once more to follow the log to the current head.
        // We must unlock because operations inside the Log iteration loop may
        // require the appendLock.
        headLocked = false;
        log.appendLock.unlock();
        localHeadReached = false;
        refresh();
    } else {
        // The call to refresh() implicitly calls segmentList.pop_back(), so we
        // do not want to call it again.
        segmentList.pop_back();
    }
    headReached = localHeadReached;
}

/**
 * Test whether or not the iterator is currently on the head of the log. When
 * iterating the head all log appends are delayed until the iterator has been
 * destroyed. This method allows the caller to know when they may be seriously
 * impacting system performance.
 */
bool
LogIterator::onHead()
{
    return headReached;
}

/**
 * Get the current position in the log that we have walked to.
 */
Log::Position
LogIterator::getPosition() {
    if (currentIterator)
        return Log::Position(currentSegmentId, currentIterator->getOffset());
    else
        return Log::Position(currentSegmentId + 1, 0);
}

/**
 * Same as getPosition, but returns Log::Reference instead.
 */
Log::Reference
LogIterator::getReference() {
    return currentIterator->getReference();
}

/**
 * Set the current position, and simultaneously refresh our segments as necessary.
 * In particular, if the head segment has been updated since this iterator was
 * constructed, or since the last call to this method, we will be able to
 * iterate past the previous end of the log.
 *
 * It is assumed that the position passed in is valid for the current log.
 */
void
LogIterator::setPosition(Log::Position position) {
    if (currentIterator)
        currentIterator.destroy();

    segmentList.clear();
    populateSegmentList(position.getSegmentId());

    // This is an indication of an error because the passed in position is bad
    assert(segmentList.size() > 0);
    if (segmentList.back() == log.head) {
        if (lockHead) {
            log.appendLock.lock();
            headLocked = true;
        }
        headReached = true;
    }
    currentIterator.construct(*segmentList.back());
    currentIterator->setOffset(position.getSegmentOffset());
    currentSegmentId = segmentList.back()->id;
    segmentList.pop_back();
}

/**
 * Get the entry currently being iterated over.
 */
LogEntryType
LogIterator::getType()
{
    return currentIterator->getType();
}

/**
 * Get the length of the entry currently being iterated over.
 */
uint32_t
LogIterator::getLength()
{
    return currentIterator->getLength();
}

/**
 * Append the contents of the current entry being iterated over to the given
 * buffer.
 *
 * \param buffer
 *      Buffer to append the entry's contents to.
 */
uint32_t
LogIterator::appendToBuffer(Buffer& buffer)
{
    return currentIterator->appendToBuffer(buffer);
}

/**
 * Convenience method that resets the given buffer and appends the current
 * entry's contents to it. See appendToBuffer for more documentation.
 */
uint32_t
LogIterator::setBufferTo(Buffer& buffer)
{
    buffer.reset();
    return appendToBuffer(buffer);
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

/**
 * (Re-)Populate our internal list of active segments by querying the
 * SegmentManager for any active segments with IDs greater than or equal to the
 * given value. Our list will then be sorted such that the next ID to iterator
 * over (the lowest ID) is at the back of the list.
 *
 * \param nextSegmentId
 *      Add active segments to our list whose identifiers are greater than or
 *      equal to this value. This parameter avoids iterating over the same
 *      segment multiple times.
 */
void
LogIterator::populateSegmentList(uint64_t nextSegmentId)
{
    log.segmentManager->getActiveSegments(nextSegmentId, segmentList);

    // Sort in descending order (so we can pop oldest ones off the back).
    std::sort(segmentList.begin(),
              segmentList.end(),
              SegmentIdLessThan());
}

} // namespace RAMCloud
