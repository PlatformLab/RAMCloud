/* Copyright (c) 2011-2015 Stanford University
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
 * Construct a new LogIterator. Upon return, either the iterator refers to
 * the first log entry, or isDone() will return true. See class documentation
 * for details on how log iteration works and the responsibilities of the
 * caller.
 *
 * \param log
 *      The log to iterate over.
 */
LogIterator::LogIterator(Log& log)
    : log(log),
      segmentList(),
      currentIterator(),
      currentSegmentId(Segment::INVALID_SEGMENT_ID),
      lastSegment(NULL),
      lastSegmentLength(),
      done(false),
      headReached(false)
{
    if (log.head == NULL) {
        // Log is empty; not sure this should ever happen in practice.
        done = true;
    }
    next();
}

/**
 * Destroy the iterator.
 */
LogIterator::~LogIterator()
{
}

/**
 * Advance the iterator forward by one entry, if possible. If the previous
 * call to this method returned true for the first time, then this call
 * will record the current length of the log, and iteration will end when
 * we reach that point (i.e. new log appends will not be considered by
 * this iterator).
 */
void
LogIterator::next()
{
    if (headReached && (lastSegment == NULL)) {
        // At this point the caller should have locked out any conflicting
        // updates and given any operations in progress a chance to complete.
        // That means that all of the relevant entries are now present
        // in the log. Record the current log head position: it will
        // define the end of the iteration.
        lastSegment = log.head;
        SegmentCertificate dummy;
        lastSegmentLength = lastSegment->getAppendedLength(&dummy);

        // The current segment (which was the head at the time of the last
        // call to this method) may have grown between then and now, so
        // reset the length of currentIterator.
        currentIterator->setLimit(
                segmentList.back()->getAppendedLength(&dummy));
    }

    // First, see if there are more entries in the current segment
    // (this is the common case).
    if (currentIterator) {
        currentIterator->next();
        if (!currentIterator->isDone())
            return;
    }

    if (done) {
        return;
    }

    // We've exhausted the current segment. Now try the next one, if there
    // is one.
    if (currentIterator) {
        if (segmentList.back() == lastSegment) {
            // This is the normal place where a search ends.
            done = true;
            return;
        }
        segmentList.pop_back();
        currentIterator.destroy();
    }

    if (segmentList.size() == 0) {
        populateSegmentList(currentSegmentId + 1);
        if (segmentList.size() == 0) {
            RAMCLOUD_DIE("Ran out of segments without finding head");
        }
    }

    if (segmentList.back() == log.head) {
        headReached = true;
    }

    LogSegment* nextSegment = segmentList.back();
    currentIterator.construct(*nextSegment);
    currentSegmentId = nextSegment->id;
    if (nextSegment == lastSegment) {
        currentIterator->setLimit(lastSegmentLength);
    }
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
 * Same as getPosition, but returns Log::Reference instead.
 */
Log::Reference
LogIterator::getReference() {
    return currentIterator->getReference();
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
