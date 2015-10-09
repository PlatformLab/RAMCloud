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

#ifndef RAMCLOUD_LOGITERATOR_H
#define RAMCLOUD_LOGITERATOR_H

#include <stdint.h>
#include <vector>
#include "Log.h"
#include "SegmentIterator.h"

namespace RAMCloud {

/**
 * The LogIterator provides the necessary state and methods to step through an
 * entire log, entry by entry. It is intended primarily for use in tablet
 * and indexlet migration, though it may be useful for other purposes as well.
 * This class is designed to allow normal operations to operate concurrently
 * with migration up until migration reaches the head segment.
 *
 * Here are the rules that must be obeyed by LogIterator clients in order to
 * avoid conflicts with concurrent updates:
 * - After each call to next, the client must invoke onHead to see if the
 *   head segment has been reached.
 * - As soon as the head segment has been reached, the caller must do
 *   whatever is needed to ensure that no more conflicting updates occur
 *   (e.g. the tablet being migrated must be marked so that new writes
 *   are rejected). The caller must wait until any existing updates have
 *   completed.
 * - Then the caller continues iterating until it reaches the end of the
 *   iteration
 * - In the next call to next after the head is reached, LogIterator will
 *   note the current extent of the log. The iteration will continue until
 *   this point is reached. New entries added to the log after this point
 *   will not be iterated (presumably the caller has locked out any
 *   updates that are relevant to the iteration).
 * - When iteration completes, the caller releases its lock(s).
 */
class LogIterator {
  PUBLIC:
    explicit LogIterator(Log& log);
    ~LogIterator();

    void next();
    bool onHead();
    Log::Reference getReference();

    /**
     * Returns true if the iteration has completed (there are no more entries
     * to consider). A false value means that there exists a "current entry"
     * that may be examined with methods such as #getType and #getLength.
     */
    bool isDone() {
        return done;
    }

    /**
     * Used by some tests that would otherwise have to take a LogIterator
     * (which is hard to create) that instead take a SegmentIterator
     * (which is easy to create from mocked segments).
     * */
    SegmentIterator* getCurrentSegmentIterator() {
        return currentIterator.get();
    }

    LogEntryType getType();
    uint32_t getLength();
    uint32_t appendToBuffer(Buffer& buffer);
    uint32_t setBufferTo(Buffer& buffer);

  PRIVATE:
    /**
     * Comparison functor used to sort Segments by ID in descending order.
     */
    struct SegmentIdLessThan {
      public:
        bool
        operator()(const LogSegment* a, const LogSegment* b)
        {
            return a->id > b->id;
        }
    };

    void populateSegmentList(uint64_t nextSegmentId);

    /// Reference to the Log we're iterating.
    Log& log;

    /// Current list of segments to iterate over. Once exhausted, the list may
    /// need to be updated (since the log could have advanced forward).
    LogSegmentVector segmentList;

    /// SegmentIterator for the Segment we're currently iterating over.
    Tub<SegmentIterator> currentIterator;

    /// Identifier of the Segment currently being iterated over.
    uint64_t currentSegmentId;

    /// The last (highest numbered) segment we will iterate over; NULL
    /// means we haven't yet decided what that segment is.
    LogSegment* lastSegment;

    /// Don't consider entries starting at this position or later in
    /// lastSegment;
    uint32_t lastSegmentLength;

    /// True means that the iteration has completed: there are no more
    /// log entries to consider.
    bool done;

    /// A flag indicating whether we have reached the head segment while
    /// iterating.
    ///
    /// If this flag is set, it means that at some point the iterator referred
    /// to the log's head. It is possible that the head advanced to a new
    /// segment after we  reached it, in which case the iterator might not
    /// be referring to the log head anymore.
    bool headReached;

    DISALLOW_COPY_AND_ASSIGN(LogIterator);
};

} // namespace

#endif // !RAMCLOUD_LOGITERATOR_H
