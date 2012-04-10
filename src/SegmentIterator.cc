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

#include "Segment.h"
#include "SegmentIterator.h"
#include "LogTypes.h"

namespace RAMCloud {

/**
 * Statically turn prefetching on or off. Prefetching attempts to prime
 * the cache for the next log entry each time next() is called.
 */
const bool prefetching = true;

/**
 * Constructor an unusable default iterator. This exists only so that
 * mock child classes can be written.
 */
SegmentIterator::SegmentIterator()
    : baseAddress(NULL),
      segmentCapacity(0),
      id(Segment::INVALID_SEGMENT_ID),
      type(LOG_ENTRY_TYPE_INVALID),
      length(0),
      blobPtr(NULL),
      sawFooter(true),
      firstEntry(NULL),
      currentEntry(NULL)
{
}

/**
 * Construct a new SegmentIterator for the given Segment object.
 * \param[in] segment
 *      The Segment object to be iterated over.
 * \return
 *      The newly constructed SegmentIterator object.
 */
SegmentIterator::SegmentIterator(const Segment *segment)
    : baseAddress(segment->getBaseAddress()),
      segmentCapacity(segment->getCapacity()),
      id(segment->getId()),
      type(LOG_ENTRY_TYPE_INVALID),
      length(0),
      blobPtr(NULL),
      sawFooter(false),
      firstEntry(NULL),
      currentEntry(NULL)
{
    commonConstructor(false);
}

/**
 * Construct a new SegmentIterator for a piece of memory that was or is used
 * as the backing for a Segment object.
 * \param[in] buffer
 *      A pointer to the first byte of the Segment backing memory.
 * \param[in] capacity 
 *      The total capacity of the Segment in bytes.
 * \param[in] ignoreCapacityMismatch
 *      If true, do not throw an exception if the capacity passed in to the
 *      constructor does not match what the SegmentHeader claims. This is
 *      useful, for instance, with filtered recovery segments, where the
 *      header indicates the full capacity of the unfiltered segment, but the
 *      actual buffer received by the new master is shorter. SegmentIterator
 *      will still ensure that bounds are not exceeded, but the warning is
 *      suppressed.
 */
SegmentIterator::SegmentIterator(const void *buffer, uint64_t capacity,
    bool ignoreCapacityMismatch)
    : baseAddress(buffer),
      segmentCapacity(capacity),
      id(-1),
      type(LOG_ENTRY_TYPE_INVALID),
      length(0),
      blobPtr(NULL),
      sawFooter(false),
      firstEntry(NULL),
      currentEntry(NULL)
{
    commonConstructor(ignoreCapacityMismatch);
}

/**
 * This destructor only exists for testing (so mock descendent classes may be
 * written).
 */
SegmentIterator::~SegmentIterator()
{
}

/**
 * Perform initialisation operations common to all constructors. This
 * includes sanity checking and setting up the first iteration's state.
 * \param[in] ignoreCapacityMismatch
 *      See the #SegmentIterator constructor.
 */
void
SegmentIterator::commonConstructor(bool ignoreCapacityMismatch)
{
    if (segmentCapacity < (sizeof(SegmentEntry) + sizeof(SegmentHeader))) {
        throw SegmentIteratorException(HERE,
                                       "impossibly small Segment provided");
    }

    const SegmentEntry *entry = (const SegmentEntry *)baseAddress;
    if (entry->type   != LOG_ENTRY_TYPE_SEGHEADER ||
        entry->length != sizeof(SegmentHeader) ||
        !isEntryValid(entry)) {
        throw SegmentIteratorException(HERE,
                                       "no valid SegmentHeader entry found");
    }

    const SegmentHeader *header = reinterpret_cast<const SegmentHeader *>(
        reinterpret_cast<const char *>(baseAddress) + sizeof(SegmentEntry));
    if (header->segmentCapacity != segmentCapacity && !ignoreCapacityMismatch) {
        throw SegmentIteratorException(HERE,
                                       "SegmentHeader disagrees with claimed "
                                       "Segment capacity");
    }

    if (id != (uint64_t)-1 && header->segmentId != id)
        throw SegmentIteratorException(HERE, "id mismatch");
    id = header->segmentId;

    SegmentEntryHandle handle = reinterpret_cast<SegmentEntryHandle>(entry);

    type    = handle->type();
    length  = handle->length();
    blobPtr = handle->userData<char*>();

    currentEntry = firstEntry = entry;
}

/**
 * Determine if the SegmentEntry provided is valid, i.e. that the SegmentEntry
 * does not overrun or underrun the buffer.
 * \param[in] entry
 *      The entry to validate.
 * \return
 *      true if the entry is valid, false otherwise.
 */
bool
SegmentIterator::isEntryValid(const SegmentEntry *entry) const
{
    uintptr_t pastEnd       = (uintptr_t)baseAddress + segmentCapacity;
    uintptr_t entryStart    = (uintptr_t)entry;

    // this is an internal error
    assert(entryStart >= (uintptr_t)baseAddress);

    if (entryStart + sizeof(*entry) > pastEnd ||
        entryStart + sizeof(*entry) + entry->length > pastEnd) {
        return false;
    }

    return true;
}

/**
 * Test if the SegmentIterator has exhausted all entries.
 * \return
 *      true if there are no more entries left to iterate, else false.
 */
bool
SegmentIterator::isDone() const
{
    return (sawFooter || currentEntry == NULL || !isEntryValid(currentEntry));
}

/**
 * Progress the iterator to the next entry in the Segment, if there is one.
 * Future calls to #getType, #getLength, #getPointer, and #getOffset will
 * reflect the next SegmentEntry's parameters.
 */
void
SegmentIterator::next()
{
    type = LOG_ENTRY_TYPE_INVALID;
    length = 0;
    blobPtr = NULL;

    if (currentEntry == NULL)
        return;

    if (currentEntry->type == LOG_ENTRY_TYPE_SEGFOOTER) {
        sawFooter = true;
        return;
    }

    uintptr_t nextEntry = (uintptr_t)currentEntry + sizeof(*currentEntry) +
        currentEntry->length;
    const SegmentEntry *entry = (const SegmentEntry *)nextEntry;

    if (!isEntryValid(entry)) {
        currentEntry = NULL;
        return;
    }

    SegmentEntryHandle handle = reinterpret_cast<SegmentEntryHandle>(entry);

    type    = handle->type();
    length  = handle->length();
    blobPtr = handle->userData<char*>();
    currentEntry = entry;

    if (prefetching) {
        nextEntry = (uintptr_t)currentEntry + sizeof(*currentEntry) +
            currentEntry->length;
        entry = (const SegmentEntry *)nextEntry;
        prefetch(entry, 128);
    }
}

/**
 * Obtain the type of the SegmentEntry currently being iterated over.
 * \return
 *      The type of the current entry.
 * \throw SegmentIteratorException
 *      An exception is thrown if the iterator has no more entries.
 */
LogEntryType
SegmentIterator::getType() const
{
    if (currentEntry == NULL)
        throw SegmentIteratorException(HERE,
                                       "getType after iteration complete");
    return type;
}

/**
 * Obtain the length of the SegmentEntry currently being iterated over.
 * \return
 *      The length of the current entry in bytes.
 * \throw SegmentIteratorException
 *      An exception is thrown if the iterator has no more entries.
 */
uint32_t
SegmentIterator::getLength() const
{
    if (currentEntry == NULL)
        throw SegmentIteratorException(HERE,
                                       "getLength after iteration complete");
    return length;
}

/**
 * Obtain the length of the SegmentEntry currently being iterated over.
 * \return
 *      The length of the current entry in bytes.
 * \throw SegmentIteratorException
 *      An exception is thrown if the iterator has no more entries.
 */
uint32_t
SegmentIterator::getLengthInLog() const
{
    return getLength() + downCast<uint32_t>(sizeof(SegmentEntry));
}

/**
 * Obtain the LogPosition corresponding to the append of this entry.
 * \return
 *      The LogPosition corresponding to this entry's append.
 * \throw SegmentIteratorException
 *      An exception is thrown if the iterator has no more entries.
 */
LogPosition
SegmentIterator::getLogPosition() const
{
    if (currentEntry == NULL) {
        throw SegmentIteratorException(HERE,
          "getLogPosition after iteration complete");
    }
    assert(getOffset() >= sizeof(SegmentEntry));
    return LogPosition(id, getOffset() - sizeof(SegmentEntry));
}

/**
 * Obtain a SegmentEntryHandle for this iterator.
 * \return
 *      The SegmentEntryHandle corresponding to the current entry in the
 *      iteration.
 * \throw
 *      An exception is thrown if the iterator has no more entries.
 */
SegmentEntryHandle
SegmentIterator::getHandle() const
{
    if (currentEntry == NULL)
        throw SegmentIteratorException(HERE,
                                       "getHandle after iteration complete");
    return reinterpret_cast<SegmentEntryHandle>(currentEntry);
}

/**
 * Obtain a reference to the header of the Segment we're interating over.
 * This can be called regardless of the iterator's current position and
 * will always succeed.
 *
 * \return
 *      A reference to a SegmentHeader structure that describes this
 *      Segment.
 */
const SegmentHeader&
SegmentIterator::getHeader() const
{
    const SegmentHeader *header = reinterpret_cast<const SegmentHeader *>(
        reinterpret_cast<const char *>(baseAddress) + sizeof(SegmentEntry));
    return *header;
}

/**
 * Obtain a const void* to the data associated with the current SegmentEntry. 
 * \return
 *      A const void* to the current data.
 * \throw SegmentIteratorException
 *      An exception is thrown if the iterator has no more entries.
 */
const void *
SegmentIterator::getPointer() const
{
    return get<void>();
}

/**
 * Obtain the byte offset of the current SegmentEntry's data within the Segment
 * being iterated over. Note that the data offset is not the SegmentEntry
 * structure, but the typed data immediately following it.
 * \return
 *      The byte offset of the current SegmentEntry's data.
 * \throw SegmentIteratorException
 *      An exception is thrown if the iterator has no more entries.
 */
uint64_t
SegmentIterator::getOffset() const
{
    if (currentEntry == NULL)
        throw SegmentIteratorException(HERE,
                                       "getOffset after iteration complete");
    return (uintptr_t)blobPtr - (uintptr_t)baseAddress;
}

/**
 * Generate the checksum for the current entry.
 * \return
 *      The current checksum for the current entry. If the entry
 *      is corrupt, this may differ from what is stored.
 * \throw SegmentIteratorException
 *      An exception is thrown if the iterator has no more entries.
 */
SegmentChecksum::ResultType
SegmentIterator::generateChecksum() const
{
    return getHandle()->generateChecksum();
}

/**
 * Returns true if this Segment was generated by the cleaner. Otherwise returns
 * false if the Segment had been directly written to by the log (i.e. it is or
 * was at some point a head segment).
 */
bool
SegmentIterator::isCleanerSegment() const
{
    return getHeader().headSegmentIdDuringCleaning !=
        Segment::INVALID_SEGMENT_ID;
}

/**
 * Determine whether the current entry's checksum is valid or not.
 * \return
 *      true if the checksum is valid, else false.
 * \throw SegmentIteratorException
 *      An exception is thrown if the iterator has no more entries.
 */
bool
SegmentIterator::isChecksumValid() const
{
    return getHandle()->isChecksumValid();
}

/**
 * Determine whether the checksum appended to the Segment this iterator
 * is associated with is correct. If a checksum does not exist, an
 * exception is thrown.
 *
 * TODO(Rumble): This probably belongs in Segment.cc, not here.
 *
 * \return
 *      true if the check is valid, else false.
 * \throw SegmentIteratorException
 *      An exception is thrown if no checksum is present in the Segment.
 */
bool
SegmentIterator::isSegmentChecksumValid() const
{
    // find the stored checksum and calculate what it should be as we go.
    SegmentIterator i(baseAddress, segmentCapacity);
    SegmentChecksum checksum;
    while (!i.isDone()) {
        if (i.getType() == LOG_ENTRY_TYPE_INVALID) {
            i.next();
            continue;
        }
        if (i.getType() == LOG_ENTRY_TYPE_SEGFOOTER)
            break;
        SegmentChecksum::ResultType entryChecksum = i.generateChecksum();
        checksum.update(&entryChecksum, sizeof(entryChecksum));
        i.next();
    }

    if (i.isDone()) {
        throw SegmentIteratorException(HERE,
                                       "no checksum exists in the Segment");
    }

    const SegmentFooter *f =
        reinterpret_cast<const SegmentFooter *>(i.getPointer());

    return (f->checksum == checksum.getResult());
}

} // namespace
