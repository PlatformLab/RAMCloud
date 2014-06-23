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
#include "LogEntryRelocator.h"

namespace RAMCloud {

/**
 * Construct a new LogEntryRelocator.
 *
 * \param segment
 *      The survivor segment that will have the entry appended to it if it
 *      needs to be relocated. This may also be NULL, in which case an attempt
 *      to append will simply fail.
 * \param maximumLength
 *      Maximum length of the entry that can be appended during relocation.
 *      This ensures that the callee does not write more data than the cleaner
 *      expects (it should always write an entry that is at most as large as
 *      the one being relocated; typically it is exactly the entry being
 *      relocated).
 */
LogEntryRelocator::LogEntryRelocator(LogSegment* segment,
                                     uint32_t maximumLength)
    : segment(segment),
      maximumLength(maximumLength),
      reference(),
      outOfSpace(false),
      didAppend(false),
      appendTicks(0),
      totalBytesAppended(0)
{
}

/**
 * Append the given entry to a survivor segment. This is used to relocate an
 * entry that is still alive. Only one call to append is permitted and the
 * entry being appended must be smaller than the maximum value indicated in
 * the constructor.
 *
 * \param type
 *      Type of the entry being appended.
 * \param buffer
 *      The entry to append.
 * \return
 *      Returns true if the append succeeded. Otherwise, returns false if there
 *      was not sufficient space. The relocator will record this failure so that
 *      another attempt will be made. The caller of this method should simply
 *      ignore the failure, as the cleaner will guarantee that the current entry
 *      stays valid until another relocation callback is fired.
 */
bool
LogEntryRelocator::append(LogEntryType type, Buffer& buffer)
{
    CycleCounter<uint64_t> _(&appendTicks);

    if (buffer.size() > maximumLength)
        throw FatalError(HERE, "Relocator cannot append larger entry!");

    if (didAppend)
        throw FatalError(HERE, "Relocator may only append once!");

    if (segment == NULL) {
        outOfSpace = true;
        return false;
    }

    uint32_t priorLength = segment->getAppendedLength();
    if (!segment->append(type, buffer, &reference)) {
        outOfSpace = true;
        return false;
    }

    totalBytesAppended = segment->getAppendedLength() - priorLength;

    didAppend = true;
    return true;
}

/**
 * If an append operation succeeded, return the log reference to the new entry's
 * location.
 */
Log::Reference
LogEntryRelocator::getNewReference()
{
    if (!didAppend)
        throw FatalError(HERE, "No append operation succeeded.");
    return reference;
}

/**
 * Return the number of cpu ticks spent appending to a survivor segment during
 * relocation.
 */
uint64_t
LogEntryRelocator::getAppendTicks()
{
    return appendTicks;
}

/**
 * Returns true if an append was attempted, but failed due to insufficient
 * space. Used by the cleaner to determine when it needs to allocate a new
 * survivor segment and retry the relocation. Otherwise, returns false.
 */
bool
LogEntryRelocator::failed()
{
    return outOfSpace;
}

/**
 * Returns true if the entry was relocated successfully.
 */
bool
LogEntryRelocator::relocated()
{
    return didAppend;
}

/**
 * Returns the total number of bytes appended, including any log metadata
 * overheads.
 */
uint32_t
LogEntryRelocator::getTotalBytesAppended()
{
    return totalBytesAppended;
}

} // end RAMCloud
