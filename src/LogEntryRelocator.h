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

#ifndef RAMCLOUD_LOGENTRYRELOCATOR_H
#define RAMCLOUD_LOGENTRYRELOCATOR_H

#include "Common.h"
#include "Buffer.h"
#include "HashTable.h"
#include "LogEntryTypes.h"
#include "LogSegment.h"

namespace RAMCloud {

/**
 * This class is used by the LogCleaner to relocate entries that are potentially
 * still alive. The cleaner constructs one of these objects and passes it to the
 * relocation handler registered with the log (this is typically a method in
 * MasterService). If the callee wishes to keep the entry being cleaned, it must
 * write a new copy using the given relocator and update any of its references.
 *
 * Using this special relocation object, rather than just passing a segment, has
 * several benefits. First, it enforces the constraint that the callee must not
 * append anything larger than the original entry. Second, it records the
 * callee's intention, making it clear when it wanted to preserve an entry but
 * there was not enough space in the survivor segment. Third, it encapsulates
 * updating segment statistics when an append is done, as well as performance
 * metrics.
 */
class LogEntryRelocator {
  public:
    LogEntryRelocator(LogSegment* segment, uint32_t maximumLength)
        : segment(segment),
          maximumLength(maximumLength),
          offset(-1),
          outOfSpace(false),
          didAppend(false),
          appendTicks(0)
    {
    }

    bool
    append(LogEntryType type, Buffer& buffer, uint32_t timestamp)
    {
        CycleCounter<uint64_t> _(&appendTicks);

        if (buffer.getTotalLength() > maximumLength)
            throw FatalError(HERE, "Relocator cannot append larger entry!");

        if (didAppend)
            throw FatalError(HERE, "Relocator may only append once!");

        if (segment == NULL) {
            outOfSpace = true;
            return false;
        }

        uint32_t priorLength = segment->getAppendedLength();
        if (!segment->append(type, buffer, offset)) {
            outOfSpace = true;
            return false;
        }
        
        uint32_t bytesUsed = segment->getAppendedLength() - priorLength;
        segment->statistics.increment(bytesUsed, timestamp);

        didAppend = true;
        return true;
    }

    HashTable::Reference
    getNewReference()
    {
        if (!didAppend)
            throw FatalError(HERE, "No append operation succeeded.");
        return HashTable::Reference((static_cast<uint64_t>(segment->slot) << 24) | offset);
    }

    uint64_t
    getAppendTicks()
    {
        return appendTicks;
    }

    bool
    failed()
    {
        return outOfSpace;
    }

  PRIVATE:
    /// The segment we will attempt to append to.
    LogSegment* segment;

    /// Maximum permitted append. The caller is required to append an entry
    /// no larger than the original (typically it is exactly the original).
    uint32_t maximumLength;

    /// If an append was done, this points to the offset of the appended
    /// entry in the segment.
    uint32_t offset;

    /// Set to true if the append operation fails. Used to notify the log
    /// cleaner that it must allocate a new survivor segment and try again.
    bool outOfSpace;

    /// Set to true if the append method was called and succeeded.
    bool didAppend;

    /// Number of cpu cycles spent in the append() routine. For log cleaner
    /// performance metrics.
    uint64_t appendTicks;
};

} // namespace

#endif // !RAMCLOUD_LOGENTRYRELOCATOR_H
