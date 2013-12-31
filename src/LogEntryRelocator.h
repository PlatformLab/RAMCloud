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
#include "Log.h"

namespace RAMCloud {

/**
 * This class is used by the LogCleaner to relocate entries that are potentially
 * still alive. The cleaner constructs one of these objects for each log entry
 * that it encounters and passes it to the relocation handler registered with
 * the log (this is typically a method in MasterService). If the callee wishes
 * to keep the entry being cleaned, it must write a new copy using the given
 * relocator and update any of its references.
 *
 * Using this special relocation object, rather than just passing a segment, has
 * several benefits. First, it enforces the constraint that the callee must not
 * append anything larger than the original entry. Second, it records the
 * callee's intention, making it clear when it wanted to preserve an entry but
 * there was not enough space in the survivor segment. Third, it encapsulates
 * updating segment statistics when an append is done, as well as performance
 * metrics.
 *
 * The callee whose entry is being cleaned needs to only decide if it wants to
 * keep the entry. If not, it does nothing. If so, it calls #append to relocate
 * the entry. If the append succeeds, it calls #getNewReference to get the
 * reference to the relocated entry. If the append fails, the callee should just
 * return. The cleaner will notice the failure, allocate a new survivor segment,
 * and call back again.
 */
class LogEntryRelocator {
  public:
    LogEntryRelocator(LogSegment* segment, uint32_t maximumLength);
    bool append(LogEntryType type, Buffer& buffer);
    Log::Reference getNewReference();
    uint64_t getAppendTicks();
    bool failed();
    bool relocated();
    uint32_t getTotalBytesAppended();
    uint32_t getTimestamp();

  PRIVATE:
    /// The segment we will attempt to append to.
    LogSegment* segment;

    /// Maximum permitted append. The caller is required to append an entry
    /// no larger than the original (typically it is exactly the original).
    uint32_t maximumLength;

    /// If an append was done this reference points to it.
    Log::Reference reference;

    /// Set to true if the append operation fails. Used to notify the log
    /// cleaner that it must allocate a new survivor segment and try again.
    bool outOfSpace;

    /// Set to true if the append method was called and succeeded.
    bool didAppend;

    /// Number of cpu cycles spent in the append() routine. For log cleaner
    /// performance metrics.
    uint64_t appendTicks;

    /// If the append() method is invoked and succeeds, this saves the total
    /// number of bytes appended.
    uint32_t totalBytesAppended;
};

} // namespace

#endif // !RAMCLOUD_LOGENTRYRELOCATOR_H
