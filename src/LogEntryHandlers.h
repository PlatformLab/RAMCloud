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

#ifndef RAMCLOUD_LOGENTRYHANDLERS_H
#define RAMCLOUD_LOGENTRYHANDLERS_H

#include "AbstractLog.h"
#include "Buffer.h"
#include "HashTable.h"
#include "LogEntryTypes.h"

namespace RAMCloud {

// Forward declare around header pain.
class LogEntryRelocator;

/**
 * This class specifies an interface that must be implemented for handling
 * various callbacks on entries appended to the log. An instance of a class
 * implementing this interface is provided to the log constructor.
 */
class LogEntryHandlers {
  public:
    virtual ~LogEntryHandlers() { }

    /**
     * This method extracts a uint32_t timestamp from the given entry.
     * If the entry does not support a timestamp, 0 should be returned.
     */
    virtual uint32_t getTimestamp(LogEntryType type, Buffer& buffer) = 0;

    /**
     * This method is called for each entry the encountered in segments
     * being cleaned. If the caller wants to retain the data, it should
     * use the relocator to store the entry in a new location and obtain
     * the new reference from it.
     *
     * The relocator may fail (due to temporarily running out of space)
     * in that case, the caller should not update any internal state and
     * return immediately. The cleaner will simply try to relocate the
     * entry again.
     *
     * After returning from this method the old entry may disappear at any
     * future time. References should have already been purged.
     */
    virtual void relocate(LogEntryType type,
                          Buffer& oldBuffer,
                          AbstractLog::Reference oldReference,
                          LogEntryRelocator& relocator) = 0;
};

} // namespace

#endif // !RAMCLOUD_LOGENTRYHANDLERS_H
