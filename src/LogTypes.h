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

#ifndef RAMCLOUD_LOGTYPES_H
#define RAMCLOUD_LOGTYPES_H

#include <functional>

#include "Common.h"

namespace RAMCloud {

/**
 * LogTime is a (Segment #, Segment Offset) tuple that represents the logical
 * time at which something was appended to the Log. It is currently only used
 * for computing table partitions.
 */
typedef std::pair<uint64_t, uint32_t> LogTime;

/**
 * Each entry in the log has an 8-bit type field. In a disaster recovery
 * situation this doesn't give us much to go on if we're trying to recover
 * segments from random memory, however, the addition of a CRC gives us a
 * reliable means of validating a suspected entry.
 */
enum LogEntryType {
    LOG_ENTRY_TYPE_UNINIT    = 0x0,
    LOG_ENTRY_TYPE_INVALID   = 'I',
    LOG_ENTRY_TYPE_SEGHEADER = 'H',
    LOG_ENTRY_TYPE_SEGFOOTER = 'F',
    LOG_ENTRY_TYPE_OBJ       = 'O',
    LOG_ENTRY_TYPE_OBJTOMB   = 'T',
    LOG_ENTRY_TYPE_LOGDIGEST = 'D'
};

} // namespace RAMCloud

namespace std {

/**
 * Specialize std::hash<LogEntryType>, which allows LogEntryType to be used as
 * keys of std::unordered_map, etc.
 */
template<>
struct hash<RAMCloud::LogEntryType> {
    size_t operator()(RAMCloud::LogEntryType type) const {
        return std::hash<uint8_t>()(static_cast<uint8_t>(type));
    }
};

} // namespace std

#endif // !RAMCLOUD_LOGTYPES_H
