/* Copyright (c) 2013-2015 Stanford University
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

#include "LogEntryTypes.h"

namespace RAMCloud {

namespace LogEntryTypeHelpers {

/**
 * Return a printable string name associated with each LogEntryType.
 */
const char*
toString(LogEntryType type)
{
    switch (type) {
    case LOG_ENTRY_TYPE_INVALID:
        return "Invalid";
    case LOG_ENTRY_TYPE_SEGHEADER:
        return "Segment Header";
    case LOG_ENTRY_TYPE_OBJ:
        return "Object";
    case LOG_ENTRY_TYPE_OBJTOMB:
        return "Object Tombstone";
    case LOG_ENTRY_TYPE_LOGDIGEST:
        return "Log Digest";
    case LOG_ENTRY_TYPE_SAFEVERSION:
        return "Object Safe Version";
    case LOG_ENTRY_TYPE_TABLESTATS:
        return "Table Stats Digest";
    case LOG_ENTRY_TYPE_RPCRESULT:
        return "Linearizable Rpc Record";
    case LOG_ENTRY_TYPE_PREP:
        return "Transaction Prepare Record";
    case LOG_ENTRY_TYPE_PREPTOMB:
        return "Transaction Prepare Tombstone";
    case LOG_ENTRY_TYPE_TXDECISION:
        return "Transaction Decision Record";
    case LOG_ENTRY_TYPE_TXPLIST:
        return "Transaction Participant List Record";
    default:
        return "<<Unknown>>";
    }
}

} // namespace LogEntryTypeHelpers

} // namespace RAMCloud
