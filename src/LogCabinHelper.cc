/* Copyright (c) 2009-2012 Stanford University
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

#include "LogCabinHelper.h"

namespace RAMCloud {

/**
 * Append a new entry to the LogCabin.
 * The "message" is serialized to get data for this entry, and it atomically
 * removes the entries indicated in "invalidates".
 *
 * \param message
 *      The ProtoBuf message to be serialized and appended to log.
 * \param invalidates
 *      A list of previous entries to be removed as part of this operation.
 * \param expectedId
 *      Makes the operation conditional on this being the ID assigned to
 *      this log entry. For example, 0 would indicate the log must be empty
 *      for the operation to succeed. Use NO_ID to unconditionally append.
 * \return
 *      The created entry ID, or NO_ID if the condition given by expectedId
 *      failed.
 * \throw LogDisappearedException
 *      If this log no longer exists because someone deleted it.
 */
EntryId
LogCabinHelper::appendProtoBuf(
    const google::protobuf::Message& message,
    const vector<EntryId>& invalidates, EntryId expectedId)
{
    string data;
    message.SerializeToString(&data);

    Entry stateEntry(data.c_str(),
                     uint32_t(data.length() + 1),
                     invalidates);
    EntryId entryId = logCabinLog.append(stateEntry, expectedId);

    return entryId;
}

/**
 * Given a LogCabin entry that was used to stored the coordinator state,
 * get the value in the entry_type field (that indicates the type of
 * state being stored).
 *
 * \param entryRead
 *      The entry whose entry_type is to be determined.
 * \return
 *      The entry_type of "entryRead".
 */
string
LogCabinHelper::getEntryType(Entry& entryRead)
{
    ProtoBuf::EntryType message;
    parseProtoBufFromEntry(entryRead, message);
    return message.entry_type();
}

/**
 * Given a LogCabin entry, parse it into a ProtoBuf.
 *
 * \param entryRead
 *      The entry to be parsed.
 * \param message
 *      The location where the parsed entry will be stored.
 */
void
LogCabinHelper::parseProtoBufFromEntry(
    Entry& entryRead, google::protobuf::Message& message)
{
    message.ParseFromArray(entryRead.getData(),
                           entryRead.getLength());
}

} // namespace RAMCloud

