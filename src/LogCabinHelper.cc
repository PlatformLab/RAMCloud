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
 * \param expectedEntryId
 *      Makes the operation conditional on this being the ID assigned to
 *      this log entry.
 * \param message
 *      The ProtoBuf message to be serialized and appended to log.
 * \param invalidates
 *      A list of previous entries to be removed as part of this operation.
 * \return
 *      The created entry ID, or NO_ID if the condition given by expectedId
 *      failed.
 * \throw LogDisappearedException
 *      If this log no longer exists because someone deleted it.
 */
EntryId
LogCabinHelper::appendProtoBuf(
    EntryId& expectedEntryId,
    const google::protobuf::Message& message,
    const vector<EntryId>& invalidates)
{
    string data;
    message.SerializeToString(&data);
    Entry stateEntry(data.c_str(),
                     uint32_t(data.length() + 1),
                     invalidates);

    EntryId entryId = logCabinLog.append(stateEntry, expectedEntryId);

    // If the write succeeded, then increment the expectedEntryId.
    if (entryId != NO_ID)
        expectedEntryId++;

    return entryId;
}

/**
 * Invalidate entries in the log.
 * This is just a convenient short-cut to appending an Entry, for appends
 * with no data.
 * \param expectedEntryId
 *      Makes the operation conditional on this being the ID assigned to
 *      this log entry. For example, 0 would indicate the log must be empty
 *      for the operation to succeed. Use NO_ID to unconditionally append.
 * \param invalidates
 *      A list of previous entries to be removed as part of this operation.
 * \return
 *      The created entry ID, or NO_ID if the condition given by expectedId
 *      failed. There's no need to invalidate this returned ID. It is the
 *      new head of the log, so one plus this should be passed in future
 *      conditions as the expectedId argument.
 * \throw LogDisappearedException
 *      If this log no longer exists because someone deleted it.
 */
EntryId
LogCabinHelper::invalidate(
    EntryId& expectedEntryId,
    const vector<EntryId>& invalidates)
{
    EntryId entryId = logCabinLog.invalidate(invalidates, expectedEntryId);

    // If the write succeeded, then increment the expectedEntryId.
    if (entryId != NO_ID)
        expectedEntryId++;

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
 *      The entry_type of "entryRead". If the LogCabin entry does not have
 *      any data, then returns NULL.
 */
string
LogCabinHelper::getEntryType(Entry& entryRead)
{
    if (entryRead.getLength() > 0) {
        ProtoBuf::EntryType message;
        parseProtoBufFromEntry(entryRead, message);
        return message.entry_type();
    } else {
        return "";
    }
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

/**
 * Read valid entries starting from the beginning through head of the log.
 *
 * Currently, read() function in the LogCabin client API returns all the
 * entries, including the ones that were invalidated.
 * Once ongaro implements a cleaner in LogCabin, the read() in LogCabin client
 * API will return only valid entries and this function will not be needed
 * anymore.
 * This function is a temporary (and inefficient) work-around.
 *
 * \return
 *      The valid entries starting at the beginning through head of the log.
 * \throw LogDisappearedException
 *      If this log no longer exists because someone deleted it.
 */
vector<Entry>
LogCabinHelper::readValidEntries()
{
    vector<Entry> entries = logCabinLog.read(0);
    vector<EntryId> allInvalidatedEntries;

    for (auto it = entries.begin(); it != entries.end(); ++it) {
        vector<EntryId> invalidates = it->getInvalidates();
        foreach (EntryId entryId, invalidates) {
            allInvalidatedEntries.push_back(entryId);
        }
     }

    // Sort "allInvalidatedEntries" such that the entry ids are arranged
    // in descending order. Then when we actually erase entries from
    // "entries", it will happen from the end towards the beginning
    // so that deleting an entry doesn't change the position of the
    // entry to be deleted after it.
    sort(allInvalidatedEntries.begin(), allInvalidatedEntries.end(),
         std::greater<EntryId>());

    // The position of an entry in "entries" is *not* the same as its entryId.
    // So for every entry to be invalidated, I have to search through the entire
    // vector to find an entry with that id.
    foreach (EntryId entryId, allInvalidatedEntries) {
        for (auto it = entries.end(); it >= entries.begin(); --it) {
            if (it->getId() == entryId) {
                RAMCLOUD_LOG(DEBUG, "Erasing entry with id %lu", entryId);
                entries.erase(it);
            }
        }
    }

    return entries;
}

} // namespace RAMCloud
