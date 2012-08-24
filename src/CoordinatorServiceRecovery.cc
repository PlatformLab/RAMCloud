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

#include "CoordinatorService.h"
#include "CoordinatorServiceRecovery.h"

namespace RAMCloud {

CoordinatorServiceRecovery::CoordinatorServiceRecovery(
        CoordinatorService& coordinatorService)
    : service(coordinatorService)
{
}

CoordinatorServiceRecovery::~CoordinatorServiceRecovery()
{
}

/**
 * Replay the LogCabin log, parse the log entries to extract the states,
 * and dispatch to the appropriate recovery methods in CoordinatorServerManger.
 */
void
CoordinatorServiceRecovery::replay(bool testing)
{
    // Get all the entries appended to the log.
    // TODO(ankitak): After ongaro has added curser API to LogCabin,
    // use that to read in one entry at a time.

    // Also, since LogCabin doesn't have a log cleaner yet, a read()
    // returns all entries, including those that were invalidated.
    // Thus, use the temporary workaround function,
    // LogCabinHelper::readValidEntries() that returns only valid entries.
    vector<Entry> entriesRead = service.logCabinHelper->readValidEntries();

    for (vector<Entry>::iterator it = entriesRead.begin();
            it < entriesRead.end(); it++) {

        EntryId entryId = it->getId();
        string entryType = service.logCabinHelper->getEntryType(*it);
        RAMCLOUD_LOG(DEBUG, "%lu - %s\n", it->getId(), entryType.c_str());

        if (testing) continue;

        // Dispatch
        if (!entryType.compare("ServerEnlisting")) {

            ProtoBuf::ServerInformation state;
            service.logCabinHelper->parseProtoBufFromEntry(*it, state);
            service.serverManager.enlistServerRecover(&state, entryId);

        } else if (!entryType.compare("ServerEnlisted")) {

            ProtoBuf::ServerInformation state;
            service.logCabinHelper->parseProtoBufFromEntry(*it, state);
            service.serverManager.enlistedServerRecover(&state, entryId);

        } else if (!entryType.compare("ServerUpdate")) {

            ProtoBuf::ServerUpdate state;
            service.logCabinHelper->parseProtoBufFromEntry(*it, state);
            service.serverManager.setMinOpenSegmentIdRecover(&state, entryId);

        } else if (!entryType.compare("StateServerDown")) {

            ProtoBuf::StateServerDown state;
            service.logCabinHelper->parseProtoBufFromEntry(*it, state);
            service.serverManager.serverDownRecover(&state, entryId);

        } else {
            throw UnexpectedEntryTypeException(
                    HERE, format("Unexpected LogCabin entry type %s",
                                 entryType.c_str()));
        }
    }
}

} // namespace RAMCloud
