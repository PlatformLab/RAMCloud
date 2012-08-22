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

#ifndef RAMCLOUD_COORDINATORSERVICERECOVERY_H
#define RAMCLOUD_COORDINATORSERVICERECOVERY_H

#include <Client/Client.h>

#include "Common.h"
#include "CoordinatorServerManager.h"

namespace RAMCloud {

class CoordinatorService;

using LogCabin::Client::Entry;
using LogCabin::Client::EntryId;
using LogCabin::Client::NO_ID;

/**
 * Recovery module for Coordinator.
 *
 * It replays the LogCabin log, parses the log entries to extract the states,
 * and dispatches to the appropriate recovery methods in CoordinatorServerManger.
 *
 * This reconstructs the Coordinator's local state to what it was before a crash.
 * It also completes any operation that was started before the crash, and
 * for which the state was appended to the log.
 */
class CoordinatorServiceRecovery {

  PUBLIC:
    struct UnexpectedEntryTypeException : public Exception {
        explicit UnexpectedEntryTypeException(
                const CodeLocation& where, string msg)
            : Exception(where, msg) {}
    };

    explicit CoordinatorServiceRecovery(CoordinatorService& coordinatorService);
    ~CoordinatorServiceRecovery();

    void replay(bool testing = false);

  PRIVATE:

    /**
     * Reference to the coordinator service initializing this class.
     */
    CoordinatorService& service;

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServiceRecovery);
};

} // namespace RAMCloud

#endif // RAMCLOUD_COORDINATORSERVICERECOVERY_H
