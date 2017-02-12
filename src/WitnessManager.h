/* Copyright (c) 2017 Stanford University
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

#ifndef RAMCLOUD_WITNESSMANAGER_H
#define RAMCLOUD_WITNESSMANAGER_H

#include <mutex>

#include "Common.h"
#include "ServerId.h"
#include "ServerTracker.h"

namespace RAMCloud {

/**
 * Used by the coordinator to map each master to its witnesses that temporarily
 * stores requests for that target master.
 *
 * This class is thread-safe.
 */
class WitnessManager {
  PUBLIC:
    /**
     * The following class holds information about a single index of a table.
     */
    struct Witness {
        Witness(ServerId id, uint64_t bufferBasePtr)
            : id(id)
            , bufferBasePtr(bufferBasePtr)
        {}
        ~Witness() {};

        /// ServerId of witness.
        ServerId id;

        /// Base pointer to temporary request buffer in witness server.
        uint64_t bufferBasePtr;
    };

    explicit WitnessManager(Context* context);
    ~WitnessManager();
    //TODO: option for active only
    vector<Witness> getWitness(ServerId masterId, int* listVersion = NULL);
    void poll();

    /**
     * Indicates how many witnesses are maintained per master.
     */
    static const int witnessFactor = WITNESS_PER_MASTER;

  PRIVATE:
    bool consumeServerTracker();
    void persist();
    void scanAndAssignWitness();

    /**
     * Provides monitor-style protection for all operations on the tablet map.
     * A Lock for this mutex must be held to read or modify any state in
     * the tablet map.
     */
    mutable std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

    /// Shared information about the server.
    Context* context;

    /// Used to track serverList changes.
    ServerTracker<void> serverTracker;

    bool scanScheduled;

    struct Master {
        Master()
            : crashed(false)
            , initialized(false)
            , listVersion(1)
            , witnesses()
        {}

        bool crashed;
        bool initialized;
        int listVersion;            // Version gets bumped when Witness get
                                    // deallocated. (to protect consistency)
        vector<Witness> witnesses;
    };

    /// Maps from a serialized 64-bit serverId of master to the list of
    /// witness information.
    typedef std::unordered_map<uint64_t, Master> IdMap;
    IdMap idMap;

    /// Maps from serverId of witness server to the number of masters it is
    /// watching.
    /// This list only contains "available" (up & running) witness servers.
    typedef std::unordered_map<uint64_t, int> CountMap;
    CountMap serviceCount;

    DISALLOW_COPY_AND_ASSIGN(WitnessManager);
};

} // namespace RAMCloud

#endif

