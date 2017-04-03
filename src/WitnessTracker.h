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

#ifndef RAMCLOUD_WITNESSTRACKER_H
#define RAMCLOUD_WITNESSTRACKER_H

#include <stack>
#include <unordered_map>
#include "Common.h"
#include "SpinLock.h"
#include "ObjectManager.h"

namespace RAMCloud {

/**
 * Used in RAMCloud master.
 */
class WitnessTracker {
  PUBLIC:
    explicit WitnessTracker(Context* context, ObjectManager* objectManager);
    ~WitnessTracker();

    void listChanged(uint32_t newListVersion, int numWitnesses,
            WireFormat::NotifyWitnessChange::WitnessInfo witnessList[]);
    uint32_t getVersion() { return listVersion; }

    void registerRpcAndSyncBatch(bool syncEarly,
                                 uint64_t keyHash, // int16_t hashIndex ?
                                 uint64_t clientLeaseId,
                                 uint64_t rpcId,
                                 LogPosition logPos);
    void freeWitnessEntries(LogPosition& synced);

    struct GcInfo {
        GcInfo()
            : gcEntry({0, 0, 0})
            , logPos() {
        }
        WireFormat::WitnessGc::GcEntry gcEntry;
        LogPosition logPos;
    };

  PRIVATE:
    /// Shared RAMCloud information.
    Context* context;

    /**
     * Used for occasional syncChanges call.
     */
    ObjectManager* objectManager;

    // Version of witness list Master should reject client requests with
    // an old version.
    uint32_t listVersion;

    struct Witness {
        ServerId witnessId;
        uint64_t bufferBasePtr;
    };
    std::vector<Witness> witnesses;

    std::vector<GcInfo> unsyncedRpcs;

  PUBLIC:
    /**
     * SyncChanges every "syncBatchSize" unsynced updates.
     */
    static uint syncBatchSize;
//    static const uint syncBatchSize = 20;
//    static const uint syncBatchSize = 50;
//    static const uint syncBatchSize = 100;

  PRIVATE:
    /**
     * Monitor-style lock. Any operation on internal data structure should
     * hold this lock.
     */
    SpinLock mutex;
    typedef std::lock_guard<SpinLock> Lock;

    DISALLOW_COPY_AND_ASSIGN(WitnessTracker);
};

}  // namespace RAMCloud

#endif // RAMCLOUD_WITNESSTRACKER_H
