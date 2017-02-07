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

#include "CoordinatorService.h"
#include "ShortMacros.h"
#include "WitnessClient.h"
#include "WitnessManager.h"

namespace RAMCloud {

/**
 * Construct a TableManager.
 *
 * \param context
 *      Overall information about the RAMCloud server.
 */
WitnessManager::WitnessManager(Context* context)
    : mutex()
    , context(context)
    , serverTracker(context)
    , scanScheduled(false)
    , idMap()
    , serviceCount()
{
}

/**
 * Destructor for WitnessManager.
 */
WitnessManager::~WitnessManager()
{
}

vector<WitnessManager::Witness>
WitnessManager::getWitness(ServerId master)
{
    Lock _(mutex);
    return idMap[master.getId()].witnesses;
}

/**
 * Perform all managing work. This method may take long time to return.
 */
void
WitnessManager::poll()
{
    Lock _(mutex);
    bool stateChanged = consumeServerTracker();
//    LOG(NOTICE, "poll() invoked. stateChanged: %d, scanScheduled: %d",
//               stateChanged, scanScheduled);
    if (stateChanged) {
        persist();
    }
    if (scanScheduled) {
        scanAndAssignWitness();
        scanScheduled = false;
    }
}

bool
WitnessManager::consumeServerTracker()
{
    bool stateChanged = false;
    ServerDetails server;
    ServerChangeEvent event;
    while (serverTracker.getChange(server, event)) {
        stateChanged = true;
        uint64_t serverId = server.serverId.getId();
        if (event == SERVER_ADDED) {
            if (server.services.has(WireFormat::MASTER_SERVICE)) {
                idMap[serverId]; // Add server to masters..
            }
            if (server.services.has(WireFormat::WITNESS_SERVICE)) {
                serviceCount[serverId]; // Add to witnesses..
            }
            scanScheduled = true;
        } else if (event == SERVER_CRASHED) {
            if (server.services.has(WireFormat::MASTER_SERVICE)) {
                assert(idMap.find(serverId) !=  idMap.end());
                idMap[serverId].crashed = true;
            }
            if (server.services.has(WireFormat::WITNESS_SERVICE)) {
                serviceCount.erase(serverId);
            }
            scanScheduled = true;
        } else if (event == SERVER_REMOVED) {
            if (server.services.has(WireFormat::MASTER_SERVICE)) {
                assert(idMap.find(serverId) !=  idMap.end());
                idMap.erase(serverId);
            }
            if (server.services.has(WireFormat::WITNESS_SERVICE)) {
                assert(serviceCount.find(serverId) == serviceCount.end());
            }
        } else {
            assert(false);
        }
    }

    return stateChanged;
}

void
WitnessManager::persist()
{
    return;
}

void
WitnessManager::scanAndAssignWitness()
{
    // Sort available witnesses in order of least burdened.
    // TODO(seojin): possibly get perf stat from master & soft by dispatch utilz
    auto cmp = [this](ServerId left, ServerId right) {
        return serviceCount[left.getId()] < serviceCount[right.getId()];
    };
    std::vector<ServerId> witnessCandidates(serviceCount.size());
    CountMap::iterator it;
    for (it = serviceCount.begin(); it != serviceCount.end(); ++it) {
        witnessCandidates.push_back(ServerId(it->first));
    }

    // Now scan entire masters in cluster & assign witnesses if necessary.
    for (IdMap::iterator it = idMap.begin(); it != idMap.end(); ++it) {
        uint64_t masterId = it->first;
        Master& master = it->second;
        if (master.crashed) {
            continue;
        }

        // If a master had been fully assigned with witnesses, we must sync
        // before assigning additional witness for that master.
        // The reason is during recovery, master will complete recovery after
        // successfully retrying from one witness. And client may have
        // succeeded witnessRecord with the crashed witness.
        bool needSyncMaster;
        if (master.initialized) {
            vector<Witness> upAndRunning;
            for (Witness& witness : master.witnesses) {
                uint64_t witnessId = witness.id.getId();
                if (serviceCount.find(witnessId) == serviceCount.end()) {
                    needSyncMaster = true;
                    LOG(NOTICE, "Detected crashed witness <%" PRIu64 "> for "
                            "master <%" PRIu64 ">", witnessId, masterId);
                } else {
                    upAndRunning.push_back(witness);
                }
            }
            if (needSyncMaster) {
                master.witnesses.swap(upAndRunning);
            }
        }

        if (needSyncMaster) {
            // TODO: ask master to sync...
        }

        sort(witnessCandidates.begin(), witnessCandidates.end(), cmp);

        // Try to assign witnesses for this master in order of current load.
        for (uint candidIndex = 0; master.witnesses.size() < witnessFactor &&
                candidIndex < witnessCandidates.size(); ++candidIndex) {
            ServerId candidate = witnessCandidates[candidIndex];
            bool validCandidate = true;

            if (candidate.getId() == masterId) {
                validCandidate = false;
            }
            for (Witness& alreadyAssigned : master.witnesses) {
                if (alreadyAssigned.id == candidate) {
                    validCandidate = false;
                    break;
                }
            }

            if (validCandidate) {
                try {
                    uint64_t basePtr = WitnessClient::witnessStart(context,
                            candidate, ServerId(masterId));
                    master.witnesses.emplace_back(candidate, basePtr);
                    serviceCount[candidate.getId()]++;
                    LOG(NOTICE, "Assigned witness <%" PRIu64 "> for master <%"
                            PRIu64 ">", candidate.getId(), masterId);
                    break;
                } catch (ServerNotUpException&) {
                    // Let it try next candidate.
                } catch (ClientException&) {
                    // Let it try next candidate.
                }
            }
        }
        if (master.witnesses.size() == witnessFactor) {
            master.initialized = true;
        }
    }
}

} // namespace RAMCloud
