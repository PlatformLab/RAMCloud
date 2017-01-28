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
#include "MasterClient.h"
#include "ShortMacros.h"
#include "WitnessManager.h"

namespace RAMCloud {

/**
 * Construct a TableManager.
 *
 * \param context
 *      Overall information about the RAMCloud server.
 * \param serverList
 *      Used for finding appropriate server to be new witness.
 */
WitnessManager::WitnessManager(Context* context,
                               CoordinatorServerList* serverList)
    : mutex()
    , context(context)
    , serverList(serverList)
    , lastWitnessId(ServerId())
    , mastersInNeedOfWitness()
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

/**
 * Assigns witness to a new master. This function sends WitnessStart rpc to
 * the newly assigned witness machines.
 *
 * \param newMaster
 *      id for master which needs witness(es).
 */
void
WitnessManager::allocWitness(ServerId newMaster)
{
    Lock _(mutex);
    for (int witnessCount = 0; witnessCount < witnessFactor; witnessCount++) {
        ServerId candidate = witnessCandidate(newMaster);

        if (candidate == ServerId()) {
            // There is no available server to serve as a witness. Just skip.
            LOG(NOTICE, "Failed to assign witness for master <%" PRIu64 "> "
                    "during server enlistment process", newMaster.getId());
            mastersInNeedOfWitness.push_back(newMaster);
            return;
        }

        assignWitness(newMaster, candidate);
        LOG(NOTICE, "Assigned witness <%" PRIu64 "> for master <%" PRIu64 ">",
                    candidate.getId(), newMaster.getId());
//        // TODO(seojin): handle the case when we cannot assign witness now..
//        // TODO(seojin): handle exceptions...?
//        uint64_t basePtr =
//                MasterClient::witnessStart(context, candidate, newMaster);
//        if (idMap.count(newMaster.getId()) == 0) {
//            idMap[newMaster.getId()];
//        }
//        idMap.at(newMaster.getId()).emplace_back(candidate, basePtr);
    }

    if ((*serverList)[newMaster].isWitness()) {
        vector<ServerId> stillInNeed;
        for (ServerId master : mastersInNeedOfWitness) {
            assignWitness(master, newMaster);
            LOG(NOTICE, "Assigned witness <%" PRIu64 "> for master <%" PRIu64
                    ">", newMaster.getId(), master.getId());
            if (idMap[master.getId()].size() < witnessFactor) {
                stillInNeed.push_back(master);
            }
        }
        mastersInNeedOfWitness.swap(stillInNeed);
    }
}

void
WitnessManager::stopWitnesses(ServerId targetMaster, ServerId witnessId)
{
    Lock _(mutex);
    if (witnessId.isValid()) {
        vector<Witness>::iterator it = idMap.at(targetMaster.getId()).begin();
        while (it != idMap.at(targetMaster.getId()).end()) {
            if ((*it).id == witnessId) {
                // TODO(seojin): Stop witness role in server..
                //idMap.at(targetMaster.getId()).erase(it);
                return;
            }
        }
        // Throw exception?
    } else {
        // TODO(seojin): Stop witness role in server..
        //idMap.erase(targetMaster.getId());
    }
}

void
WitnessManager::freeWitnesses(ServerId targetMaster, ServerId witnessId)
{
    Lock _(mutex);
    if (witnessId.isValid()) {
        vector<Witness>::iterator it = idMap.at(targetMaster.getId()).begin();
        while (it != idMap.at(targetMaster.getId()).end()) {
            if ((*it).id == witnessId) {
                // TODO(seojin): GC witness in server..
                idMap.at(targetMaster.getId()).erase(it);
                serviceCount[witnessId.getId()]--;
                return;
            }
        }
        // Throw exception?
    } else {
        // TODO(seojin): GC witness in server..
        idMap.erase(targetMaster.getId());
    }
}

vector<WitnessManager::Witness>
WitnessManager::getWitness(ServerId master)
{
    Lock _(mutex);
    return idMap[master.getId()];
}

/**
 * Proposes a valid candidate for witness server.
 *
 * \param master
 *      A target master which will be witnessed.
 * \return
 *      A valid witness candidate (not same as master server & not duplicate).
 *      Invalid ServerId will be returned if we cannot assign witness at the
 *      moment.
 */
ServerId
WitnessManager::witnessCandidate(ServerId master)
{
    uint trial = 0;
    do {
        if (trial++ > serverList->size()) {
            return ServerId();
        }
        lastWitnessId = serverList->nextServer(lastWitnessId,
                                               {WireFormat::WITNESS_SERVICE});
        vector<Witness>& alreadyAssigned = idMap[master.getId()];
        for (uint i = 0; i < alreadyAssigned.size(); i++) {
            if (alreadyAssigned[i].id == lastWitnessId) {
                continue;
            }
        }
    } while (lastWitnessId == master || (perfectLoadBalance &&
                        serviceCount[lastWitnessId.getId()] >= witnessFactor));
    return lastWitnessId;
}

/**
 * Assign witenss to a master. Used to assign a single witness to a master.
 *
 * \param master
 *      master getting witness
 * \param witnessId
 *      witness being assigned
 */
void
WitnessManager::assignWitness(ServerId master, ServerId witnessId)
{
    // Verify this is correct assignment.
    assert(witnessId != master);
    vector<Witness>& alreadyAssigned = idMap[master.getId()];
    for (uint i = 0; i < alreadyAssigned.size(); i++) {
        if (alreadyAssigned[i].id == witnessId) {
            assert(false);
        }
    }

    // assign..
    // TODO(seojin): handle the case when we cannot assign witness now..
    // TODO(seojin): handle exceptions...?
    uint64_t basePtr =
            MasterClient::witnessStart(context, witnessId, master);
    if (idMap.count(master.getId()) == 0) {
        idMap[master.getId()];
    }
    idMap.at(master.getId()).emplace_back(witnessId, basePtr);
    serviceCount[witnessId.getId()]++;
}

} // namespace RAMCloud
