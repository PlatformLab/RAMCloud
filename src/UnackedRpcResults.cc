/* Copyright (c) 2014 Stanford University
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

#include "UnackedRpcResults.h"

namespace RAMCloud {

/**
 * Default destructor
 */
UnackedRpcResults::~UnackedRpcResults()
{
    for (ClientMap::iterator it = clients.begin(); it != clients.end(); ++it) {
        Client* client = it->second;
        delete client;
    }
}

/**
 * Add a client's id and prepare the storage for tracking its unacked rpcs.
 * This function should be invoked when a client initially request
 * its registration as a user of linearizable RPCs.
 *
 * \param clientId
 *      Client's id to be registered. (Id should be > 0)
 */
void
UnackedRpcResults::addClient(uint64_t clientId)
{
    Lock lock(mutex);
    ClientMap::iterator it = clients.find(clientId);
    if (it == clients.end())
        clients[clientId] = new Client(default_rpclist_size);
}

/**
 * Check whether a particular rpc id has been received in this master
 * previously. If so, returns information about that RPC. If not, creates
 * a new record indicating that the RPC is now being processed.
 *
 * \param clientId
 *      RPC sender's id.
 * \param rpcId
 *      RPC's id to be checked.
 * \param ackId
 *      The ack number transmitted with the RPC whose id number is rpcId.
 * \param[out] result
 *      If the RPC already has been completed, the pointer to its result (the \a
 *      result value previously passed to #UnackedRpcResults::recordCompletion)
 *      will be stored here.
 *      If the RPC is still in progress or never started, NULL will be stored.
 *
 * \return
 *      True if the RPC with \a rpcId has been started previously.
 *      False if the \a rpcId has never been passed to this method
 *      for this client before.
 *
 * \throw NoClientInfo
 *      We do not have enough information to determine whether there was a
 *      duplicate RPC with same \a rpcId. Usually this happens when client
 *      is timed out in current master, and all status data are cleaned.
 * \throw StaleRpc
 *      The rpc with \a rpcId is already acknowledged by client, so we should
 *      not have received this request in the first place.
 */
bool
UnackedRpcResults::checkDuplicate(uint64_t clientId,
                                  uint64_t rpcId,
                                  uint64_t ackId,
                                  void** result)
{
    Lock lock(mutex);
    *result = NULL;
    ClientMap::iterator it = clients.find(clientId);
    if (it == clients.end()) {
        throw NoClientInfo(HERE);
    }

    Client* client = it->second;
    //1. Handle Ack.
    if (client->maxAckId < ackId)
        client->maxAckId = ackId;

    //2. Check if the same RPC has been started.
    //   There are four cases to handle.
    if (rpcId <= client->maxAckId) {
        //StaleRpc: rpc is already acknowledged.
        throw StaleRpc(HERE);
    } else if (client->maxRpcId < rpcId) {
        //Beyond the window of maxAckId and maxRpcId.
        client->maxRpcId = rpcId;
        client->recordNewRpc(rpcId);
        return false;
    } else if (client->hasRecord(rpcId)) {
        //Inside the window duplicate rpc.
        *result = client->result(rpcId);
        return true;
    } else {
        //Inside the window new rpc.
        client->recordNewRpc(rpcId);
        return false;
    }
}

/**
 * Used during recovery to figure out whether to retain a log record containing
 * the results of an RPC. This function indicates whether we can tell from the
 * information seen so far that a given RPC's result need not be retained.
 * (the client has acknowledged receiving the result.)
 *
 * \param clientId
 *      RPC sender's id.
 * \param rpcId
 *      RPC's id to be checked.
 * \param ackId
 *      The ack number transmitted with the RPC with \a rpcId.
 *
 * \return
 *      True if the log record may be useful, or
 *      false if the log record is not needed anymore.
 */
bool
UnackedRpcResults::shouldRecover(uint64_t clientId,
                                 uint64_t rpcId,
                                 uint64_t ackId)
{
    Lock lock(mutex);
    ClientMap::iterator it;
    it = clients.find(clientId);
    if (it == clients.end()) {
        clients[clientId] = new Client(default_rpclist_size);
        it = clients.find(clientId);
    }
    Client* client = it->second;
    if (client->maxAckId < ackId)
        client->maxAckId = ackId;

    return rpcId > client->maxAckId;
}

/**
 * Records the completed RPC's result.
 * After a linearizable RPC completes, this function should be invoked.
 *
 * \param clientId
 *      RPC sender's id.
 * \param rpcId
 *      RPC's id to be checked.
 * \param result
 *      The pointer to the result of rpc in log.
 */
void
UnackedRpcResults::recordCompletion(uint64_t clientId,
                                    uint64_t rpcId,
                                    void* result)
{
    Lock lock(mutex);
    ClientMap::iterator it = clients.find(clientId);
    assert(it != clients.end());
    Client* client = it->second;

    assert(client->maxAckId < rpcId);
    if (client->maxRpcId < rpcId) {
        client->maxRpcId = rpcId;
    }

    //TODO(seojin): PROBLEM. if rpc id is not in table yet during recovery?
    //Sol1. explicit call of recordNewRpc from recovery service.
    //Sol2. make a new function specifically for recovery.
    client->updateResult(rpcId, result);
}

/**
 * This method indicates whether the client has acknowledged receiving
 * the result from a given RPC. This should be used by LogCleaner to check
 * whether it should remove a log element or copy to new location.
 *
 * \param clientId
 *      RPC sender's id.
 * \param rpcId
 *      RPC's id to be checked.
 *
 * \return
 *      True if the RPC is already acknowledged and safe to clean its record.
 *      False if the RPC record is still valid and need to be kept.
 */
bool
UnackedRpcResults::isRpcAcked(uint64_t clientId, uint64_t rpcId)
{
    Lock lock(mutex);
    ClientMap::iterator it;
    it = clients.find(clientId);
    if (it == clients.end()) {
        return true;
    } else {
        Client* client = it->second;
        return client->maxAckId >= rpcId;
    }
}

/**
 * Clean up stale clients who haven't communicated long.
 */
void
UnackedRpcResults::cleanByTimeout()
{
    //TODO(seojin): implement.
}

/**
 * Check Client's internal data structure holds any record of an outstanding
 * RPC with \a rpcId.
 *
 * \param rpcId
 *      The id of Rpc.
 *
 * \return
 *      True if there is a record for rpc with rpcId. False otherwise.
 */
bool
UnackedRpcResults::Client::hasRecord(uint64_t rpcId) {
    return rpcs[rpcId % len].id == rpcId;
}

/**
 * Gets the previously recorded result of an rpc.
 *
 * \param rpcId
 *      The id of an Rpc.
 *
 * \return
 *      The pointer to the result of the RPC.
 */
void*
UnackedRpcResults::Client::result(uint64_t rpcId) {
    assert(rpcs[rpcId % len].id == rpcId);
    return rpcs[rpcId % len].result;
}

/**
 * Mark the start of processing a new RPC on Client's internal data strucutre.
 *
 * \param rpcId
 *      The id of new Rpc.
 */
void
UnackedRpcResults::Client::recordNewRpc(uint64_t rpcId) {
    if (rpcs[rpcId % len].id > maxAckId)
        resizeRpcs(10);
    rpcs[rpcId % len] = UnackedRpc(rpcId, NULL);
}

/**
 * Record result of rpc on Client's internal data structure.
 * Caller of this function must make sure #Client::recordNewRpc was called
 * with \a rpcId.
 *
 * \param rpcId
 *      The id of Rpc.
 * \param result
 *      New pointer to result of the Rpc.
 */
void
UnackedRpcResults::Client::updateResult(uint64_t rpcId, void* result) {
    assert(rpcs[rpcId % len].id == rpcId);
    rpcs[rpcId % len].result = result;
}

/**
 * Resizes Client's rpcs size. This function is called when the size of
 * Client's rpcs is not enough to keep all valid RPCs' info.
 *
 * \param increment
 *      The increment of the size of the dynamic array, #Client::rpcs
 */
void
UnackedRpcResults::Client::resizeRpcs(int increment) {
    int newLen = len + increment;
    UnackedRpc* to = new UnackedRpc[newLen](); //initialize with <0, NULL>

    for (int i = 0; i < len; ++i) {
        if (rpcs[i].id <= maxAckId)
            continue;
        to[rpcs[i].id % newLen] = rpcs[i];
    }
    delete rpcs;
    rpcs = to;
    len = newLen;
}

} // namespace RAMCloud
