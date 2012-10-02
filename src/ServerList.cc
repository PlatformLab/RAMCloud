/* Copyright (c) 2011-2012 Stanford University
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

#include <unordered_set>

#include "Common.h"
#include "ServerList.h"
#include "ServerTracker.h"
#include "ShortMacros.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Constructor for ServerList.

 * \param context
 *      Overall information about the RAMCloud server.  The constructor
 *      will modify context so that its serverList member refers to this
 *      object.
 */
ServerList::ServerList(Context* context)
    : AbstractServerList(context)
    , serverList()
{
}

/**
 * Destructor for ServerList.
 */
ServerList::~ServerList()
{
}

//////////////////////////////////////////////////////////////////////
// ServerList - Protected Methods (inherited from AbstractServerList)
//////////////////////////////////////////////////////////////////////
ServerDetails*
ServerList::iget(ServerId id)
{
    uint32_t index = id.indexNumber();
    if ((index < serverList.size()) && serverList[index]) {
        ServerDetails* details = serverList[index].get();
        if (details->serverId == id)
            return details;
    }
    return NULL;
}

ServerDetails*
ServerList::iget(uint32_t index)
{
    return (serverList[index]) ? serverList[index].get() : NULL;
}

/**
 * Return the number of valid indexes in this list w/o lock. Valid does not mean
 * that they're occupied, only that they are within the bounds of the array.
 */
size_t
ServerList::isize() const
{
    return serverList.size();
}


//////////////////////////////////////////////////////////////////////
// ServerList Public Methods
//////////////////////////////////////////////////////////////////////
/**
 * Return the ServerId associated with a given index. If there is none,
 * an invalid ServerId is returned (i.e. the isValid() method will return
 * false.
 */
ServerId
ServerList::operator[](uint32_t index)
{
    Lock lock(mutex);
    if (index >= serverList.size() || !serverList[index])
        return ServerId(/* invalid id */);
    return serverList[index]->serverId;
}

/**
 * Apply a server list from the coordinator to the local server list so they
 * are consistent. In addition, all registered trackers will receive
 * notification of all events related to servers they are aware of along with
 * notifications for any new servers in the updated list.
 *
 * Outdated/repeated updates are ignored.
 *
 * The safety of this method relies heavily on invariants placed on the
 * ordering of updates from the coordinator as well as the ordering of the
 * entries in the updates themselves.
 *
 * The coordinator must (and should already) ensure:
 * 1) A full list is only sent to a server when it has received no other
 *    server list update yet. It is ok for a full list update to be resent
 *    due to rpc retries, but once it has been applied on a server no other
 *    full lists may be sent.
 * 2) Updates must not be sent to this server out-of-order. If the
 *    coordinator cannot confirm that an update has been applied it has no
 *    other option than to retry the update until acknowledgement.
 * 3) For enlisting server that "replace" and old server in the cluster
 *    (that is, a server re-enlisting with backup data written by a former
 *    cluster member process) the order the CRASHED/DOWN event for the
 *    replaced server versus the UP event can affect correctness. Without
 *    care restarting backups may inadverently discard important segment
 *    replicas. The ordering is upheld by the current CoordinatorServerList
 *    implementation in two ways:
 *    a) Update lists are only single-entry only and dispatched in the order
 *       they were generated. The coordinator code is ordered carefully to
 *       ensure the DOWN/CRASHED for the old server precedes the UP of the
 *       enlisting server.
 *    b) Full lists are the only multi-entry lists; the entries are in the
 *       order they appear in the coordinator server list structure. Because
 *       "slots" in that structure are reused it is possible that an
 *       UP for the enlisting server will appear in the full list before the
 *       CRASHED/DOWN for the backup it replaces. *However*, this is still
 *       safe. The newly enlisting server receiving the full list cannot
 *       have ever created a replica on the replaced server. Proof: the
 *       first population happened from this full list (which is the only
 *       full list it will ever receive) and the replaced server was
 *       either CRASHED or DOWN: statuses which aren't eligible for backup
 *       use. This means no restarted process part of that full list can ever
 *       find a replica generated by this master. Because the only threat
 *       to safety comes when backup generate isReplicaNeeded rpcs to masters
 *       that generated replicas they found on storage after restart the
 *       natural full list ordering is safe.
 * 4) That all servers are updated to CRASHED state before they are updated
 *    to DOWN. This ensures that trackers see UP -> CRASHED -> DOWN for
 *    each server and simplifies the use of trackers. (Trackers may see
 *    CRASHED -> DOWN only for servers which were initially CRASHED in
 *    the full list and they may see multiple UPs; users of trackers must take
 *    that into account.)
 *
 * \param list
 *      A complete snapshot of the coordinator's server list.
 */
void
ServerList::applyServerList(const ProtoBuf::ServerList& list)
{
    // Ignore older updates
    if (list.version_number() <= version) {
        LOG(NOTICE, "A repeated/old update version %lu was sent to "
                "a ServerList with version %lu.",
                list.version_number(), version);
        return;
    }

    if (list.type() == ProtoBuf::ServerList::FULL_LIST && version) {
        LOG(ERROR, "Coordinator sent a full list to a server whose server"
            "list was already populated. This is a bug and should never"
            "happen unless the coordinator code is busted.");
        return;
    }

    LOG(NOTICE, "Server List from coordinator:\n%s",
                list.DebugString().c_str());

    foreach (const auto& server, list.server()) {
        ServerStatus status = static_cast<ServerStatus>(server.status());
        uint32_t index = ServerId{server.server_id()}.indexNumber();
        if (index >= serverList.size())
            serverList.resize(index + 1);
        auto& entry = serverList[index];
        if (status == ServerStatus::UP) {
            entry.construct(ServerDetails(server));
            foreach (ServerTrackerInterface* tracker, trackers)
                tracker->enqueueChange(*entry, ServerChangeEvent::SERVER_ADDED);
        } else if (status == ServerStatus::CRASHED) {
            entry.construct(ServerDetails(server));
            foreach (ServerTrackerInterface* tracker, trackers) {
                tracker->enqueueChange(*entry,
                                       ServerChangeEvent::SERVER_CRASHED);
            }
        } else if (status == ServerStatus::DOWN) {
            entry->status = ServerStatus::DOWN;
            foreach (ServerTrackerInterface* tracker, trackers) {
                tracker->enqueueChange(*entry,
                                       ServerChangeEvent::SERVER_REMOVED);
            }
            entry.destroy();
        } else {
            assert(false);
        }
    }

    version = list.version_number();
    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->fireCallback();
}

// - private -

/**
 * Add a server to the server list and enqueue the ADDED event to all
 * registered trackers, but don't fire callbacks. Take care that this
 * will blindly replace any entry for a server with an id that
 * has the same index number.
 * Used only for unit testing. Never call this in real code; all
 * server list entry changes should come from the coordinator via
 * applyServerList().
 */
void
ServerList::testingAdd(const ServerDetails server)
{
    uint32_t index = server.serverId.indexNumber();
    if (index >= serverList.size())
        serverList.resize(index + 1);
    auto& entry = serverList.at(index);
    entry.construct(server);
    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->enqueueChange(*entry, ServerChangeEvent::SERVER_ADDED);
}

/**
 * Crash a server from the server list and enqueue the CRASHED event to all
 * registered trackers, but don't fire callbacks. Take care that this blindly
 * crashes the server in the the slot that corresponds to the index number of
 * the server id. It's your problem to make sure that it is populated and that
 * that makes sense.
 * Used only for unit testing. Never call this in real code; all
 * server list entry changes should come from the coordinator via
 * applyServerList().
 */
void
ServerList::testingCrashed(ServerId serverId)
{
    auto& entry = serverList.at(serverId.indexNumber());
    entry->status = ServerStatus::CRASHED;
    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->enqueueChange(*entry, ServerChangeEvent::SERVER_CRASHED);
}

/**
 * Remove a server from the server list and enqueue the REMOVED event to all
 * registered trackers, but don't fire callbacks. Take care that this blindly
 * removes the server in the the slot that corresponds to the index number of
 * the server id. It's your problem to make sure that it is populated and that
 * that makes sense.
 * Used only for unit testing. Never call this in real code; all
 * server list entries should come from the coordinator via
 * applyServerList().
 */
void
ServerList::testingRemove(ServerId serverId)
{
    auto& entry = serverList.at(serverId.indexNumber());
    entry->status = ServerStatus::DOWN;
    foreach (ServerTrackerInterface* tracker, trackers)
        tracker->enqueueChange(*entry, ServerChangeEvent::SERVER_REMOVED);
    entry.destroy();
}

} // namespace RAMCloud
