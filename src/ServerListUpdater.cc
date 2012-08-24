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

#include <chrono>

#include "Common.h"
#include "ClientException.h"
#include "CoordinatorServerList.h"
#include "Cycles.h"
#include "MembershipClient.h"
#include "ServerListUpdater.h"
#include "ShortMacros.h"

#define FLUSH_STATUS_RECHECK_INTERVAL std::chrono::milliseconds(1);

namespace RAMCloud {
//////////////////////////////////////////////////////////////////////
// ServerListUpdater Class - Constructor/Destructor
//////////////////////////////////////////////////////////////////////

/**
 * Constructor for ServerListUpdater.
 *
 * \param context
 *      Overall information about the RAMCloud server
 *
 * \param parent
 *      The CoordinatorServerList instance that spawned this updater.
 */
ServerListUpdater::ServerListUpdater(Context& context,
        CoordinatorServerList& parent)
    : parent(parent)
    , context(context)
    , hasUpdatesOrStop()
    , thread()
    , mutex()
    , msgQueue()
    , stopMsg(STOP)
    , stop(true)
{
}

ServerListUpdater::~ServerListUpdater()
{
    halt();
}

//////////////////////////////////////////////////////////////////////
// ServerListUpdater - Public Interface Functions
//////////////////////////////////////////////////////////////////////

/**
 * Starts the ServerListUpdater thread unless one already exists.
 *
 * Note: If halt() was previously called and there were updates on the queue,
 * this call will resume sending the queued, possibly stale, updates.
 */
void
ServerListUpdater::start()
{
    Lock _(mutex);
    if (!thread) {
        stop = false;
        thread.construct(&ServerListUpdater::loop, this);
    }
}

/**
 * Stops all future and current updates that have not yet been propagated.
 * Update/FullList calls will be queued until start() is called again, which
 * will resuming sending out updates again.
 *
 * Note: This method is intended for graceful destruction and testing.
 */
void
ServerListUpdater::halt()
{
    Lock lock(mutex);
    stop = true;
    hasUpdatesOrStop.notify_all();
    lock.unlock();

    if (thread && thread->joinable())
        thread->join();

    thread.destroy();
}

/**
 * Enqueues a send full ServerList command into the message queue. This update
 * will be propagated through a background thread only if this ServerListUpdater
 * has not been halt() yet. To force a synchronization point, call flush().
 *
 * \param id - ServerId to send to
 * \param serverList - the serialized ProtoBuf::ServerList to send.
 */
void
ServerListUpdater::sendFullList(const ServerId& id,
        const ProtoBuf::ServerList& serverList)
{
    Lock _(mutex);
    msgQueue.emplace(FULL_LIST, id, serverList);
    hasUpdatesOrStop.notify_all();
}

/**
 * Enqueues a send membership update into the queue. This command will
 * handle sending a full ServerList if a missed update is detected by the
 * recipient.
 *
 * Note however, the full ServerList sent will be the list AT THE MOMENT
 * the RPC is sent out, NOT when this call was made. This can result in
 * duplicate or stale full lists to be sent at a later time (if there are
 * send full lists enqueued, but not yet processed in the queue).
 *
 * This update will be propagated through a background thread only if this
 * ServerListUpdater has not been halt() yet. To force a synchronization point,
 * call flush();
 *
 *
 * \param ids - The ServerId(s) to send the update to.
 * \param serverList - The serialized ProtoBuf::ServerList to send.
 */
void
ServerListUpdater::sendUpdate(const std::vector<ServerId>& ids,
        const ProtoBuf::ServerList& serverList)
{
    Lock _(mutex);
    msgQueue.emplace(UPDATE, ids, serverList);
    hasUpdatesOrStop.notify_all();
}

/**
 * Waits until all the messages have been processed in the queue. This call
 * will resume update propagation for all future send calls unless halt()ed
 * again.
 */
void
ServerListUpdater::flush()
{
    Lock lock(mutex);

    while (!msgQueue.empty()) {
        // Restart thread if necessary
        if (!thread) {
            stop = false;
            thread.construct(&ServerListUpdater::loop, this);
        }

        // Wait for finish or new halt()
        hasUpdatesOrStop.notify_all();
        hasUpdatesOrStop.wait(lock);
    }
}

//////////////////////////////////////////////////////////////////////
// ServerListUpdater - Private Functions
//////////////////////////////////////////////////////////////////////
/**
 * Gets work from the front of the queue and blocks while the queue is empty.
 * A workDone() call must be made to pop the first item off the queue
 * when the thread is done with it.
 *
 * If a stop halt() call is made, this method will return a stop message
 * regardless of what's actually on the queue.
 *
 * \return - Reference to the item to process (or a stop message if halt()ed).
 */
ServerListUpdater::Message&
ServerListUpdater::getWork()
{
    Lock lock(mutex);
    while (!stop) {
        if (!msgQueue.empty())
            return msgQueue.front();

        hasUpdatesOrStop.wait(lock);
    }

    return stopMsg;
}

/**
 * Pops the first item off the queue without returning it.
 *
 * It is assumed that getWork() was called prior and thus there is only an
 * assert check.
 */
void
ServerListUpdater::workDone()
{
    Lock _(mutex);
    assert(!msgQueue.empty());

    msgQueue.pop();

    if (msgQueue.empty())
        hasUpdatesOrStop.notify_all();
}

/**
 * Main loop run by the thread. It repeatedly gets work from the queue,
 * processes it, and pops work off the queue while the thread is not halted().
 */
void
ServerListUpdater::loop()
{
    try {
        while (!stop) {
            Message& msg = getWork();
            if (msg.opcode == STOP)
                return;
            handleRequest(msg);
            workDone();
        }
    } catch (const std::exception& e) {
        LOG(ERROR, "Fatal error in ServerListUpdater: %s", e.what());
        throw;
    } catch (...) {
        LOG(ERROR, "Unknown fatal error in ServerListUpdater.");
        throw;
    }
}

/**
 * Handles a request on the msg queue
 *
 * \param msg - Reference to the Msg on the queue to decode.
 */
void
ServerListUpdater::handleRequest(Message& msg) {
    Tub<ProtoBuf::ServerList> serializedServerList;

    foreach (ServerId id, msg.recipients) {
        // Ensure server is still up before sending updates
        if (!parent.isUp(id)) {
            LOG(NOTICE, "Async sendUpdate to %s occured after it was "
                    "removed/downed in the CoordinatorServerList.",
                    id.toString().c_str());
            continue;
        }

        try {
            switch (msg.opcode) {
                case UPDATE:
                    sendMembershipUpdate(id, msg.update, serializedServerList);
                    break;
                case FULL_LIST:
                    LOG(DEBUG, "Sending server list to server id %s as "
                            "requested", id.toString().c_str());
                    MembershipClient::setServerList(context, id, msg.update);
                    break;
                default:
                    LOG(NOTICE, "A malformed opcode was found in the "
                            "msgQueue: %d", msg.opcode);
                continue;
            }
        } catch (const ServerDoesntExistException& e) {
            // Log but fail quietly otherwise since there's nothing we can do.
            LOG(NOTICE, "Async sendUpdate to %s occured after it was "
                    "removed/downed in the CoordinatorServerList.",
                    id.toString().c_str());
        }
    }
}

/**
 * Sends the update to the specified server specified by a ServerId. If the
 * server returns an error, a full list will be generated from the parent
 * CoordinatorServerList WHEN THIS CALL OCCURS (which may be different from
 * when it was originally enqueued by the parent) and sent.
 *
 *
 * See CoordinatorServerList::sendMembershipUpdate for more info.
 *
 * \param id - ServerId of the server to contact
 * \param update - the update to send
 * \param serializedServerList - Tub reference to a Protobuf::ServerList. If
 * the tub is empty, it will be generated/serialized else its contents will be
 * reused.
 *
 * \throw ServerDoesntExistException
 *      The intended serverId for this update is no longer a part of the cluster
 */
void
ServerListUpdater::sendMembershipUpdate(
        ServerId id,
        ProtoBuf::ServerList& update,
        Tub<ProtoBuf::ServerList>& serializedServerList)
{
    UpdateServerListRpc rpc(context, id, update);

    bool succeeded = false;
    uint64_t start = Cycles::rdtsc();
    uint64_t stalled = 0;
    const uint64_t timeoutNs = 250 * 1000 * 1000; // 250 ms
    while (!rpc.isReady() && stalled < timeoutNs)
        stalled = Cycles::toNanoseconds(Cycles::rdtsc() - start);
    if (stalled < timeoutNs) {
        try {
            succeeded = rpc.wait();
        } catch (const ServerDoesntExistException& e) {}
    } else {
        rpc.cancel();
        LOG(NOTICE, "Failed to send cluster membership update to %s",
            id.toString().c_str());
    }

    if (succeeded) {
         LOG(DEBUG, "Server list update sent to server %s",
             id.toString().c_str());
         return;
    }

    // If this server had missed a previous update it will return
    // failure and expect us to push the whole list again.
    LOG(NOTICE, "Server %s had lost an update. Sending whole list.",
        id.toString().c_str());
    if (!serializedServerList) {
        serializedServerList.construct();
        parent.serialize(*serializedServerList);
    }
    SetServerListRpc rpc2(context, id,
                         *serializedServerList);
    start = Cycles::rdtsc();
    stalled = 0;
    while (!rpc2.isReady() && stalled < timeoutNs)
        stalled = Cycles::toNanoseconds(Cycles::rdtsc() - start);
    if (stalled < timeoutNs) {
        try {
            rpc2.wait();
        } catch (const ServerDoesntExistException& e) {}
    } else {
        rpc2.cancel();
        LOG(NOTICE, "Failed to send full cluster server list to %s "
            "after it failed to accept the update",
            id.toString().c_str());
    }
}
} // namespace RAMCloud
