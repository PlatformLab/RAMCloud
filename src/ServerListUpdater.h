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

#ifndef RAMCLOUD_SERVERLISTUPDATER_H
#define RAMCLOUD_SERVERLISTUPDATER_H

#include <Client/Client.h>
#include <condition_variable>
#include <thread>
#include <queue>

#include "ServerList.pb.h"

#include "MembershipClient.h"
#include "ServerId.h"
#include "Tub.h"

namespace RAMCloud {
/**
 * Asynchronously propagates CoordinatorServerList updates.
 *
 * The public interface functions are thread-safe.
 */

/// Forward Declaration
class CoordinatorServerList;

class ServerListUpdater {
  PUBLIC:
    ServerListUpdater(Context& context, CoordinatorServerList& parent);
    ~ServerListUpdater();

    void start();
    void halt();

    void sendFullList(const ServerId& id,
        const ProtoBuf::ServerList& serverList);
    void sendUpdate(const std::vector<ServerId>& ids,
        const ProtoBuf::ServerList& update);
    void flush();

  PRIVATE:
    /// Actions that can be enqueued on the message queue.
    enum Opcode {
        UPDATE,
        FULL_LIST,
        STOP
    };

    /// Stores info needed for the BackgroundUpdater to do an async update.
    struct Message {
        Message(Opcode opcode, std::vector<ServerId> recipients,
                ProtoBuf::ServerList update)
            : opcode(opcode)
            , recipients(recipients)
            , update(update)
        {
        }
        Message(Opcode opcode, ServerId recipient,
                ProtoBuf::ServerList update)
            : opcode(opcode)
            , recipients()
            , update(update)
        {
            recipients.push_back(recipient);
        }
        explicit Message(Opcode opcode)
            : opcode(opcode)
            , recipients()
            , update()
        {
        }

        Opcode opcode;                    // Type of Message/Stop
        std::vector<ServerId> recipients; // ServerIds to receive this update
        ProtoBuf::ServerList update;      // Update to be send to recipients.
    };

    void sendMembershipUpdate(ServerId id, ProtoBuf::ServerList& update,
        Tub<ProtoBuf::ServerList>& serializedServerList);
    Message& getWork();
    void handleRequest(Message& msg);
    void workDone();
    void loop();

    const char* getLocator(ServerId id);

    /// The CoordinatorServerList instance that controls this BackgroundUpdater.
    CoordinatorServerList& parent;

    /// Shared RAMCloud information.
    Context& context;

    /// Signals when queue status changes
    /// (more work added/is newly empty/stopped)
    std::condition_variable hasUpdatesOrStop;

    /// The thread that is spawned and runs the BackgroundNotifier::loop()
    /// method when start() is called.
    Tub<std::thread> thread;

    /// Lock and mutex used to guard access to the message queue and the
    /// stop bool. The condition variable waits on this lock/mutex.
    typedef std::unique_lock<std::mutex> Lock;
    std::mutex mutex;

    /// Queue that stores pending membership updates and full list requests
    /// to be processed by the BackgroundUpdater. When a message is being
    /// processed, it remains on the front of the queue.
    std::queue<Message> msgQueue;

    /// The default message instructing the thread to exit the loop.
    Message stopMsg;

    /// Variable that indicates that the loop() method should return
    /// and therefore exit the updater thread. Do NOT set this manually,
    /// use halt().
    bool stop;
};
} // namespace RAMCloud

#endif  //!RAMCLOUD_SERVERLISTUPDATER_H
