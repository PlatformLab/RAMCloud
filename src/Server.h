/* Copyright (c) 2012-2015 Stanford University
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

#ifndef RAMCLOUD_SERVER_H
#define RAMCLOUD_SERVER_H

#include "BackupService.h"
#include "CoordinatorClient.h"
#include "CoordinatorSession.h"
#include "FailureDetector.h"
#include "MasterService.h"
#include "MembershipService.h"
#include "PingService.h"
#include "ServerConfig.h"
#include "ServerId.h"
#include "ServerList.h"
#include "WorkerTimer.h"

namespace RAMCloud {

class BindTransport;

/**
 * Container for the various services and resources that make up a single
 * RAMCloud server (master and/or backup, but not coordinator).  Typically
 * there is a single Server created on main(), but this class can be used
 * to create many servers for testing (e.g., in MockCluster).
 *
 * Server is not (yet) capable of providing the CoordinatorService; it is
 * pieced together manually in CoordinatorMain.cc.
 */
class Server {
  PUBLIC:
    explicit Server(Context* context, const ServerConfig* config);
    ~Server();

    void startForTesting(BindTransport& bindTransport);
    void run();

  PRIVATE:
    ServerId createAndRegisterServices();
    void enlist(ServerId replacingId);

    /**
     * Shared RAMCloud information.
     */
    Context* context;

    /**
     * Configuration that controls which services are started as part of
     * this service and how each of those services is configured.  Many
     * of the contained parameters are set via option parsing from the
     * command line, but many are also internally set and used specifically
     * to create strange server configurations for testing.
     */
    ServerConfig config;

    /**
     * Read speed of the local backup's storage in MB/s.  Measured
     * startForTesting() used in run() during server enlistment unless
     * config.backup.mockSpeed is non-zero.
     */
    uint32_t backupReadSpeed;

    /**
     * The id of this server.  This id only becomes valid during run() or after
     * startForTesting() when the server enlists with the coordinator and
     * receives its cluster server id.
     */
    ServerId serverId;

    /**
     * The following variable is NULL if a serverList was already provided
     * in \c context. If not, we create a new one and store its pointer here
     * (so we can delete it in the destructor) as well as in context.
     * Normally, the server list should be accessed from \c context, not here.
     */
    ServerList* serverList;

    /// If enabled detects other Server in the cluster, else empty.
    Tub<FailureDetector> failureDetector;

    /**
     * The MasterService running on this Server, if requested, else empty.
     * See config.services.
     */
    Tub<MasterService> master;

    /**
     * The BackupService running on this Server, if requested, else empty.
     * See config.services.
     */
    Tub<BackupService> backup;

    /**
     * The MembershipService running on this Server, if requested, else empty.
     * See config.services.
     */
    Tub<MembershipService> membership;

    /**
     * The PingService running on this Server, if requested, else empty.
     * See config.services.
     */
    Tub<PingService> ping;

    // The class and variable below are used to run enlistment in a
    // worker thread, so that post-enlistment initialization doesn't
    // keep us from servicing RPCs.
    class EnlistTimer: public WorkerTimer {
      public:
        EnlistTimer(Server* server, ServerId formerServerId)
                : WorkerTimer(server->context->dispatch)
                , server(server)
                , formerServerId(formerServerId)
            {
                // Start enlistment immediately.
                start(0);
            }
        virtual void handleTimerEvent() {
            server->enlist(formerServerId);
        }
        Server* server;
        ServerId formerServerId;
        DISALLOW_COPY_AND_ASSIGN(EnlistTimer);
    };
    Tub<EnlistTimer> enlistTimer;

    DISALLOW_COPY_AND_ASSIGN(Server);
};

} // namespace RAMCloud

#endif
