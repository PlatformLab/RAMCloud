/* Copyright (c) 2012 Stanford University
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

namespace RAMCloud {

class BindTransport;

/**
 * Container for the various services and resources that make up a single
 * RAMCloud server.  Typically there is a single Server created on main(),
 * but this class also allows creation of many Servers which helps
 * for testing (see MockCluster).
 *
 * Server is not (yet) capable of providing the CoordinatorService; it is
 * pieced together manually in CoordinatorMain.cc.
 */
class Server {
  PUBLIC:
    /**
     * Bind a configuration to a Server, but don't start anything up
     * yet.
     *
     * \param context
     *      Overall information about the RAMCloud server.
     * \param config
     *      Specifies which services and their configuration details for
     *      when the Server is run.
     */
    explicit Server(Context& context, const ServerConfig& config)
        : context(context)
        , config(config)
        , backupReadSpeed()
        , backupWriteSpeed()
        , serverId()
        , serverList(context)
        , failureDetector()
        , master()
        , backup()
        , membership()
        , ping()
    {
        context.coordinatorSession->setLocation(
                config.coordinatorLocator.c_str());
    }

    void startForTesting(BindTransport& bindTransport);
    void run() __attribute__ ((noreturn));

  PRIVATE:
    ServerId createAndRegisterServices(BindTransport* bindTransport);
    void enlist(ServerId replacingId);

    /**
     * Shared RAMCloud information.
     */
    Context& context;

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
     * Write speed of the local backup's storage in MB/s.  Measured
     * startForTesting() used in run() during server enlistment unless
     * config.backup.mockSpeed is non-zero.
     */
    uint32_t backupWriteSpeed;

    /**
     * The id of this server.  This id only becomes valid during run() or after
     * startForTesting() when the server enlists with the coordinator and
     * receives its cluster server id.
     */
    ServerId serverId;

    /**
     * List of servers in the cluster which can be used by services to receive
     * cluster membership change notifications and to track per-server
     * details.  This class is maintained by the membership service.
     * See ServerList and ServerTracker.
     *
     * Note, if no membership service is requested on this service
     * (config.services) then this list will go un-updated.  Keep in mind,
     * this can lead to infinite loops in code that waits for notifications
     * of changes to the list.
     */
    ServerList serverList;

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
};

} // namespace RAMCloud

#endif
