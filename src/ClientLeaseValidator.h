/* Copyright (c) 2015 Stanford University
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

#ifndef RAMCLOUD_CLIENTLEASEVALIDATOR_H
#define RAMCLOUD_CLIENTLEASEVALIDATOR_H

#include "Minimal.h"
#include "Context.h"
#include "CoordinatorClient.h"
#include "ClusterClock.h"
#include "WireFormat.h"

namespace RAMCloud {

using WireFormat::ClientLease;

class ClientLeaseValidator {
  PUBLIC:
    /**
     * ClientLeaseValidator constructor.
     *
     * \param context
     *      Overall information about the RAMCloud server and allows this module
     *      to send RPCs.
     * \param clusterClock
     *      Provides access to the current cluster-time and allows this module
     *      to keep the cluster-time updated.
     */
    ClientLeaseValidator(Context* context, ClusterClock* clusterClock)
        : context(context)
        , clusterClock(clusterClock)
    {}

    /**
     * Provides hint as to whether the given lease needs to be validated.  This
     * operation is faster than validation and can be used to skip the full
     * validation if unnecessary.  Used by the UnackedRpcResults cleaner to
     * quickly determine if a lease needs to be validated.
     *
     * \param clientLease
     *      The client lease to be evaluated.
     * \return
     *      True, if there is a change the lease has expired and thus should
     *      be validated. False, otherwise.
     */
    bool needsValidation(ClientLease clientLease) {
        ClusterTime expirationTime(clientLease.leaseExpiration);
        clusterClock->updateClock(ClusterTime(clientLease.timestamp));

        if (expirationTime < clusterClock->getTime()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Validate the given client lease.  In the normal case where we expect the
     * lease to be valid, this validation operation is fast.  In some cases
     * where the lease may have expired, this method may be slow and involve an
     * RPC to the coordinator.
     *
     * \param clientLease
     *      The client lease to be evaluated.
     * \param[out] leaseInfo
     *      Returns updated lease information if the lease is still valid.
     * \return
     *      True, if the lease is still considered
     */
    bool validate(ClientLease clientLease, ClientLease* leaseInfo = NULL) {
        uint64_t clientId = clientLease.leaseId;

        if (needsValidation(clientLease)) {
            //contact coordinator for lease expiration and clusterTime.
            WireFormat::ClientLease lease =
                CoordinatorClient::getLeaseInfo(context, clientId);
            clusterClock->updateClock(ClusterTime(lease.timestamp));
            if (lease.leaseId == 0) {
                return false;
            } else if (leaseInfo) {
                *leaseInfo = lease;
            }
        }
        return true;
    }

  PRIVATE:
    Context* context;
    ClusterClock* clusterClock;

    DISALLOW_COPY_AND_ASSIGN(ClientLeaseValidator);
};

} // namespace RAMCloud

#endif // RAMCLOUD_CLIENTLEASEVALIDATOR_H
