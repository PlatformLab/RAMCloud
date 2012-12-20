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

#ifndef RAMCLOUD_UPDATEREPLICATIONEPOCHTASK_H
#define RAMCLOUD_UPDATEREPLICATIONEPOCHTASK_H

#include "Common.h"
#include "CoordinatorClient.h"
#include "CoordinatorSession.h"
#include "ServerId.h"
#include "TaskQueue.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * A Task (see TaskQueue) which provides access to the latest
 * ReplicationEpoch acknowledged by the coordinator for a particular
 * ServerId, and allows easy, asynchronous updates to the value stored
 * on the coordinator.
 *
 * Logically part of ReplicaManager. Used as part of backup recovery
 * to prevent replicas which the master lost contact with from being
 * detected as the head of the log during a recovery.
 */
class UpdateReplicationEpochTask : public Task {
  PUBLIC:

    /**
     * Construct an instance to track and update the replication epoch
     * stored on the coordinator.
     *
     * \param context
     *      Overall information about this RAMCloud server.
     * \param taskQueue
     *      The TaskQueue which this Task will schedule itself with in
     *      the case the replication epoch stored on the coordinator
     *      needs to be updated.
     * \param serverId
     *      The ServerId of the master whose replication epoch is to be updated
     *      on the coordinator.
     */
    UpdateReplicationEpochTask(Context* context,
                               TaskQueue* taskQueue,
                               const ServerId* serverId)
        : Task(*taskQueue)
        , context(context)
        , serverId(serverId)
        , current()
        , sent()
        , requested()
        , rpc()
    {}

    /**
     * Returns true if the \a segmentId, \a epoch pair is at least as "great"
     * as the replication epoch pair durably stored on the coordinator, false
     * otherwise. That is, when any replica with a lower segment id or the
     * same segment id but a lower epoch will not be used in any future
     * recovery of this server. Callers should use updateToAtLeast() to
     * attempt to set this value and then use this method to poll that it
     * has been set on the coordinator.
     */
    bool isAtLeast(uint64_t segmentId, uint64_t epoch) {
        return current >= ReplicationEpoch{segmentId, epoch};
    }

    /**
     * Try to update the replication epoch stored on the coordinator to at
     * least \a segmentId, \a epoch. If the coordinator can be contacted then
     * it is guaranteed that isAtLeast(segmentId, epoch) will return true at
     * some point in the future once the coordinator has durably stored the new
     * replication epoch or perhaps a higher value from subsequent calls to
     * this method. If the coordinator becomes unavailable then
     * isAtLeast(segmentId, epoch) may return true indefinitely until the
     * coordinator becomes available again.
     */
    void updateToAtLeast(uint64_t segmentId, uint64_t epoch) {
        RAMCLOUD_TEST_LOG("request update to master recovery info for %s to "
                          "%lu,%lu",
                          serverId->toString().c_str(), segmentId, epoch);
        ReplicationEpoch newEpoch{segmentId, epoch};
        if (requested > newEpoch)
            return;
        requested = newEpoch;
        schedule();
    }

    /**
     * Called by #taskQueue when it makes progress if this Task is scheduled.
     * That is, whenever a rpc needs to be sent or there is an outstanding rpc
     * to the coordinator.
     * This should never be called by normal users of this class, but only
     * by its #taskQueue.
     */
    virtual void performTask() {
#ifdef TESTING
        // When running tests, if there does not seem to be a coordinator
        // present, then just skip the call.
        if (context->coordinatorSession->getLocation().empty()) {
            current = requested;
            return;
        }
#endif
        if (!rpc) {
            if (current != requested) {
                ProtoBuf::MasterRecoveryInfo recoveryInfo;
                recoveryInfo.set_min_open_segment_id(requested.first);
                recoveryInfo.set_min_open_segment_epoch(requested.second);
                rpc.construct(context, *serverId, recoveryInfo);
                sent = requested;
            }
        } else {
            if (rpc->isReady()) {
                rpc->wait();
                current = sent;
                RAMCLOUD_LOG(DEBUG, "coordinator replication epoch for %s "
                             "updated to %lu,%lu", serverId->toString().c_str(),
                             current.first, current.second);
                rpc.destroy();
            }
        }
        if (current != requested)
            schedule();
    }

  PROTECTED:
    /**
     * Shared RAMCloud information.
     */
    Context* context;

    /**
     * The ServerId of the master whose replication epoch is to be updated on
     * the coordinator.
     */
    const ServerId* serverId;

    typedef std::pair<uint64_t, uint64_t> ReplicationEpoch;

    /**
     * The highest value known to have been acknowledged on the coordinator as
     * the replication epoch for this #serverId. If this differs from
     * #requested (set via updateToAtLeast()) then an rpc will be sent to
     * update the value on the coordinator until success.
     */
    ReplicationEpoch current;

    /**
     * The last value sent to the coordinator for replication epoch so far.
     * Used so that if #requested is updated while an rpc is outstanding we
     * still know what value the coordinator acknowledged when it was
     * reaped and can update #current accordingly.
     */
    ReplicationEpoch sent;

    /**
     * The highest caller requested value for replication epoch seen so far.
     * If this differs from #current then it will be sent to the coordinator
     * the next time there is no outstanding rpc in progress.
     */
    ReplicationEpoch requested;

    /**
     * Holds an ongoing rpc to the coordinator to update the replication epoch
     * for this #serverId, if any rpc is outstanding.
     */
    Tub<SetMasterRecoveryInfoRpc> rpc;

    DISALLOW_COPY_AND_ASSIGN(UpdateReplicationEpochTask);
};

} // namespace RAMCloud

#endif
