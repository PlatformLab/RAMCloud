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

#ifndef RAMCLOUD_MINOPENSEGMENTID_H
#define RAMCLOUD_MINOPENSEGMENTID_H

#include "Common.h"
#include "CoordinatorClient.h"
#include "ServerId.h"
#include "TaskManager.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * A Task (see TaskManager) which provides access to the latest
 * minOpenSegmentId acknowledged by the coordinator for a particular
 * ServerId, and allows easy, asynchronous updates to the value stored
 * on the coordinator.
 *
 * Logically part of ReplicaManager.  Used as part of backup recovery
 * to prevent replicas which the master lost contact with from being
 * detected as the head of the log during a recovery.
 */
class MinOpenSegmentId : public Task {
  PUBLIC:

    /**
     * Construct an instance to track and update the minOpenSegmentId
     * stored on the coordinator.
     *
     * \param taskManager
     *      The TaskManager which this Task will schedule itself with in
     *      the case the minOpenSegmentId stored on the coordinator
     *      needs to be updated.
     * \param coordinator
     *      A client which is used to communicate with the coordinator
     *      in the case the minOpenSegmentId stored on the coordinator needs
     *      to be updated.
     * \param serverId
     *      The ServerId of the master whose minOpenSegmentId is to be updated
     *      on the coordinator.
     */
    MinOpenSegmentId(TaskManager* taskManager,
                     CoordinatorClient* coordinator,
                     const ServerId* serverId)
        : Task(*taskManager)
        , coordinator(coordinator)
        , serverId(serverId)
        , current(0)
        , sent(0)
        , requested(0)
        , rpc()
    {}

    /**
     * Returns true if \a segmentId is greater than the current value for
     * minOpenSegmentId durably stored on the coordinator, false otherwise.
     * To increase this value see updateToAtLeast().  Callers will generally
     * want to use this to see if the durable minOpenSegmentId is sufficient
     * to proceed safely, or they'll call updateToAtLeast() and then begin
     * polling isGreaterThan(segmentId) to wait for the update (or a following,
     * higher update) to be acknowledged as applied by the coordinator.
     */
    bool isGreaterThan(uint64_t segmentId) {
        return current > segmentId;
    }

    /**
     * Try to update the minOpenSegmentId stored on the coordinator to at least
     * \a segmentId.  If the coordinator can be contacted then it is guaranteed
     * that isGreaterThan(segmentId) will return false at some point in the
     * future once the coordinator has durably stored a minOpenSegmentId of
     * either \a segmentId or perhaps a higher value from subsequent calls to
     * this method.  If the coordinator becomes unavailable then
     * isGreaterThan(segmentId) may return true indefinitely until the
     * coordinator becomes available again.
     *
     * \param segmentId
     *      If this value is higher than the highest value ever passed to this
     *      method then try to update the value stored on
     */
    void updateToAtLeast(uint64_t segmentId) {
        RAMCLOUD_LOG(DEBUG, "request update to minOpenSegmentId for %lu to %lu",
                     serverId->getId(), segmentId);
        if (requested > segmentId)
            return;
        requested = segmentId;
        schedule();
    }

    /**
     * Called by #taskManager when it makes progress if this Task is scheduled.
     * That is, whenever a rpc needs to be sent or there is an outstanding rpc
     * to the coordinator.
     * This should never be called by normal users of this class, but only
     * by its #taskManager.
     */
    virtual void performTask() {
        if (!coordinator) {
            // For unit testing to prevent the need for a coordinator (that is,
            // other unit tests, not the tests for MinOpenSegmentId).
            current = requested;
            return;
        }
        if (!rpc) {
            if (current != requested) {
                rpc.construct(*coordinator, *serverId, requested);
                sent = requested;
            }
        } else {
            if (rpc->isReady()) {
                try {
                    (*rpc)();
                    current = sent;
                    RAMCLOUD_LOG(DEBUG, "coordinator minOpenSegmentId for %lu "
                                 "updated to %lu", serverId->getId(), current);
                } catch (TransportException& e) {
                    RAMCLOUD_LOG(WARNING, "Problem communicating with the "
                                 "coordinator during setMinOpenSegmentId call, "
                                 "retrying");
                }
                rpc.destroy();
            }
        }
        if (current != requested)
            schedule();
    }

  PROTECTED:
    /**
     * The coordinator whose value of minOpenSegmentId should be kept in sync.
     * Can be NULL for testing in which case the local, cached value of
     * minOpenSegmentId is used to emulate updates to the coordinator-stored
     * value.
     */
    CoordinatorClient* coordinator;

    /**
     * Complete unholy garbage.  This has to be a pointer because the reference
     * to the serverId is provided before the server actually receives the value
     * for its ServerId.  This can be NULL if coordinator is NULL.
     */
    const ServerId* serverId;

    /**
     * The highest value known to have been acknowledged on the coordinator as
     * the minOpenSegmentId for this #serverId.  If this differs from
     * #requested (set via updateToAtLeast()) then an rpcs will be sent to
     * update the value on the coordinator until success.
     */
    uint64_t current;

    /**
     * The last value sent to the coordinator for minOpenSegmentId so far.
     * Used so that if #requested is updated while an rpc is outstanding we
     * still know what value the coordinator acknowledged when it was
     * reaped and can update #current accordingly.
     */
    uint64_t sent;

    /**
     * The highest caller requested value for minOpenSegmentId seen so far.
     * If this differs from #current then it will be sent to the coordinator
     * the next time there is no outstanding rpc in progress.
     */
    uint64_t requested;

    /**
     * Holds an ongoing rpc to the coordinator to update the minOpenSegmentId
     * for this #serverId, if any rpc is outstanding.
     */
    Tub<CoordinatorClient::SetMinOpenSegmentId> rpc;

    DISALLOW_COPY_AND_ASSIGN(MinOpenSegmentId);
};

} // namespace RAMCloud

#endif
