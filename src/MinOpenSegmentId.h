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

class MinOpenSegmentId : public Task {
  PUBLIC:
    MinOpenSegmentId(TaskManager* taskManager,
                     CoordinatorClient* coordinator,
                     const ServerId* serverId)
        : Task(*taskManager)
        , coordinator(coordinator)
        , serverId(serverId)
        , current(0)
        , requested(0)
        , rpc()
    {}

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
                schedule();
            }
            // If current == requested then the task can go to sleep.
        } else {
            try {
                (*rpc)();
                current = requested;
            } catch (TransportException& e) {
                RAMCLOUD_LOG(WARNING, "Problem communicating with the "
                             "coordinator during setMinOpenSegmentId call, "
                             "retrying");
                schedule();
            }
            rpc.destroy();
            // If current == requested then the task can go to sleep.
        }
    }

    virtual bool isGreaterThan(uint64_t segmentId) {
        return current > segmentId;
    }

    virtual void updateToAtLeast(uint64_t segmentId) {
        if (requested > segmentId)
            return;
        requested = segmentId;
        schedule();
    }

  PROTECTED:
    CoordinatorClient* coordinator;
    /**
     * Complete unholy garbage.
     */
    const ServerId* serverId;
    uint64_t current;
    uint64_t requested;
    Tub<CoordinatorClient::SetMinOpenSegmentId> rpc;

    DISALLOW_COPY_AND_ASSIGN(MinOpenSegmentId);
};

} // namespace RAMCloud

#endif
