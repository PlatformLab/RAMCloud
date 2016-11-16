/* Copyright (c) 2016 Stanford University
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

#ifndef RAMCLOUD_UNSYNCEDRPCTRACKER_H
#define RAMCLOUD_UNSYNCEDRPCTRACKER_H

#include <queue>
#include <unordered_map>
#include "Common.h"
#include "ObjectRpcWrapper.h"
#include "Transport.h"
#include "WireFormat.h"

namespace RAMCloud {

/**
 * A temporary storage for RPC requests that have been responded by master but
 * have not been made durable in backups.
 *
 * Each client should keep an instance of this class to keep the information
 * on which RPCs were processed by a master, so that should be retried in case
 * of crash of the master.
 *
 * TODO: more detailed explanation why retry such RPCs?
 */
class UnsyncedRpcTracker {
  PUBLIC:
    explicit UnsyncedRpcTracker(Context* context);
    ~UnsyncedRpcTracker();
    void registerUnsynced(Transport::SessionRef session,
                          void* rpcRequest,
                          uint64_t tableId,
                          uint64_t keyHash,
                          uint64_t objVer,
                          WireFormat::LogPosition logPos,
                          std::function<void()> callback);
    void flushSession(Transport::Session* sessionPtr);
    void UpdateSyncPoint(Transport::Session* session,
                         WireFormat::LogPosition syncPoint);
    void pingMasterByTimeout();

  PRIVATE:
    /**
     * Holds info about an RPC whose effect is not made durable yet, which is
     * necessary to retry the RPC when a master crashes and loose the effects.
     */
    struct UnsyncedRpc {

        /// Default constructor
        UnsyncedRpc(void* rpcRequest, uint64_t tableId, uint64_t keyHash,
                    uint64_t objVer, WireFormat::LogPosition logPos,
                    std::function<void()> callback)
            : request(rpcRequest), tableId(tableId), keyHash(keyHash),
              objVersion(objVer), logPosition(logPos), callback(callback) {}

        /**
         * The pointer to the RPC request that was originally constructed by
         * this client. In case of master crash, a retry RPC with this request
         * will be sent to recovery master.
         * This request must be constructed by linearizable object RPC.
         */
        void* request;

        /**
         * Information about an object that determines which server the request
         * is sent to; we must save this information for use in retries.
         */
        uint64_t tableId;
        uint64_t keyHash;

        /**
         * Updated object version returned from master. This version will be
         * used to sanity check when recovery master processes the retry;
         * If a master already accepted an update request on the same key from
         * other clients and cannot successfully recover the original state by
         * retry, the master will notify the linearizability violations.
         */
        uint64_t objVersion;

        /**
         * Location of updated value of the object in master's log.
         * This information will be matched later with master's sync point,
         * so that we can safely discard RPC records as they become durable.
         */
        WireFormat::LogPosition logPosition;

        /**
         * The callback to be invoked as the effects of this RPC becomes
         * permanently durable.
         */
        std::function<void()> callback;

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(UnsyncedRpc);
    };

    /**
     * Each instance of this class stores information about unsynced RPCs
     * sent to a master, which is identified by Transport::Session.
     */
    struct Master {
      PUBLIC:
        /**
         * Constructor for Master
         *
         * \param session
         *      The boost_intrusive pointer to transport session
         */
        explicit Master(Transport::SessionRef& session)
            : syncPoint()
            , session(session)
            , rpcs()
        {}

        /**
         * Indicating the master's to log is synchronized with backups up to
         * this log position.
         */
        WireFormat::LogPosition syncPoint;

        /**
         * Used to prevent Transport::Session instance from destruction by
         * holding this smart pointer until the destruction of Master.
         */
        Transport::SessionRef session;

        /**
         * Queue keeping #UnsyncedRpc sent to this master.
         */
        std::queue<UnsyncedRpc> rpcs;

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(Master);
    };

    /**
     * RPC to send retries of requests that are lost due to a master's crash.
     */
    class RetryUnsyncedRpc : public ObjectRpcWrapper {
      public:
        RetryUnsyncedRpc(Context* context, uint64_t tableId, uint64_t keyHash,
                         void* requestToRetry)
            : ObjectRpcWrapper(context, tableId, keyHash,
                               sizeof(WireFormat::ResponseCommon), NULL)
        {
            rawRequest = requestToRetry;
            // TODO(seojin): Set flag of retry.

            send();
        }

        /// \copydoc RpcWrapper::docForWait
        void wait() {simpleWait(context);}

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(RetryUnsyncedRpc);
    };

    /// Helper methods
    Master* getOrInitMasterRecord(Transport::SessionRef& session);

    /**
     * Maps from #Session to target #Master.
     * Masters are dynamically allocated and must be freed explicitly.
     */
    typedef std::unordered_map<Transport::Session*, Master*> MasterMap;
    MasterMap masters;

    /**
     * Monitor-style lock. Any operation on internal data structure should
     * hold this lock.
     */
    std::mutex mutex;
    typedef std::lock_guard<std::mutex> Lock;

    Context* context;

    DISALLOW_COPY_AND_ASSIGN(UnsyncedRpcTracker);
};

}  // namespace RAMCloud

#endif // RAMCLOUD_UNSYNCEDRPCTRACKER_H
