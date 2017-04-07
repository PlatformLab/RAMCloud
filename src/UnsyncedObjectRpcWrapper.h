/* Copyright (c) 2017 Stanford University
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

#include "LinearizableObjectRpcWrapper.h"
#include "Minimal.h"

#ifndef RAMCLOUD_UNSYNCEDOBJECTRPCWRAPPER_H
#define RAMCLOUD_UNSYNCEDOBJECTRPCWRAPPER_H

namespace RAMCloud {

struct WitnessEntry {
    uint64_t serverId;
    uint16_t hashIndex;
};

/**
 * ObjectRpcWrapper
 */
class UnsyncedObjectRpcWrapper : public LinearizableObjectRpcWrapper {
  public:
    explicit UnsyncedObjectRpcWrapper(RamCloud* ramcloud, bool async,
            uint64_t tableId, const void* key, uint16_t keyLength,
            uint32_t responseHeaderLength, Buffer* response = NULL);
    explicit UnsyncedObjectRpcWrapper(RamCloud* ramcloud, bool async,
            uint64_t tableId, uint64_t keyHash, uint32_t responseHeaderLength,
            Buffer* response = NULL);
    virtual ~UnsyncedObjectRpcWrapper() {/*TODO: implement*/}
    void cancel();
    virtual bool isReady();
    Transport::Session* getSessionUsed() { return session.get(); }

    static uint64_t rejectCount;
    static uint64_t totalCount;

  PROTECTED:
    virtual void send();
    bool waitInternal(Dispatch* dispatch, uint64_t abortTime = ~0UL);
    void clearAndRetry(uint32_t minDelayMicros, uint32_t maxDelayMicros);

    // General client information.
    RamCloud* ramcloud;

    enum Asynchrony {
        SYNC,
        ASYNC,              // Backed by retries from client
        ASYNC_DURABLE       // Backed by Witness recording & client retries
    };
    // If true, the new object will not be immediately replicated to backups.
    // Requests will be sent to witness concurrently.
    Asynchrony async;

    class WitnessRecordRpc : public RpcWrapper {
      public:
        WitnessRecordRpc(Context* context,
                Transport::SessionRef& sessionToWitness, uint64_t witnessId,
                uint64_t targetMasterId, uint64_t bufferBasePtr,
                int16_t hashIndex, uint64_t tableId, uint64_t keyHash,
                ClientRequest request);
        bool wait();

      PROTECTED:
        virtual bool handleTransportError();

      PRIVATE:
        Context* context;

      public:
        uint64_t tableId;

        /**
         * Only survive if accepted. Set to 0 if the record request was
         * rejected by witness.
         */
        uint64_t witnessServerId;

        /**
         * Only survive if accepted. Set to 0 if the record request was
         * rejected by witness.
         */
        uint64_t targetMasterId;

        /**
         * Only survive if accepted. Set to -1 if the record request was
         * rejected by witness.
         */
        int16_t hashIndex;

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(WitnessRecordRpc);
    };

    Tub<WitnessRecordRpc> witnessRecordRpcs[WITNESS_PER_MASTER];

    /// For non-durable RPCs, request buffer is linking this memory.
    /// We keep this pointer to register this request to UnsyncedRpcTracker.
    /// For regular durable RPCs, its value is null.
//    ClientRequest rawRequest;

    DISALLOW_COPY_AND_ASSIGN(UnsyncedObjectRpcWrapper);
};

} // end RAMCloud

#endif  // RAMCLOUD_OBJECTRPCWRAPPER_H
