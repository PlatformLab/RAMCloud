/* Copyright (c) 2009-2012 Stanford University
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

#ifndef RAMCLOUD_WITNESSSERVICE_H
#define RAMCLOUD_WITNESSSERVICE_H

#include <thread>
#include <map>

#include "Common.h"
#include "CoordinatorClient.h"
#include "MasterClient.h"
#include "Service.h"
#include "ServerConfig.h"
#include "TaskQueue.h"

namespace RAMCloud {

/**
 * Handles rpc requests from Masters and the Coordinator to temporary store
 * of recent unsynced requests and to facilitate the recovery of object data
 * when masters crash.
 */
class WitnessService : public Service
                     , ServerTracker<void>::Callback {
  PUBLIC:
    WitnessService(Context* context, const ServerConfig* config);
    virtual ~WitnessService();

    void dispatch(WireFormat::Opcode opcode, Rpc* rpc);

    static inline void gc(const WireFormat::WitnessGc::Request* reqHdr,
                          WireFormat::WitnessGc::Response* respHdr,
                          Buffer* requestPayload);
    static inline void record(const WireFormat::WitnessRecord::Request* reqHdr,
                              WireFormat::WitnessRecord::Response* respHdr,
                              Buffer* requestPayload);
    static inline void prepRecovery(Context* context, uint64_t crashedMasterId);

    static const int MAX_REQUEST_SIZE = 2048;
//    static const int NUM_ENTRIES_PER_TABLE = 512; // Must be power of 2.
    static const int NUM_ENTRIES_PER_TABLE = 4096; // Must be power of 2.
    static const int HASH_BITMASK = NUM_ENTRIES_PER_TABLE - 1;

  PRIVATE:
    void start(const WireFormat::WitnessStart::Request* reqHdr,
                WireFormat::WitnessStart::Response* respHdr,
                Rpc* rpc);
    void getRecoveryData(
                const WireFormat::WitnessGetRecoveryData::Request* reqHdr,
                WireFormat::WitnessGetRecoveryData::Response* respHdr,
                Rpc* rpc);

    void trackerChangesEnqueued();

    /**
     * Holds information to recover an RPC request in case of the master's crash
     */
    struct Entry {
        bool occupied; // TODO(seojin): check padding to 64-bit improves perf?
        WireFormat::WitnessRecord::RecordEntryHeader header;
        char request[MAX_REQUEST_SIZE];
    };

    /**
     * Holds information of a master being witnessed. Holds recent & unsynced
     * requests to the master.
     */
    struct Master {
        uint64_t id;
        bool writable;
        Entry table[NUM_ENTRIES_PER_TABLE];
    };

    /**
     * Shared RAMCloud information.
     */
    Context* context;

    /**
     * Provides mutual exclusion between handling RPCs and garbage collector.
     * Locked once for all RPCs in dispatch().
     */
    std::mutex mutex;
    typedef std::mutex Mutex;
    typedef std::unique_lock<Mutex> Lock;

    /// Settings passed to the constructor
    const ServerConfig* config;

    /// Used to track serverList changes and GC useless witness buffers.
    ServerTracker<void> gcTracker;

    /**
     * All witness buffers for masters this witness is recording for;
     * Maps a masterId to witness info for that master.
     */
    std::unordered_map<uint64_t, Master*> buffers;

    DISALLOW_COPY_AND_ASSIGN(WitnessService);
};

inline void
WitnessService::gc(const WireFormat::WitnessGc::Request* reqHdr,
                   WireFormat::WitnessGc::Response* respHdr,
                   Buffer* requestPayload)
{
    Master* buffer = reinterpret_cast<Master*>(reqHdr->bufferBasePtr);

    // Sanity check.
    if (buffer->id != reqHdr->targetMasterId || !buffer->writable) {
        respHdr->common.status = Status::STATUS_OK; // TODO: Something wrong...
        return;
    }

    WireFormat::WitnessGc::GcEntry* entries =
            reinterpret_cast<WireFormat::WitnessGc::GcEntry*>(
            requestPayload->getRange(sizeof32(*reqHdr),
                                     sizeof32(WireFormat::WitnessGc::GcEntry)
                                        * reqHdr->numEntries));
    for (int i = 0; i < reqHdr->numEntries; ++i) {
        auto common = reinterpret_cast<WireFormat::AsyncRequestCommon*>(
                buffer->table[entries[i].hashIndex].request);
        if (buffer->table[entries[i].hashIndex].occupied &&
                common->lease.leaseId == entries[i].clientLeaseId &&
                common->rpcId == entries[i].rpcId) {
            buffer->table[entries[i].hashIndex].occupied = false;
        }
    }

    respHdr->common.status = Status::STATUS_OK;
    // TODO: respond blocking requests in workerThread.
    respHdr->numOps = 0;
}


inline void
WitnessService::record(const WireFormat::WitnessRecord::Request* reqHdr,
                       WireFormat::WitnessRecord::Response* respHdr,
                       Buffer* requestPayload)
{
    Master* buffer = reinterpret_cast<Master*>(reqHdr->bufferBasePtr);
    assert(reqHdr->entryHeader.requestSize <= MAX_REQUEST_SIZE);
    assert(reqHdr->hashIndex < NUM_ENTRIES_PER_TABLE);

    // Sanity check.
    if (buffer->id != reqHdr->targetMasterId || !buffer->writable) {
        respHdr->accepted = false;
        respHdr->common.status = Status::STATUS_OK;
        return;
    }

    if (!buffer->table[reqHdr->hashIndex].occupied) {
        buffer->table[reqHdr->hashIndex].occupied = true;
        buffer->table[reqHdr->hashIndex].header = reqHdr->entryHeader;
        requestPayload->copy(
                sizeof32(*reqHdr),
                static_cast<uint32_t>(reqHdr->entryHeader.requestSize),
                buffer->table[reqHdr->hashIndex].request);
        respHdr->accepted = true;
    } else {
        respHdr->accepted = false;
    }

    respHdr->common.status = Status::STATUS_OK;
}

/**
 * Intended to run on dispatch thread. Only call this if continuation == 0.
 * \param context
 * \param crashedMasterId
 */
inline void
WitnessService::prepRecovery(Context* context, uint64_t crashedMasterId)
{
    WitnessService* ws = reinterpret_cast<WitnessService*>(
            context->services[WireFormat::WITNESS_SERVICE]);
    Lock _(ws->mutex);
    Master* buffer = ws->buffers[crashedMasterId];
    if (buffer->writable) {
        // Block further witness record requests to this table and
        // ensure everything written so far is visible to worker thread.
        buffer->writable = false;
        // Thread handoff does sfence operation. No need additional one here.
        //__asm__ __volatile__("sfence" ::: "memory");
    }
}

} // namespace RAMCloud

#endif
