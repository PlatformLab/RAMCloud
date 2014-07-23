/* Copyright (c) 2014 Stanford University
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

#ifndef RAMCLOUD_UNACKEDRPCRESULTS_H
#define RAMCLOUD_UNACKEDRPCRESULTS_H

#include <unordered_map>
#include "Common.h"
#include "SpinLock.h"

namespace RAMCloud {

/**
 * A temporary storage for results of linearizable RPCs that have not been
 * acknowledged by client.
 *
 * Each master should keep an instance of this class to keep the information
 * on which rpc has been processed with its result. The information should be
 * used to avoid processing re-tried RPCs again.
 */
class UnackedRpcResults {
  PUBLIC:
    UnackedRpcResults() : clients(20), mutex(), default_rpclist_size(5) {}
    ~UnackedRpcResults();

    void addClient(uint64_t clientId);
    bool checkDuplicate(uint64_t clientId,
                        uint64_t rpcId,
                        uint64_t ackId,
                        void** result);
    bool shouldRecover(uint64_t clientId, uint64_t rpcId, uint64_t ackId);
    void recordCompletion(uint64_t clientId, uint64_t rpcId, void* result);
    bool isRpcAcked(uint64_t clientId, uint64_t rpcId);

    /// Thrown if already acknowledged rpcId is checked in checkDuplicate().
    struct StaleRpc : public Exception {
        explicit StaleRpc(const CodeLocation& where) : Exception(where) {}
    };

    /// Thrown if checkDuplicate finds that it doesn't have enough information
    /// to decide whether the RPC already has been executed. (eg. Client forgot
    ///  to register, or the information is thrown away due to a timeout.)
    struct NoClientInfo : public Exception {
        explicit NoClientInfo(const CodeLocation& where) : Exception(where) {}
    };

  PRIVATE:
    void resizeRpcList(uint64_t clientId, int size);
    void cleanByTimeout();

    /**
     * Holds info about outstanding RPCs, which is needed to avoid re-doing
     * the same RPC.
     */
    struct UnackedRpc {
        UnackedRpc(uint64_t rpcId = 0, void* resultPtr = 0)
            : id(rpcId), result(resultPtr) {}

        /**
         * Rpc id assigned by a client.
         */
        uint64_t id;

        /**
         * Pointer to the result of the RPC in log-structured memory.
         * This value may be NULL to indicate the RPC is still in progress.
         */
        void* result;
    };

    /**
     * Stores unacknowledged rpc's id and pointer to result from a client
     * with some additional useful information for performance.
     */
    class Client {
      PUBLIC:
        /**
         * Constructor for Client
         *
         * \param size
         *      Initial size of the array which keeps UnackedRpc.
         */
        explicit Client(int size) : maxRpcId(0),
                                    maxAckId(0),
                                    lastUpdateTime(0),
                                    rpcs(new UnackedRpc[size]()),
                                    len(size) {
        }
        ~Client() {
            delete rpcs;
        }

        bool hasRecord(uint64_t rpcId);
        void* result(uint64_t rpcId);
        void recordNewRpc(uint64_t rpcId);
        void updateResult(uint64_t rpcId, void* result);

        /**
         * Keeps the largest RpcId from the client seen in this master.
         * This is used to optimize checkDuplicate() in general case.
         */
        uint64_t maxRpcId;

        /**
         * The largest ack number received from this client.
         * We do not need to retain information for RPCs with rpcId <= maxAckId.
         */
        uint64_t maxAckId;

        /**
         * Used for cleaning inactive client's data on timeout.
         */
        time_t lastUpdateTime;

      PRIVATE:
        void resizeRpcs(int increment);

        /**
         * Dynamically allocated array keeping #UnackedRpc of this client.
         * This keeps the status of each RPC until being acknowledged.
         * The array is initially with small size, let's say 5. Then, the info
         * about a rpc with id = i is recorded in (i % 5)th index of the array.
         *
         *   Example of array #Client::rpcs
         * | index  |  0  |  1  |  2  |  3  |  4  |
         * | rpc id |  5  | 11  |  7  |  8  |  9  |
         *
         * If the index (i % len), where #Client::len is the size of array, is
         * already occupied with other rpc which is not yet acknowledged, we
         * need to increase the size of array. But the maximum size of array
         * is bounded by the hard limit of the maximum number of outstanding
         * RPC by a client.
         */
        UnackedRpc* rpcs;

        /**
         * Length of the dynamic array, #rpcs.
         */
        int len;

        DISALLOW_COPY_AND_ASSIGN(Client);
    };

    /**
     * Maps from a registered client ID to #Client.
     */
    std::unordered_map<uint64_t, Client*> clients;
    typedef std::unordered_map<uint64_t, Client*> ClientMap;

    /**
     * Monitor-style lock. Any operation on internal data structure should
     * hold this lock.
     */
    std::mutex mutex;
    typedef std::lock_guard<std::mutex> Lock;

    /**
     * This value is used as initial array size of each Client instance.
     */
    int default_rpclist_size;

    DISALLOW_COPY_AND_ASSIGN(UnackedRpcResults);
};

}  // namespace RAMCloud

#endif // RAMCLOUD_UNACKEDRPCRESULTS_H
