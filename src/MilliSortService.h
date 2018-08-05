/* Copyright (c) 2011-2016 Stanford University
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

#ifndef RAMCLOUD_MILLISORTSERVICE_H
#define RAMCLOUD_MILLISORTSERVICE_H

#include "CommunicationGroup.h"
#include "Context.h"
#include "Dispatch.h"
#include "Service.h"
#include "ServerConfig.h"
#include "WorkerManager.h"

namespace RAMCloud {

/**
 * This Service supports a variety of requests used for cluster management,
 * such as pings, server controls, and server list management.
 */
class MilliSortService : public Service {
  public:
    explicit MilliSortService(Context* context,
            const ServerConfig* serverConfig);
    ~MilliSortService();
    void dispatch(WireFormat::Opcode opcode, Rpc* rpc);

    CommunicationGroup*
    getCommunicationGroup(int groupId)
    {
        CommGroupTable::iterator it = communicationGroupTable.find(groupId);
        return (it != communicationGroupTable.end()) ? it->second : NULL;
    }

    bool
    registerCommunicationGroup(CommunicationGroup* group)
    {
        if (communicationGroupTable.find(group->id) ==
                communicationGroupTable.end()) {
            communicationGroupTable[group->id] = group;
            return true;
        } else {
            return false;
        }
    }

    void
    initWorld()
    {
        if (!world) {
            // FIXME: Geee, I can't believe this is actually what you need to do
            // to iterate over the server list.
            std::vector<ServerId> allNodes;
            // FIXME: not sure why serverList can have duplicate records.
            std::array<bool, 1000> set = {0};
            bool end = false;
            for (ServerId id = ServerId(0); !end;) {
                id = context->serverList->nextServer(id,
                        ServiceMask({WireFormat::MILLISORT_SERVICE}), &end);
                if (!set[id.indexNumber()]) {
                    allNodes.push_back(id);
//                    LOG(WARNING, "server id %u, gen. %u", id.indexNumber(),
//                            id.generationNumber());
                    set[id.indexNumber()] = true;
                }
            }
            std::sort(allNodes.begin(), allNodes.end(), std::less<ServerId>());
            world.construct(WORLD, serverId.indexNumber() - 1, allNodes);
        }
        registerCommunicationGroup(world.get());
    }

    /**
     * Helper function to initiate a collective operation. It creates and
     * schedules a collective operation task on the rule engine, sets up the
     * continuation code to run upon the completion of the operation, and
     * handles RPCs for this collective operation that arrive early.
     *
     * \tparam T
     *      Concrete type of the collective operation to create.
     * \tparam Callback
     *      Concrete type of the callback.
     * \tparam Args
     *      Types of the arguments passed to the constructor of the collective
     *      operation.
     * \param opId
     *      Unique identifier of the collective operation.
     * \param callback
     *      Continuation code to run upon the completion of the collective
     *      operation.
     * \param args
     *      Arguments to be passed to the constructor of the collective
     *      operation.
     */
    template <typename Op, typename... Args>
    void
    invokeCollectiveOp(Tub<Op>& op, int opId, Args&&... args)
    {
        op.construct(opId, args...);

        // FIXME: sync with the dispatch thread? the following doesn't work; op.construct may be blocking?
        Dispatch::Lock _(context->dispatch);

        CollectiveOpRecord* record = getCollectiveOpRecord(opId);
        record->op = op.get();
//        LOG(WARNING, "about to handle early RPCs from op %d, record %p, op %p",
//                opId, record, op.get());
        for (Transport::ServerRpc* serverRpc : record->serverRpcList) {
            context->workerManager->collectiveOpRpcs.push_back(serverRpc);
//            LOG(WARNING, "RPC from collective op %d arrives early!", opId);
        }
        record->serverRpcList.clear();
        // FIXME: can't do it here because pivotsShuffle needs a ptr to op
//        op.wait();
//        collectiveOpTable.erase(opId);
    }

    /**
     * Helper function for use in handling RPCs used by collective operations.
     * Record the RPC for future processing if the corresponding collective
     * operation has not been created.
     *
     * \tparam Op
     *      Type of the collective operation (e.g., AllGather, AllShuffle, etc.)
     * \param reqHdr
     *      Header from the incoming RPC request; contains parameters
     *      for this operation.
     * \param[out] respHdr
     *      Header for the response that will be returned to the client.
     *      The caller has pre-allocated the right amount of space in the
     *      response buffer for this type of request, and has zeroed out
     *      its contents (so, for example, status is already zero).
     * \param[out] rpc
     *      Complete information about the remote procedure call; can be
     *      used to read additional information beyond the request header
     *      and/or append additional information to the response buffer.
     * \return
     *      True if the corresponding collective operation has been created
     *      and it has finished processing the RPC.
     */
    template <typename Op>
    void
    handleCollectiveOpRpc(const typename Op::RpcType::Request* reqHdr,
            typename Op::RpcType::Response* respHdr, Rpc* rpc)
    {
        CollectiveOpRecord* record = getCollectiveOpRecord(reqHdr->common.opId);
        Op* collectiveOp = record->getOp<Op>();
        if (collectiveOp == NULL) {
            LOG(ERROR, "unable to find the collective op object, opId %u",
                    reqHdr->common.opId);
        } else {
            collectiveOp->handleRpc(reqHdr, respHdr, rpc);
        }
    }

  PRIVATE:
    void initMilliSort(const WireFormat::InitMilliSort::Request* reqHdr,
                WireFormat::InitMilliSort::Response* respHdr,
                Rpc* rpc);
    void startMilliSort(const WireFormat::StartMilliSort::Request* reqHdr,
                WireFormat::StartMilliSort::Response* respHdr,
                Rpc* rpc);
    void treeBcast(const WireFormat::TreeBcast::Request* reqHdr,
                WireFormat::TreeBcast::Response* respHdr,
                Rpc* rpc);
    void flatGather(const WireFormat::FlatGather::Request* reqHdr,
                WireFormat::FlatGather::Response* respHdr,
                Rpc* rpc);
    void allGather(const WireFormat::AllGather::Request* reqHdr,
                WireFormat::AllGather::Response* respHdr,
                Rpc* rpc);
    void allShuffle(const WireFormat::AllShuffle::Request* reqHdr,
                WireFormat::AllShuffle::Response* respHdr,
                Rpc* rpc);
    void sendData(const WireFormat::SendData::Request* reqHdr,
                WireFormat::SendData::Response* respHdr,
                Rpc* rpc);

    /// Shared RAMCloud information.
    Context* context;

    /// This server's ServerConfig, which we export to curious parties.
    /// NULL means we'll reject curious parties.
    const ServerConfig* serverConfig;

    using CommGroupTable = std::unordered_map<int, CommunicationGroup*>;
    CommGroupTable communicationGroupTable;

    enum CollectiveCommunicationOpId {
        GATHER_PIVOTS,
        GATHER_SUPER_PIVOTS,
        BROADCAST_PIVOT_BUCKET_BOUNDARIES,
        ALLSHUFFLE_PIVOTS,
        ALLGATHER_DATA_BUCKET_BOUNDARIES,
        ALLSHUFFLE_DATA,
        FINISH_SORTING,
        INVALID_OP_ID
    };


    enum Stage {
        UNINITIALIZED                   = 0,
        READY_TO_START                  = 1,
        LOCAL_SORT_AND_PICK_PIVOTS      = 2,
        PICK_SUPER_PIVOTS               = 3,
        PICK_PIVOT_BUCKET_BOUNDARIES    = 4,
        PIVOT_BUCKET_SORT               = 5,
        PICK_DATA_BUCKET_BOUNDARIES     = 6,
        DATA_BUCKET_SORT                = 7,
        FINISHED
    };

    ///
    using SortKey = int;

    // ----------------------
    // Computation steps
    // ----------------------

    void partition(std::vector<SortKey>* keys, int numPartitions,
            std::vector<SortKey>* pivots);
    void localSortAndPickPivots();
    void pickSuperPivots();
    void pickPivotBucketBoundaries();
    void pivotBucketSort();
    void pickDataBucketBoundaries();
    void dataBucketSort();

    enum CommunicationGroupId {
        WORLD                   = 0,
        MY_PIVOT_SERVER_GROUP   = 1,
        ALL_PIVOT_SERVERS       = 2
    };

    /// Rank of the pivot server this node belongs to. -1 means unknown.
    int pivotServerRank;

    // True if this node is a pivot server. Only valid when #pivotServerRank
    /// is not -1.
    bool isPivotServer;

    // -------- Node state --------
    /// Which #Stage the node is in (or, the next task to perform).
    // FIXME: remove this variable; replace it with logging
    Stage stage;

    /// # data tuples on each node initially. -1 means unknown.
    int numDataTuples;

    /// # pivot servers. -1 means unknown.
    int numPivotServers;

    /// Keys of the data tuples to be sorted.
    std::vector<SortKey> keys;

    // using Value = uint64_t;
    // std::vector<Value> values;

    std::vector<SortKey> sortedKeys;

    /// Selected keys that evenly divide local #keys on this node into # nodes
    /// partitions.
    std::vector<SortKey> localPivots;

    /// Data bucket boundaries that determine the final destination of each data
    /// tuple on this node. Same on all nodes.
    /// For example, dataBucketBoundaries = {1, 5, 9} means all data are divided
    /// into 3 buckets: (-Inf, 1], (1, 5], and (5, 9].
    std::vector<SortKey> dataBucketBoundaries;

    /// Contains all nodes in the service.
    Tub<CommunicationGroup> world;

    /// Contains the local node, the pivot server it's assigned to (or the local
    /// node itself is a pivot server), and all other nodes that are assigned to
    /// this pivot server.
    Tub<CommunicationGroup> myPivotServerGroup;

    // -------- Pivot server state --------
    /// Pivots gathered from this pivot server's slave nodes. Always empty on
    /// normal nodes.
    std::vector<SortKey> gatheredPivots;

    /// Selected pivots that evenly divide #gatherPivots into # pivot servers
    /// partitions. Different on each pivot server. Always empty on normal
    /// nodes.
    std::vector<SortKey> superPivots;

    /// Pivot bucket boundaries that determine the destination of each pivot on
    /// this pivot server. Same on all pivot servers. Always empty on normal
    /// nodes.
    std::vector<SortKey> pivotBucketBoundaries;

    /// After #gatheredPivots on all nodes are sorted globally and spread across
    /// all pivot servers, this variable holds the local portion of this node.
    /// Always empty on normal nodes.
    std::vector<SortKey> sortedGatheredPivots;

    // TODO: local view of #dataBucketBoundaries
    std::vector<SortKey> partialDataBucketBoundaries;

    /// # pivots on all nodes that are smaller than #sortedGatherPivots. Only
    /// computed on pivot servers.
    int numSmallerPivots;

    /// # pivots on all nodes. Only computed on pivot servers.
    int numPivotsInTotal;

    /// Contains all pivot servers (including the root) in #nodes. Always empty
    /// on normal nodes.
    Tub<CommunicationGroup> allPivotServers;

    // -------- Root node state --------
    /// Contains super pivots collected from all pivot servers. Always empty on
    /// non-root nodes.
    std::vector<SortKey> globalSuperPivots;

    friend class TreeBcast;

    DISALLOW_COPY_AND_ASSIGN(MilliSortService);
};


} // end RAMCloud

#endif  // RAMCLOUD_MILLISORTSERVICE_H
