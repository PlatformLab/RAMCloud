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

#include "MilliSortClient.h"
#include "MilliSortService.h"
#include "AllGather.h"
#include "AllShuffle.h"
#include "Broadcast.h"
#include "Gather.h"

namespace RAMCloud {

/**
 * Construct an MilliSortService.
 *
 * \param context
 *      Overall information about the RAMCloud server or client. The new
 *      service will be registered in this context.
 * \param config
 *      Contains various parameters that configure the operation of
 *      this server.
 */
MilliSortService::MilliSortService(Context* context,
        const ServerConfig* serverConfig)
    : context(context)
    , serverConfig(serverConfig)
    , communicationGroupTable()
    , pivotServerRank(-1)
    , isPivotServer()
    , stage(UNINITIALIZED)
    , numDataTuples(-1)
    , numPivotServers(-1)
    , keys()
    , sortedKeys()
    , localPivots()
    , dataBucketBoundaries()
    , world()
    , myPivotServerGroup()
    , gatheredPivots()
    , superPivots()
    , pivotBucketBoundaries()
    , sortedGatheredPivots()
    , partialDataBucketBoundaries()
    , numSmallerPivots()
    , numPivotsInTotal()
    , allPivotServers()
    , globalSuperPivots()
{
    context->services[WireFormat::MILLISORT_SERVICE] = this;
}

MilliSortService::~MilliSortService()
{
    context->services[WireFormat::MILLISORT_SERVICE] = NULL;
}

/**
 * Dispatch an RPC to the right handler based on its opcode.
 */
void
MilliSortService::dispatch(WireFormat::Opcode opcode, Rpc* rpc)
{
    switch (opcode) {
        case WireFormat::InitMilliSort::opcode:
            callHandler<WireFormat::InitMilliSort, MilliSortService,
                        &MilliSortService::initMilliSort>(rpc);
            break;
        case WireFormat::StartMilliSort::opcode:
            callHandler<WireFormat::StartMilliSort, MilliSortService,
                        &MilliSortService::startMilliSort>(rpc);
            break;
        case WireFormat::TreeBcast::opcode:
            callHandler<WireFormat::TreeBcast, MilliSortService,
                        &MilliSortService::treeBcast>(rpc);
            break;
        case WireFormat::FlatGather::opcode:
            callHandler<WireFormat::FlatGather, MilliSortService,
                        &MilliSortService::flatGather>(rpc);
            break;
        case WireFormat::AllGather::opcode:
            callHandler<WireFormat::AllGather, MilliSortService,
                        &MilliSortService::allGather>(rpc);
            break;
        case WireFormat::AllShuffle::opcode:
            callHandler<WireFormat::AllShuffle, MilliSortService,
                        &MilliSortService::allShuffle>(rpc);
            break;
        case WireFormat::SendData::opcode:
            callHandler<WireFormat::SendData, MilliSortService,
                        &MilliSortService::sendData>(rpc);
            break;
        default:
            prepareErrorResponse(rpc->replyPayload, 
                    STATUS_UNIMPLEMENTED_REQUEST);
    }
}


/**
 * Top-level server method to handle the INIT_MILLISORT request.
 *
 * \copydetails Service::ping
 */
void
MilliSortService::initMilliSort(const WireFormat::InitMilliSort::Request* reqHdr,
        WireFormat::InitMilliSort::Response* respHdr, Rpc* rpc)
{
    // FIXME: use timetrace
//    LOG(NOTICE, "Received INIT_MILLISORT request, fromClient = %u, "
//            "# tuples = %u", reqHdr->fromClient, reqHdr->dataTuplesPerServer);

    // We are busy serving another sorting request.
    if ((READY_TO_START < stage) && (stage < FINISHED)) {
        respHdr->common.status = STATUS_SORTING_IN_PROGRESS;
        return;
    }

    initWorld();

    // Initialization request from an external client should only be processed
    // by the root node, which would then broadcast the request to all nodes.
    if (reqHdr->fromClient) {
        if (world->rank > 0) {
            respHdr->common.status = STATUS_MILLISORT_ERROR;
            return;
        }

        TreeBcast treeBcast(context, world.get());
        treeBcast.send<InitMilliSortRpc>(reqHdr->dataTuplesPerServer,
                reqHdr->nodesPerPivotServer, false);
        treeBcast.wait();

        Buffer* result = treeBcast.getResult();
        uint32_t offset = 0;
        while (offset < result->size()) {
            respHdr->numNodesInited +=
                    result->read<WireFormat::InitMilliSort::Response>(
                    &offset)->numNodesInited;
        }

//        for (int i = 0; i < world->size(); i++) {
//            // TODO: not sure if I can send RPC to myself in RAMCloud
////            if (serverId == world->getNode(i)) {
////                continue;
////            }
//            InitMilliSortRpc initRpc(context, world->getNode(i),
//                    reqHdr->dataTuplesPerServer, reqHdr->nodesPerPivotServer,
//                    false);
//            initRpc.wait();
//            respHdr->numNodesInited += initRpc.getNodesInited();
//        }

        return;
    }

    numDataTuples = reqHdr->dataTuplesPerServer;
    int nodesPerPivotServer = (int) reqHdr->nodesPerPivotServer;
    pivotServerRank = world->rank / nodesPerPivotServer *
            nodesPerPivotServer;
    isPivotServer = (pivotServerRank == world->rank);
    numPivotServers = world->size() / nodesPerPivotServer +
            (world->size() % nodesPerPivotServer > 0 ? 1 : 0);

    // Construct communication sub-groups.
    communicationGroupTable.clear();
    registerCommunicationGroup(world.get());
    vector<ServerId> nodes;
    for (int i = 0; i < nodesPerPivotServer; i++) {
        if (pivotServerRank + i >= world->size()) {
            break;
        }
        nodes.push_back(world->getNode(pivotServerRank + i));
    }
    myPivotServerGroup.construct(MY_PIVOT_SERVER_GROUP,
            world->rank % nodesPerPivotServer, nodes);
    registerCommunicationGroup(myPivotServerGroup.get());
    if (isPivotServer) {
        nodes.clear();
        for (int i = 0; i < numPivotServers; i++) {
            nodes.push_back(world->getNode(i * nodesPerPivotServer));
        }
        allPivotServers.construct(ALL_PIVOT_SERVERS,
                world->rank / nodesPerPivotServer, nodes);
        registerCommunicationGroup(allPivotServers.get());
    } else {
        allPivotServers.destroy();
    }

    // Generate data tuples.
    for (int i = 0; i < numDataTuples; i++) {
        // TODO: generate random keys
//        keys.push_back(rand() % 10000);
        keys.push_back(world->rank + i * world->size());
    }
    sortedKeys.clear();

    //
    stage = READY_TO_START;
    localPivots.clear();
    dataBucketBoundaries.clear();
    gatheredPivots.clear();
    superPivots.clear();
    pivotBucketBoundaries.clear();
    sortedGatheredPivots.clear();
    partialDataBucketBoundaries.clear();
    numSmallerPivots = 0;
    numPivotsInTotal = 0;
    globalSuperPivots.clear();

    //
    respHdr->numNodesInited = 1;
}

/**
 * Top-level server method to handle the START_MILLISORT request.
 *
 * \copydetails Service::ping
 */
void
MilliSortService::startMilliSort(const WireFormat::StartMilliSort::Request* reqHdr,
        WireFormat::StartMilliSort::Response* respHdr, Rpc* rpc)
{
    if (stage == UNINITIALIZED) {
        respHdr->common.status = STATUS_MILLISORT_UNINITIALIZED;
        return;
    } else if (stage > READY_TO_START) {
        respHdr->common.status = STATUS_SORTING_IN_PROGRESS;
        return;
    }

    // Start request from external client should only be processed by the root
    // node, which would then broadcast the start request to all nodes.
    if (reqHdr->fromClient) {
        if (world->rank > 0) {
            respHdr->common.status = STATUS_MILLISORT_ERROR;
            return;
        }

        // Start broadcasting the start request.
        TreeBcast startBcast(context, world.get());
        startBcast.send<StartMilliSortRpc>(false);

        // FIXME: I am throwing away an entire worker thread here; should I use
        // sleep? Arachne?
//        while (!startBcast.isReady()) {
//            yield();
//        }
        startBcast.wait();
        return;
    }

    // Kick start the sorting process.
    localSortAndPickPivots();
}

void
MilliSortService::partition(std::vector<SortKey>* keys, int numPartitions,
        std::vector<SortKey>* pivots)
{
    assert(pivots->empty());
    // Let P be the number of partitions. Pick P pivots that divide #keys into
    // P partitions as evenly as possible. Since the sizes of any two partitions
    // differ by at most one, we have:
    //      s * k + (s + 1) * (P - k) = numKeys
    // where s is the number of keys in smaller partitions and k is the number
    // of smaller partitions.
    int numKeys = downCast<int>(keys->size());
    int s = numKeys / numPartitions;
    int k = numPartitions * (s + 1) - numKeys;
    int pivotIdx = -1;
    for (int i = 0; i < numPartitions; i++) {
        if (i < k) {
            pivotIdx += s;
        } else {
            pivotIdx += s + 1;
        }
        pivots->push_back((*keys)[pivotIdx]);
    }
}

void
MilliSortService::localSortAndPickPivots()
{
    // FIXME: use time trace
    LOG(WARNING, "localSortAndPickPivots started");

    stage = LOCAL_SORT_AND_PICK_PIVOTS;
    std::sort(keys.begin(), keys.end());
    partition(&keys, std::min((int)keys.size(), world->size()), &localPivots);

    // Merge pivots from slave nodes as they arrive.
    auto merger = [this] (Buffer* pivotsBuffer) {
            uint32_t offset = 0;
            while (offset < pivotsBuffer->size()) {
                gatheredPivots.push_back(*pivotsBuffer->read<SortKey>(&offset));
            }
            std::sort(gatheredPivots.begin(), gatheredPivots.end());
    };

    // Initiate the gather op that collects pivots to the pivot server.
    // FIXME: if all we gonna do is to call and wait, we might as well use
    // invokeCollectiveOp and do the collectiveOpTable cleanup inside.
    Tub<FlatGather> gatherPivots;
    invokeCollectiveOp<FlatGather>(gatherPivots, GATHER_PIVOTS, context,
            myPivotServerGroup.get(), 0, (uint32_t) localPivots.size(),
            localPivots.data(), &merger);
    gatherPivots->wait();
    collectiveOpTable.erase(GATHER_PIVOTS);

    // Pivot servers will advance to pick super pivots when the gather op
    // completes. Normal nodes will wait to receive data bucket boundaries.
    if (isPivotServer) {
        LOG(WARNING, "#localPivots %lu, #gatheredPivots %lu",
                localPivots.size(), gatheredPivots.size());
        pickSuperPivots();
    } else {
        pickDataBucketBoundaries();
    }
}

void
MilliSortService::pickSuperPivots()
{
    LOG(WARNING, "pickSuperPivots started");

    assert(isPivotServer);
    stage = PICK_SUPER_PIVOTS;
    partition(&gatheredPivots, allPivotServers->size(), &superPivots);

    // Merge super pivots from pivot servers as they arrive.
    auto merger = [this] (Buffer* pivotsBuf) {
            uint32_t offset = 0;
            while (offset < pivotsBuf->size()) {
                globalSuperPivots.push_back(*pivotsBuf->read<SortKey>(&offset));
            }
            std::sort(globalSuperPivots.begin(), globalSuperPivots.end());
    };

    // Initiate the gather op that collects super pivots to the root node.
    Tub<FlatGather> gatherSuperPivots;
    invokeCollectiveOp<FlatGather>(gatherSuperPivots, GATHER_SUPER_PIVOTS,
            context, allPivotServers.get(), 0, (uint32_t) superPivots.size(),
            superPivots.data(), &merger);
    // fixme: this is duplicated everywhere
    gatherSuperPivots->wait();
    collectiveOpTable.erase(GATHER_SUPER_PIVOTS);

    // The root node will advance to pick pivot bucket boundaries when the
    // gather op completes. Other pivot servers will wait to receive pivot
    // bucket boundaries.
    if (world->rank == 0) {
        pickPivotBucketBoundaries();
    }
}

void
MilliSortService::pickPivotBucketBoundaries()
{
    LOG(WARNING, "pickPivotBucketBoundaries started");

    assert(world->rank == 0);
    stage = PICK_PIVOT_BUCKET_BOUNDARIES;
    partition(&globalSuperPivots, allPivotServers->size(),
            &pivotBucketBoundaries);

    // Broadcast computed pivot bucket boundaries to the other pivot servers
    // (excluding ourselves).
    // TODO: can we unify the API of broadcast and other collective op?
    TreeBcast bcast(context, allPivotServers.get());
    bcast.send<SendDataRpc>(BROADCAST_PIVOT_BUCKET_BOUNDARIES,
            // TODO: simplify the SendDataRpc api?
            downCast<uint32_t>(sizeof(SortKey) * pivotBucketBoundaries.size()),
            pivotBucketBoundaries.data());
    bcast.wait();

    pivotBucketSort();
}

void
MilliSortService::pivotBucketSort()
{
    LOG(WARNING, "pivotBucketSort started, #pivotBucketBoundaries %lu",
            pivotBucketBoundaries.size());

    assert(isPivotServer);
    stage = PIVOT_BUCKET_SORT;

    // Merge pivots from pivot servers as they arrive.
    auto merger = [this] (Buffer* data) {
        uint32_t offset = 0;
        numSmallerPivots += *data->read<uint32_t>(&offset);
        numPivotsInTotal += *data->read<uint32_t>(&offset);
        while (offset < data->size()) {
            sortedGatheredPivots.push_back(*data->read<SortKey>(&offset));
        }

        // FIXME: this should really be an in-place parallel merge operation
        std::sort(sortedGatheredPivots.begin(), sortedGatheredPivots.end());
    };

    // Send pivots to their designated pivot servers determined by the pivot
    // bucket boundaries.
    Tub<AllShuffle> pivotsShuffle;
    invokeCollectiveOp<AllShuffle>(pivotsShuffle, ALLSHUFFLE_PIVOTS, context,
            allPivotServers.get(), &merger);
    uint32_t k = 0;
    for (int rank = 0; rank < int(pivotBucketBoundaries.size()); rank++) {
        // Prepare the data to send to node `rank`.
        uint32_t numSmallerPivots = k;
        uint32_t numPivots = downCast<uint32_t>(gatheredPivots.size());

        // The data message starts with two 32-bit numbers: the number of pivots
        // smaller than the current bucket and the total number of pivots this
        // pivot server has. They are used to compute the data bucket boundaries
        // in the next step.
        Buffer* buffer = pivotsShuffle->getSendBuffer(rank);
        buffer->appendCopy(&numSmallerPivots);
        buffer->appendCopy(&numPivots);

        SortKey boundary = pivotBucketBoundaries[rank];
        while ((k < gatheredPivots.size()) && (gatheredPivots[k] <= boundary)) {
            buffer->appendCopy(&gatheredPivots[k]);
            k++;
        }
        pivotsShuffle->closeSendBuffer(rank);
    }
    pivotsShuffle->wait();
    collectiveOpTable.erase(ALLSHUFFLE_PIVOTS);

    // Pivot servers will advance to pick data bucket boundaries when the
    // shuffle completes.
    pickDataBucketBoundaries();
}

void
MilliSortService::pickDataBucketBoundaries()
{
    LOG(WARNING, "pickDataBucketBoundaries started");

    stage = PICK_DATA_BUCKET_BOUNDARIES;
    if (isPivotServer) {
        // TODO: doc. that it's copied from sortAndPartition and modified.
        int s = numPivotsInTotal / world->size();
        int k = world->size() * (s + 1) - numPivotsInTotal;
        int globalIdx = -1;
        for (int i = 0; i < world->size(); i++) {
            if (i < k) {
                globalIdx += s;
            } else {
                globalIdx += s + 1;
            }

            int localIdx = globalIdx - numSmallerPivots;
            if ((0 <= localIdx) && (localIdx < (int) sortedGatheredPivots.size())) {
                partialDataBucketBoundaries.push_back(
                        sortedGatheredPivots[localIdx]);
            }
        }
    }

    // Merge data bucket boundaries from pivot servers as they arrive.
    auto merger = [this] (Buffer* pivotsBuffer) {
            uint32_t offset = 0;
            while (offset < pivotsBuffer->size()) {
                SortKey boundary = *pivotsBuffer->read<SortKey>(&offset);
//                LOG(WARNING, "boundary %d", boundary);
                dataBucketBoundaries.push_back(boundary);
            }
            std::sort(dataBucketBoundaries.begin(), dataBucketBoundaries.end());
    };

    // TODO: I accidentally used ruleEngine.createAndSchedule<AllGather> and
    // spent 30 mins debugging. How to prevent this kind of error?
    Tub<AllGather> allGather;
    if (isPivotServer) {
        LOG(WARNING, "# partialDataBucketBoundaries %lu",
                partialDataBucketBoundaries.size());
        invokeCollectiveOp<AllGather>(allGather,
                ALLGATHER_DATA_BUCKET_BOUNDARIES, context,
                allPivotServers.get(), world.get(),
                (uint32_t) partialDataBucketBoundaries.size(),
                partialDataBucketBoundaries.data(), &merger);
    } else {
        invokeCollectiveOp<AllGather>(allGather,
                ALLGATHER_DATA_BUCKET_BOUNDARIES, context, &merger);
    }
    allGather->wait();
    collectiveOpTable.erase(ALLGATHER_DATA_BUCKET_BOUNDARIES);

    // All nodes will advance to the final data shuffle when the all-gather
    // completes.
    dataBucketSort();
}

void
MilliSortService::dataBucketSort()
{
    LOG(WARNING, "dataBucketSort started, # dataBucketBoundaries %lu",
            dataBucketBoundaries.size());
    // FIXME: add assertions at each step.

    stage = DATA_BUCKET_SORT;
    // TODO: shall we do the merge online? actually, even better, partialDataBucketBoundaries
    // are sorted globally, we know exactly where to insert even they come!
    std::sort(dataBucketBoundaries.begin(), dataBucketBoundaries.end());

    // Merge data from other nodes as they arrive.
    auto merger = [this] (Buffer* data) {
        uint32_t offset = 0;
        while (offset < data->size()) {
            sortedKeys.push_back(*data->read<SortKey>(&offset));
        }
        // FIXME: this should really be an in-place parallel merge operation
        std::sort(sortedKeys.begin(), sortedKeys.end());
    };

    // Send data to their designated nodes determined by the data bucket
    // boundaries.
    Tub<AllShuffle> dataShuffle;
    invokeCollectiveOp<AllShuffle>(dataShuffle, ALLSHUFFLE_DATA, context,
            world.get(), &merger);
    uint32_t k = 0;
    for (int rank = 0; rank < int(dataBucketBoundaries.size()); rank++) {
        Buffer* buffer = dataShuffle->getSendBuffer(rank);
        SortKey boundary = dataBucketBoundaries[rank];
        while ((k < keys.size()) && (keys[k] <= boundary)) {
            buffer->appendCopy(&keys[k]);
            k++;
        }
        dataShuffle->closeSendBuffer(rank);
    }
    dataShuffle->wait();
    collectiveOpTable.erase(ALLSHUFFLE_DATA);

    stage = FINISHED;
    string sortResult;
    for (SortKey& key : sortedKeys) {
        sortResult += std::to_string(key);
        sortResult += " ";
    }
    LOG(WARNING, "result: %s", sortResult.c_str());
}


void
MilliSortService::treeBcast(const WireFormat::TreeBcast::Request* reqHdr,
            WireFormat::TreeBcast::Response* respHdr, Rpc* rpc)
{
    TreeBcast::handleRpc(context, reqHdr, rpc);
}

void
MilliSortService::flatGather(const WireFormat::FlatGather::Request* reqHdr,
            WireFormat::FlatGather::Response* respHdr, Rpc* rpc)
{
    handleCollectiveOpRpc<FlatGather>(reqHdr, respHdr, rpc);
}

void
MilliSortService::allGather(const WireFormat::AllGather::Request* reqHdr,
            WireFormat::AllGather::Response* respHdr, Rpc* rpc)
{
    handleCollectiveOpRpc<AllGather>(reqHdr, respHdr, rpc);
}

void
MilliSortService::allShuffle(const WireFormat::AllShuffle::Request* reqHdr,
            WireFormat::AllShuffle::Response* respHdr, Rpc* rpc)
{
    handleCollectiveOpRpc<AllShuffle>(reqHdr, respHdr, rpc);
}

void
MilliSortService::sendData(const WireFormat::SendData::Request* reqHdr,
            WireFormat::SendData::Response* respHdr, Rpc* rpc)
{
    switch (reqHdr->dataId) {
        // TODO: hmm, right now we handle incoming data differently in Bcast &
        // gather; in gather, we use merger to incorporate incoming data and
        // no need for SendDataRpc, while here we use SendData RPC handler to
        // to incorporate the data. Think about how to make it uniform.
        case BROADCAST_PIVOT_BUCKET_BOUNDARIES: {
            // FIXME: info leak in handler; have to handle differently based on
            // rank
            if (world->rank > 0) {
                Buffer *request = rpc->requestPayload;
                for (uint32_t offset = sizeof(*reqHdr);
                     offset < request->size();) {
                    pivotBucketBoundaries.push_back(
                            *request->read<SortKey>(&offset));
                }
                rpc->sendReply();
                pivotBucketSort();
            }
            break;
        }
        default:
            assert(false);
    }
}

} // namespace RAMCloud
