#ifndef GRANULARCOMPUTING_ALLGATHER_H
#define GRANULARCOMPUTING_ALLGATHER_H

#include <boost/dynamic_bitset.hpp>

#include "Broadcast.h"
#include "Buffer.h"
#include "CommunicationGroup.h"
#include "RpcWrapper.h"
#include "RuleEngine.h"
#include "Service.h"
#include "WireFormat.h"

namespace RAMCloud {

class AllGather {
  public:

    using RpcType = WireFormat::AllGather;

    /**
     * Constructor used by nodes who are senders in the all-gather operation.
     *
     * \tparam T
     * \tparam Merger
     * \param opId
     * \param context
     * \param senderGroup
     *      Nodes responsible for sending data in the all-gather operation.
     * \param receiverGroup
     *      Nodes responsible for receiving data in the all-gather operation.
     * \param numElements
     * \param elements
     * \param merger
     */
    template <typename T, typename Merger>
    explicit AllGather(int opId, Context* context,
            CommunicationGroup* senderGroup, CommunicationGroup* receiverGroup,
            int numElements, const T* elements, Merger* merger)
        // FIXME: right now, we assume a sender is always a receiver (i.e.,
        // senderGroup is a subset of the receiverGroup). Need to set this var.
        // properly based on receiverGroup in the future (e.g. test if
        // receiverGroup->rank is negative).
        : isReceiver(true)
        , merger(*merger)
        , opId(opId)
        , mutex("receivedFrom")
        , receivedFrom()
        , sendDataRpc()
        , broadcast()
    {
        // FIXME: right now, BCAST API assumes the initial broadcaster is
        // inside the communication group, which is too restricted.
        broadcast.construct(context, receiverGroup);
        broadcast->send<AllGatherRpc>(opId,
                (uint32_t) senderGroup->size(), (uint32_t) senderGroup->rank,
                uint32_t(sizeof(T) * numElements), elements);
    }

    /**
     * Constructor used by nodes who are purely receivers in the all-gather
     * operation.
     *
     * \tparam Merger
     *      Concrete type of #merger.
     * \param opId
     *      Unique identifier of this collective operation.
     * \param context
     *      Service context.
     * \param merger
     *      Used to incorporate received data.
     */
    template <typename Merger>
    explicit AllGather(int opId, Context* context, Merger* merger)
        : isReceiver(true)
        , merger(*merger)
        , opId(opId)
        , mutex("receivedFrom")
        , receivedFrom()
        , sendDataRpc()
        , broadcast()
    {}

    ~AllGather() = default;

    void handleRpc(const WireFormat::AllGather::Request* reqHdr,
            WireFormat::AllGather::Response* respHdr, Service::Rpc* rpc);
    bool isReady();
    void wait();

  PRIVATE:
    class AllGatherRpc : public ServerIdRpcWrapper {
      public:
        explicit AllGatherRpc(Context* context, ServerId serverId,
                int opId, int numSenders, int senderId, uint32_t length,
                const void* data)
            : ServerIdRpcWrapper(context, serverId,
                sizeof(WireFormat::AllGather::Response))
        {
            WireFormat::AllGather::Request* reqHdr(
                    allocHeader<WireFormat::AllGather>(downCast<uint32_t>(opId)));
            reqHdr->numSenders = downCast<uint32_t>(numSenders);
            reqHdr->senderId = downCast<uint32_t>(senderId);
            request.appendExternal(data, length);
            send();
        }

        static void
        appendRequest(Buffer* request, int opId, int numSenders, int senderId,
                uint32_t length, const void* data)
        {
            WireFormat::AllGather::Request* reqHdr(
                    allocHeader<WireFormat::AllGather>(request, downCast<uint32_t>(opId)));
            reqHdr->numSenders = downCast<uint32_t>(numSenders);
            reqHdr->senderId = downCast<uint32_t>(senderId);
            request->appendExternal(data, length);
        }

        /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
        void wait()
        {
            waitAndCheckErrors();
            // Chop off the AllGather response header.
            response->truncateFront(responseHeaderLength);
        }

        static const uint32_t responseHeaderLength =
                sizeof(WireFormat::AllGather::Response);

        DISALLOW_COPY_AND_ASSIGN(AllGatherRpc);
    };

    bool isReceiver;

    std::function<void(Buffer*)> merger;

    int opId;

    SpinLock mutex;

    /// Used to record the nodes from which we have received data. Only present
    /// on receiver nodes.
    Tub<boost::dynamic_bitset<>> receivedFrom;

    /// Only present if #isSender is true.
    Tub<AllGatherRpc> sendDataRpc;

    /// Only present if #isSender is true.
    Tub<TreeBcast> broadcast;

    DISALLOW_COPY_AND_ASSIGN(AllGather)
};

}

#endif