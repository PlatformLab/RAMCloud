#include "AllShuffle.h"

namespace RAMCloud {

void
AllShuffle::closeSendBuffer(int rank)
{
    if (rank == group->rank) {
        SpinLock::Guard _(mutex);
        merger(&localData);
    } else {
        outstandingRpcs++;
        outgoingRpcs[rank]->send();
    }
}

Buffer*
AllShuffle::getSendBuffer(int rank)
{
    return rank == group->rank ? &localData : &outgoingRpcs[rank]->request;
}

void
AllShuffle::handleRpc(const WireFormat::AllShuffle::Request* reqHdr,
        WireFormat::AllShuffle::Response* respHdr, Service::Rpc* rpc)
{
    SpinLock::Guard _(mutex);

    uint32_t senderId = reqHdr->senderId;
//    LOG(WARNING, "received AllShuffleRpc from server %u", senderId + 1);
    if (receivedFrom.test(senderId)) {
        respHdr->common.status = STATUS_MILLISORT_REDUNDANT_DATA;
        return;
    }
    receivedFrom.set(senderId);

    // Chop off the AllShuffle header and incorporate the data.
    rpc->requestPayload->truncateFront(sizeof(*reqHdr));
    if (merger) {
        merger(rpc->requestPayload);
    }
}

bool
AllShuffle::isReady()
{
    for (auto& rpc : outgoingRpcs) {
        if (rpc && rpc->isReady()) {
            outstandingRpcs--;
            rpc.destroy();
        }
    }

    SpinLock::Guard _(mutex);
    return (outstandingRpcs == 0) &&
           (int(receivedFrom.count()) == group->size());
}

void
AllShuffle::wait()
{
    while (!isReady());
}

}