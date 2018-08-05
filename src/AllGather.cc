#include "AllGather.h"

namespace RAMCloud {

void
AllGather::handleRpc(const WireFormat::AllGather::Request* reqHdr,
        WireFormat::AllGather::Response* respHdr, Service::Rpc* rpc)
{
    SpinLock::Guard _(mutex);

    if (!receivedFrom) {
        receivedFrom.construct(reqHdr->numSenders);
    }

    uint32_t senderId = reqHdr->senderId;
//    LOG(WARNING, "received AllGatherRpc from sender %u", senderId+1);
    if (receivedFrom->test(senderId)) {
        respHdr->common.status = STATUS_MILLISORT_REDUNDANT_DATA;
        return;
    }
    receivedFrom->set(senderId);

    // Chop off the AllGather header and incorporate the data.
    rpc->requestPayload->truncateFront(sizeof(*reqHdr));
    if (merger) {
        merger(rpc->requestPayload);
    }
}

bool
AllGather::isReady()
{
    if (broadcast && !broadcast->isReady()) {
        return false;
    }
    if (isReceiver) {
        SpinLock::Guard _(mutex);
        if (!receivedFrom ||
                (receivedFrom->count() < receivedFrom->size())) {
            return false;
        }
    }
    return true;
}

void
AllGather::wait()
{
    while (!isReady()) {}
}

}