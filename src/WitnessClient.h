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

#ifndef RAMCLOUD_WITNESSCLIENT_H
#define RAMCLOUD_WITNESSCLIENT_H

#include "ObjectRpcWrapper.h"
#include "ServerId.h"
#include "ServerIdRpcWrapper.h"
#include "ServerMetrics.h"
#include "Transport.h"
#include "WitnessTracker.h"

namespace RAMCloud {

/**
 * This class implements the client-side interface to the witness service.
 * The class contains only static methods, so you shouldn't ever need
 * to instantiate an object.
 */
class WitnessClient {
  public:
    static uint64_t witnessStart(Context* context, ServerId serverId,
            ServerId requestTarget);
    static std::vector<ClientRequest> witnessGetData(Context* context,
            ServerId witnessId, Buffer* response, ServerId crashedServerId,
            uint64_t tableId, uint64_t startKeyHash, uint64_t endKeyHash,
            int16_t* continuation = 0);

  private:
    WitnessClient();
};

class WitnessRecoveryData {
    std::vector<ClientRequest> requests;
};


/**
 * Encapsulates the state of a MasterClient::witnessStart
 * request, allowing it to execute asynchronously.
 */
class WitnessStartRpc : public ServerIdRpcWrapper {
  public:
    WitnessStartRpc(Context* context, ServerId serverId,
                    ServerId requestTarget);
    ~WitnessStartRpc() {}
    uint64_t wait();

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(WitnessStartRpc);
};

/**
 * Encapsulates the state of a MasterClient::witnessGc
 * request, allowing it to execute asynchronously.
 */
class WitnessGcRpc : public ServerIdRpcWrapper {
  public:
    WitnessGcRpc(Context* context, ServerId witnessId, Buffer* response,
            ServerId targetMasterId, uint64_t bufferBasePtr,
            std::vector<WitnessTracker::GcInfo>& gcEntries);
    ~WitnessGcRpc() {}
    void wait(std::vector<ClientRequest>* blockingRequests);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(WitnessGcRpc);
};

/**
 * Encapsulates the state of a MasterClient::witnessGetData
 * request, allowing it to execute asynchronously.
 */
class WitnessGetDataRpc : public ServerIdRpcWrapper {
  public:
    WitnessGetDataRpc(Context* context, ServerId witnessId, Buffer* response,
            ServerId crashedServerId, uint64_t tableId, uint64_t startKeyHash,
            uint64_t endKeyHash, int16_t continuation = 0);
    ~WitnessGetDataRpc() {}
    void wait(int16_t* continuation, std::vector<ClientRequest>* requests);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(WitnessGetDataRpc);
};

} // namespace RAMCloud

#endif // RAMCLOUD_WITNESSCLIENT_H
