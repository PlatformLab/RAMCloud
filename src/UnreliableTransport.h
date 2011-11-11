/* Copyright (c) 2011 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef RAMCLOUD_UNRELIABLETRANSPORT_H
#define RAMCLOUD_UNRELIABLETRANSPORT_H

#include <unordered_map>

#include "Common.h"
#include "BoostIntrusive.h"
#include "Driver.h"
#ifdef INFINIBAND
#include "Infiniband.h"
#endif
#include "ObjectPool.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * An transport that does not provide reliability.
 * This should not be relied on for correctness, but it is useful as a
 * comparison point when benchmarking other transports.
 */
class UnreliableTransport : public Transport {
  public:
    explicit UnreliableTransport(Driver* driver);
    ~UnreliableTransport();
    string getServiceLocator();
    SessionRef getSession(const ServiceLocator& serviceLocator,
            uint32_t timeoutMs = 0);

  private:
    struct UnreliableClientRpc : public ClientRpc {
        UnreliableClientRpc(UnreliableTransport& t,
                            Buffer* request,
                            Buffer* response,
                            Driver::Address& serverAddress);
        // expose ClientRpc::markFinished, which is protected
        void markFinished() { ClientRpc::markFinished(); }
        UnreliableTransport& t;
        const uint32_t nonce;
        IntrusiveListHook listEntries;
        DISALLOW_COPY_AND_ASSIGN(UnreliableClientRpc);
    };

    struct UnreliableServerRpc : public ServerRpc {
        UnreliableServerRpc(UnreliableTransport& t,
                            const uint32_t nonce,
                            const Driver::Address* clientAddress);
        void sendReply();
        UnreliableTransport& t;
        const uint32_t nonce;
        const Driver::Address* clientAddress;
        IntrusiveListHook listEntries;
        DISALLOW_COPY_AND_ASSIGN(UnreliableServerRpc);
    };

    struct UnreliableSession : public Session {
        UnreliableSession(UnreliableTransport& t,
                          const ServiceLocator& serviceLocator);
        void abort(const string& message) {}
        ClientRpc* clientSend(Buffer* request, Buffer* response);
        void release();
        UnreliableTransport& t;
        std::unique_ptr<Driver::Address> serverAddress;
        DISALLOW_COPY_AND_ASSIGN(UnreliableSession);
    };

    /// Wire format for packet headers.
    struct Header {
        Header(bool clientToServer, bool moreWillFollow, uint32_t nonce)
            : clientToServer(clientToServer)
            , moreWillFollow(moreWillFollow)
            , nonce(nonce & NONCE_MASK) {}
        bool getClientToServer() { return clientToServer; }
        bool getMoreWillFollow() { return moreWillFollow; }
        uint32_t getNonce() { return nonce; }
        void setClientToServer(bool c) { clientToServer = c; }
        void setMoreWillFollow(bool m) { moreWillFollow = m; }
        void setNonce(uint32_t n) { nonce = (n & NONCE_MASK); }
        enum { NONCE_MASK = 0x3FFFFFFFU };
      private:
        uint32_t clientToServer:1;
        uint32_t moreWillFollow:1;
        uint32_t nonce:30;
    };

    void sendPacketized(const Driver::Address* recipient,
                        Header headerTemplate, Buffer& payload);

    std::unique_ptr<Driver> driver;

    INTRUSIVE_LIST_TYPEDEF(UnreliableClientRpc, listEntries)
        ClientRpcList;

    /// Holds client RPCs that are waiting for a complete response.
    ClientRpcList clientPendingList;

    ObjectPool<UnreliableServerRpc> serverRpcPool;

    INTRUSIVE_LIST_TYPEDEF(UnreliableServerRpc, listEntries)
        ServerRpcList;

    /// Hold server RPCs that are waiting for a complete request.
    ServerRpcList serverPendingList;

#ifdef INFINIBAND
    /// work-around for cost of allocating Infiniband address handles
    std::unordered_map<uint64_t, ibv_ah*> infinibandAddressHandleCache;
#endif

    DISALLOW_COPY_AND_ASSIGN(UnreliableTransport);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_UNRELIABLETRANSPORT_H
