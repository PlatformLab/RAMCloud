/* Copyright (c) 2010 Stanford University
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

#ifndef RAMCLOUD_SERVER_H
#define RAMCLOUD_SERVER_H

#include "Common.h"
#include "ClientException.h"
#include "Metrics.h"
#include "Rpc.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * A base class for RPC servers. Although this class is meant to be subclassed,
 * it serves PINGs so you can use it as a placeholder to aid in development.
 */
class Server {
  public:
    Server() {}
    virtual ~Server() {}
    virtual void run();

    void ping(const PingRpc::Request& reqHdr,
              PingRpc::Response& respHdr,
              Transport::ServerRpc& rpc);

  protected:
    void dispatch(RpcType type, Transport::ServerRpc& rpc);

    const char*
    getString(Buffer& buffer, uint32_t offset, uint32_t length) const;


    /**
     * Helper function to be used in dispatch.
     * Extracts the request from the RPC, allocates and zeros space for the
     * response, and calls the handler.
     * \tparam Rpc
     *      An RPC struct (e.g., PingRpc).
     * \tparam S
     *      The class which defines \a handler and is a subclass of Server.
     * \tparam handler
     *      The method of \a S which executes an RPC.
     */
    template <typename Rpc, typename S,
              void (S::*handler)(const typename Rpc::Request&,
                                 typename Rpc::Response&,
                                 Transport::ServerRpc&)>
    void
    callHandler(Transport::ServerRpc& rpc) {
        const typename Rpc::Request* reqHdr =
            rpc.recvPayload.getStart<typename Rpc::Request>();
        if (reqHdr == NULL)
            throw MessageTooShortError();
        typename Rpc::Response* respHdr =
            new(&rpc.replyPayload, APPEND) typename Rpc::Response;
        /* Clear the response header, so that unused fields are zero;
         * this makes tests more reproducible, and it is also needed
         * to avoid possible security problems where random server
         * info could leak out to clients through unused packet
         * fields. */
        memset(respHdr, 0, sizeof(*respHdr));
        (static_cast<S*>(this)->*handler)(*reqHdr, *respHdr, rpc);
    }

    /**
     * Wait for an incoming RPC request, dispatch it, and send a response.
     */
    template<typename S>
    void
    handleRpc() {
        Transport::ServerRpc& rpc(*transportManager.serverRecv());
        RpcResponseCommon* responseCommon = NULL;
        try {
            const RpcRequestCommon* header;
            header = rpc.recvPayload.getStart<RpcRequestCommon>();
            if (header == NULL)
                throw MessageTooShortError();
            Metrics::setup(header->perfCounter);
            Metrics::mark(MARK_RPC_PROCESSING_BEGIN);
            static_cast<S*>(this)->dispatch(header->type, rpc);
            responseCommon = const_cast<RpcResponseCommon*>(
                rpc.replyPayload.getStart<RpcResponseCommon>());
        } catch (ClientException& e) {
            responseCommon = const_cast<RpcResponseCommon*>(
                rpc.replyPayload.getStart<RpcResponseCommon>());
            if (responseCommon == NULL) {
                responseCommon =
                    new(&rpc.replyPayload, APPEND) RpcResponseCommon;
            }
            responseCommon->status = e.status;
        }
        Metrics::mark(MARK_RPC_PROCESSING_END);
        responseCommon->counterValue = Metrics::read();
        rpc.sendReply();
    }

  private:
    friend class ServerTest;
    DISALLOW_COPY_AND_ASSIGN(Server);
};


} // end RAMCloud

#endif  // RAMCLOUD_SERVER_H
