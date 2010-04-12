/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file RPC.h Header for the ClientRPC and ServerRPC.
 */

#ifndef RAMCLOUD_RPC_H
#define RAMCLOUD_RPC_H

#include <Buffer.h>
#include <Common.h>
#include <Service.h>

namespace RAMCloud {

// Dummy Transport implementation, so that it compiles.

struct ClientTransportToken {
    Service *s;
    uint64_t rpcId;
    ClientTransportToken() : s(NULL), rpcId(0) { }
};

struct ServerTransportToken {
    Service s;
    uint64_t rpcId;
    ServerTransportToken() : s(), rpcId(0) { }
};

class Transport {
  public:
    inline void clientSend(Service *s, Buffer* payload,
                           ClientTransportToken *token_) { }

    inline void clientRecv(Buffer *payload_, ClientTransportToken *token) { }

    inline void serverRecv(Buffer* payload_, ServerTransportToken *token_) { }

    inline void serverSend(Buffer* payload, ServerTransportToken *token) { }
};

inline Transport* transport() { return new Transport(); }

// End Dummy Transport implementation

class ClientRPC {
  public:
    void startRPC(Service *dest, Buffer* rpcPayload);

    Buffer* getReply();

    inline Buffer* getRPCPayload() { return rpcPayload; }

    ClientRPC() : rpcPayload(NULL), replyPayload(NULL), token() { }

  private:
    Buffer* rpcPayload;
    Buffer* replyPayload;
    ClientTransportToken token;
};

class ServerRPC {
  public:
    Buffer* getRequest();

    void sendReply(Buffer* replyPayload);

    ServerRPC() : reqPayload(NULL), replyPayload(NULL), token() { }

  private:
    Buffer* reqPayload;
    Buffer* replyPayload;
    ServerTransportToken token;
};

}  // namespace RAMCloud

#endif
