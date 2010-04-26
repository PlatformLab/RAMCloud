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
#include <TCPTransport.h>

namespace RAMCloud {

/**
 * \class ClientRPC
 *
 * This class represents a single RPC made by a client process. This process
 * maybe an actual client library process on an application machine, or a server
 * process talking to its backups. In general, any process which sends an RPC
 * first and then waits for a reply, either immediately or after some time, uses
 * this class.
 */
class ClientRPC {
  public:
    void startRPC(Service *dest, Buffer* rpcPayload);

    Buffer* getReply();

    explicit ClientRPC(Transport* transIn)
            : rpcPayload(NULL), replyPayload(NULL), token(), trans(transIn) { }

  private:
    Buffer* rpcPayload;            // A pointer to keep track of the Buffer
                                   // which contains the intial payload we send
                                   // out. This Buffer is allocated by the
                                   // caller of startRPC(). We keep track of it,
                                   // in case we need to retransmit the RPC at a
                                   // later stage.
    Buffer* replyPayload;          // Buffer space, allocated by us, which will
                                   // be used to store the reply to the RPC we
                                   // sent out.
    Transport::ClientToken token;  // The token which is returned from startRPC.
                                   // The Transport layer uses this token to
                                   // keep track of this particular RPC, and its
                                   // destination, in its system. Thus, this
                                   // token must be passed when we call
                                   // getReply().
    Transport *trans;              // The Transport layer that this RPC uses.
                                   // Passed to us as part of the constructor.

    friend class ClientRPCTest;
    DISALLOW_COPY_AND_ASSIGN(ClientRPC);
};

/**
 * \class ServerRPC
 *
 * This class represents a single RPC made by a server process, for example, a
 * master or backup server. Any process which first listens for incoming RPCs,
 * and then sends back a response to that RPC may use this class.
 */
class ServerRPC {
  public:
    Buffer* getRequest();

    void sendReply(Buffer* replyPayload);

    explicit ServerRPC(Transport *transIn)
            : reqPayload(NULL), replyPayload(NULL), token(), trans(transIn) { }

  private:
    Buffer* reqPayload;            // Buffer space for the payload of the RPC
                                   // request we have received. This Buffer is
                                   // allocated by us.
    Buffer* replyPayload;          // A pointer to the Buffer containing the
                                   // response for the RPC. We need to keep
                                   // track of this in case we have to
                                   // retransmit it again at a later stage.
    Transport::ServerToken token;  // The token which is returned from
                                   // getRequest(). The Transport layer uses
                                   // this toekn to keep track of this
                                   // particular RPC, and it's destination, in
                                   // its system.
    Transport *trans;              // The Transport layer that this RPC
                                   // uses. Passed to us as part of the
                                   // constructor.

    friend class ServerRPCTest;
    DISALLOW_COPY_AND_ASSIGN(ServerRPC);
};

}  // namespace RAMCloud

#endif
