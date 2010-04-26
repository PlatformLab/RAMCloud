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
 * \file RPC.cc Implementation of the ClientRPC and ServerRPC classes.
 */

#include <Buffer.h>
#include <RPC.h>

namespace RAMCloud {

void ClientRPC::startRPC(Service *dest, Buffer* rpcPayload) {
    // Send the RPC. Hang onto the buffer in case we need to retransmit.

    if (/*dest->getServiceId() == 0 || */trans == NULL) {
        return;
    }

    try {
        trans->clientSend(dest, rpcPayload, &token);
    } catch (TransportException te) {
        printf("Caught TransportException in ClientRPC::startRPC: %s\n",
               te.message.c_str());
    }
    
    this->rpcPayload = rpcPayload;
}

Buffer* ClientRPC::getReply() {
    // Check if replyPayload is set. Call blocking recv if not.
    if (trans == NULL) return NULL;

    if (!replyPayload) {
        replyPayload = new Buffer();
        try {
            trans->clientRecv(replyPayload, &token);
        } catch (TransportException te) {
            printf("Caught TransportException in ClientRPC::startRPC: %s\n",
                   te.message.c_str());
        }
    }

    return replyPayload;
}

Buffer* ServerRPC::getRequest() {
    // Block on serverRecv;
    if (trans == NULL) return NULL;
    reqPayload = new Buffer();

    try {
        trans->serverRecv(reqPayload, &token);
    } catch (TransportException te) {
        printf("Caught TransportException in ClientRPC::startRPC: %s\n",
               te.message.c_str());
    }
    
    return reqPayload;
}

void ServerRPC::sendReply(Buffer* replyPayload) {
    if (trans == NULL) return;
    // Send the RPC. Don't hang onto the buffer, put it in the history list of
    // replies.
    try {
        trans->serverSend(replyPayload, &token);
    } catch (TransportException te) {
        printf("Caught TransportException in ClientRPC::startRPC: %s\n",
               te.message.c_str());
    }
}

}  // namespace RAMCloud
