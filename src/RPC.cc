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

/**
 * Send out an RPC request, to the specified service, using the Transport
 * layer specified when this object was created. We keep a reference to the
 * payload Buffer, in case we need to retransmit this RPC at a later stage.
 *
 * \param[in]  dest        The Service to which we are sending this RPC.
 * \param[in]  rpcPayload  The Buffer containing the payload to put on the wire.
 */
void ClientRPC::startRPC(Service *dest, Buffer* rpcPayload) {
    if (dest == NULL) return;
    if (trans == NULL) return;
    if (rpcPayload == NULL) return;

    try {
        trans->clientSend(dest, rpcPayload, &token);
    } catch (TransportException te) {
        printf("Caught TransportException in ClientRPC::startRPC: %s\n",
               te.message.c_str());
        // TODO(aravindn): Try sending again?
    }
    
    this->rpcPayload = rpcPayload;
}

/**
 * Get this RPC's reply payload. If the reply hasn't been received
 * yet, this call blocks till we get the reply packet. Otherwise, it immediately
 * returns a pointer to the reply Buffer.
 *
 * \return A pointer to a Buffer containing the reply payload.
 */
Buffer* ClientRPC::getReply() {
    if (trans == NULL) return NULL;
    if (!rpcPayload) return NULL;  // Means startRPC() hasn't been called yet.

    if (!replyPayload) {
        replyPayload = new Buffer();
        try {
            trans->clientRecv(replyPayload, &token);
        } catch (TransportException te) {
            printf("Caught TransportException in ClientRPC::getReply: %s\n",
                   te.message.c_str());
        }
    }

    return replyPayload;
}

/**
 * Wait for and return an new RPC request. This function blacks until a new RPC
 * request is received.
 *
 * \return A pointer to a Buffer containing the new RPC request payload.
 */
Buffer* ServerRPC::getRequest() {
    if (trans == NULL) return NULL;

    reqPayload = new Buffer();

    try {
        trans->serverRecv(reqPayload, &token);
    } catch (TransportException te) {
        printf("Caught TransportException in ClientRPC::getRequest: %s\n",
               te.message.c_str());
    }
    
    return reqPayload;
}

/**
 * Send a reply to the RPC request we received. We don't need to keep around a
 * pointer to the reply payload, since we put it in a history list of RPC
 * replies.
 *
 * \param[in]  replyPayload  The Buffer containing the reply payload to put on
 *                           the wire.
 */
void ServerRPC::sendReply(Buffer* replyPayload) {
    if (!reqPayload) return;  // Means getRequest() has not been called yet.
    if (trans == NULL) return;
    if (replyPayload == NULL) return;

    try {
        trans->serverSend(replyPayload, &token);
    } catch (TransportException te) {
        printf("Caught TransportException in ClientRPC::sendReply: %s\n",
               te.message.c_str());
    }
    
    // TODO(aravindn): Put the replyPayload Buffer in a history list so that we
    // can handle retransmitted rpc requests.
}

}  // namespace RAMCloud
