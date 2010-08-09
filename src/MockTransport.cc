/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file
 * Implementation of a class that implements Transport for tests,
 * without an actual network.
 */

#include <MockTransport.h>

namespace RAMCloud {

//------------------------------
// MockTransport class
//------------------------------

/**
 * Construct a MockTransport.
 */
MockTransport::MockTransport()
            : outputLog(), inputMessage(NULL),
              serverRecvCount(0), serverSendCount(0),
              clientSendCount(0), clientRecvCount(0) { }

/**
 * Wait for an incoming request. This is a fake method that uses
 * a request message explicitly provided by the test, or an empty
 * buffer if none was provided.
 */
Transport::ServerRPC*
MockTransport::serverRecv() {
    return new MockServerRPC(this);
}

/**
 * Issue an RPC request using this transport.
 *
 * This is a fake method; it simply logs information about the request.
 *
 * \param service
 *      Indicates which service the request should be sent to.
 * \param payload
 *      Contents of the request message.
 * \param[out] response
 *      When a response arrives, the response message will be made
 *      available via this Buffer.
 *
 * \return  A pointer to the allocated space or \c NULL if there is not enough
 *          space in this Allocation.
 */
Transport::ClientRPC*
MockTransport::clientSend(const Service* service, Buffer* payload,
                          Buffer* response) {
    if (outputLog.length() != 0) {
        outputLog.append(" | ");
    }
    outputLog.append("clientSend: ");
    outputLog.append(payload->toString());
    return new MockClientRPC(this, response);
}

/**
 * This method is invoked by tests to provide a string that will
 * be used to synthesize an input message the next time one is
 * needed (such as for an RPC result).
 *
 * \param s
 *      A string representation of the contents of a buffer,
 *      in the format expected by Buffer::fillFromString.
 */
void
MockTransport::setInput(const char* s) {
    inputMessage = s;
}

//-------------------------------------
// MockTransport::MockServerRPC class
//-------------------------------------

/**
 * Construct a MockServerRPC.
 * The input message is taken from transport->inputMessage, if
 * it contains data.
 *
 * \param transport
 *      The MockTransport object that this RPC is associated with.
 */
MockTransport::MockServerRPC::MockServerRPC(MockTransport* transport)
        : transport(transport)
{
    if (transport->inputMessage != NULL) {
        recvPayload.fillFromString(transport->inputMessage);
        transport->inputMessage = NULL;
    }
}

/**
 * Send a reply for an RPC. This method just logs the contents of
 * the reply message and deletes this reply object.
 */
void
MockTransport::MockServerRPC::sendReply() {
    if (transport->outputLog.length() != 0) {
        transport->outputLog.append(" | ");
    }
    transport->outputLog.append("serverReply: ");
    transport->outputLog.append(replyPayload.toString());
    delete this;
}

/**
 * End an RPC without sending a response. This method is obsolete
 * and should eventually be eliminated.
 */
void
MockTransport::MockServerRPC::ignore() {
    delete this;
}

//-------------------------------------
// MockTransport::MockClientRPC class
//-------------------------------------

/**
 * Construct a MockClientRPC.
 *
 * \param transport
 *      The MockTransport object that this RPC is associated with.
 * \param[out] response
 *      Buffer in which the response message should be placed.
 */
MockTransport::MockClientRPC::MockClientRPC(MockTransport* transport,
                                            Buffer* response)
        : transport(transport), response(response) { }

/**
 * Wait for a response to arrive for this RPC. This is a fake implementation
 * that simply returns a prepared response supplied to us explicitly
 * by the current test (or an empty buffer, if nothing was supplied).
 */
void
MockTransport::MockClientRPC::getReply() {
    if (transport->inputMessage != NULL) {
        response->fillFromString(transport->inputMessage);
        transport->inputMessage = NULL;
    }
    delete this;
}

}  // namespace RAMCloud
