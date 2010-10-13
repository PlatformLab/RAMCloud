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

#include "MockTransport.h"
#include "TestUtil.h"

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
 * Return an incoming request. This is a fake method that uses a request
 * message explicitly provided by the test, or returns NULL.
 */
Transport::ServerRpc*
MockTransport::serverRecv() {
    if (inputMessage == NULL)
       return NULL;
    else
       return new MockServerRpc(this);
}

Transport::SessionRef
MockTransport::getSession(const ServiceLocator& serviceLocator)
{
    return new MockSession(this, serviceLocator);
}

Transport::SessionRef
MockTransport::getSession()
{
    return new MockSession(this);
}


/**
 * Issue an RPC request using this transport.
 *
 * This is a fake method; it simply logs information about the request.
 *
 * \param payload
 *      Contents of the request message.
 * \param[out] response
 *      When a response arrives, the response message will be made
 *      available via this Buffer.
 *
 * \return  A pointer to the allocated space or \c NULL if there is not enough
 *          space in this Allocation.
 */
Transport::ClientRpc*
MockTransport::MockSession::clientSend(Buffer* payload, Buffer* response) {
    if (transport->outputLog.length() != 0) {
        transport->outputLog.append(" | ");
    }
    transport->outputLog.append("clientSend: ");
    transport->outputLog.append(toString(payload));
    return new MockClientRpc(transport, response);
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
// MockTransport::MockServerRpc class
//-------------------------------------

/**
 * Construct a MockServerRpc.
 * The input message is taken from transport->inputMessage, if
 * it contains data.
 *
 * \param transport
 *      The MockTransport object that this RPC is associated with.
 */
MockTransport::MockServerRpc::MockServerRpc(MockTransport* transport)
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
MockTransport::MockServerRpc::sendReply() {
    if (transport->outputLog.length() != 0) {
        transport->outputLog.append(" | ");
    }
    transport->outputLog.append("serverReply: ");
    transport->outputLog.append(toString(&replyPayload));
    delete this;
}

//-------------------------------------
// MockTransport::MockClientRpc class
//-------------------------------------

/**
 * Construct a MockClientRpc.
 *
 * \param transport
 *      The MockTransport object that this RPC is associated with.
 * \param[out] response
 *      Buffer in which the response message should be placed.
 */
MockTransport::MockClientRpc::MockClientRpc(MockTransport* transport,
                                            Buffer* response)
        : transport(transport), response(response) { }

/**
 * Wait for a response to arrive for this RPC. This is a fake implementation
 * that simply returns a prepared response supplied to us explicitly
 * by the current test (or an empty buffer, if nothing was supplied).
 */
void
MockTransport::MockClientRpc::getReply() {
    if (transport->inputMessage != NULL) {
        response->fillFromString(transport->inputMessage);
        transport->inputMessage = NULL;
    }
    delete this;
}

}  // namespace RAMCloud
