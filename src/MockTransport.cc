/* Copyright (c) 2010-2014 Stanford University
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

#include "TestUtil.h"
#include "MockTransport.h"
#include "WireFormat.h"

namespace RAMCloud {
uint32_t RAMCloud::MockTransport::sessionDeleteCount = 0;

//------------------------------
// MockTransport class
//------------------------------

/**
 * Construct a MockTransport.
 */
MockTransport::MockTransport(Context* context,
                             const ServiceLocator *serviceLocator)
            : context(context)
            , outputLog()
            , output()
            , status(Status(STATUS_MAX_VALUE+1))
            , inputMessages()
            , lastNotifier(NULL)
            , serverSendCount(0)
            , clientRecvCount(0)
            , sessionCreateCount(0)
            , locatorString()
{
    if (serviceLocator != NULL) {
        locatorString = serviceLocator->getOriginalString();
    } else {
        locatorString = "mock:";
    }
}

/**
 * See Transport::getServiceLocator.
 */
string
MockTransport::getServiceLocator()
{
    return locatorString;
}

Transport::SessionRef
MockTransport::getSession(const ServiceLocator& serviceLocator,
        uint32_t timeoutMs)
{
    sessionCreateCount++;

    // magic hook to invoke failed getSession in testing
    if (strstr(serviceLocator.getOriginalString().c_str(),
            "host=error") != NULL)
        throw TransportException(HERE, "Failed to open session");

    MockSession* session = new MockSession(this, serviceLocator);
    session->setServiceLocator(serviceLocator.getOriginalString());
    return session;
}

Transport::SessionRef
MockTransport::getSession()
{
    MockSession* session =  new MockSession(this);
    session->setServiceLocator("test:");
    return session;
}

/**
 * Destructor for MockSession: just log the destruction.
 */
MockTransport::MockSession::~MockSession()
{
    MockTransport::sessionDeleteCount++;
}

/**
 * Abort this session; this method does nothing except create a
 * log message.
 */
void
MockTransport::MockSession::abort()
{
    transport->appendToOutput(ABORT, "");
}

// See Transport::Session::cancelRequest for documentation.
void
MockTransport::MockSession::cancelRequest(RpcNotifier* notifier)
{
    transport->appendToOutput(CANCEL, "");
}

// See Transport::Session::getRpcInfo for documentation.
string
MockTransport::MockSession::getRpcInfo()
{
    return "dummy RPC info for MockTransport";
}

// See Transport::Session::sendRequest for documentation.
void
MockTransport::MockSession::sendRequest(Buffer* request, Buffer* response,
                RpcNotifier* notifier)
{
    response->reset();
    transport->appendToOutput(SEND_REQUEST, *request);
    if (!transport->inputMessages.empty()) {
        const char *resp = transport->inputMessages.front();
        transport->inputMessages.pop();
        if (resp == NULL) {
            notifier->failed();
        } else {
            response->fillFromString(resp);
            notifier->completed();
        }
    }
    transport->lastNotifier = notifier;
}

/**
 * Clear all testing RPC responses; see setInput().
 */
void
MockTransport::clearInput()
{
    while (!inputMessages.empty())
        inputMessages.pop();
}

/**
 * This method is invoked by tests to provide a string that will
 * be used to synthesize an input message the next time one is
 * needed (such as for an RPC result). The value NULL may be used when
 * one wants to throw a transport exception instead of a response.
 *
 * \param s
 *      A string representation of the contents of a buffer,
 *      in the format expected by Buffer::fillFromString.
 */
void
MockTransport::setInput(const char* s)
{
    inputMessages.push(s);
}

/**
 * Clear #outputLog and #output so that newly enqueued messages are appended
 * to the start of those structures.
 */
void
MockTransport::clearOutput()
{
    outputLog = "";
    output.clear();
}

//-------------------------------------
// MockTransport::MockServerRpc class
//-------------------------------------

/**
 * Construct a MockServerRpc.
 *
 * \param transport
 *      The MockTransport object that this RPC is associated with.
 * \param message
 *      Describes contents of message (parsed by #fillFromString).
 */
MockTransport::MockServerRpc::MockServerRpc(MockTransport* transport,
                                            const char* message)
        : transport(transport)
{
    if (message != NULL) {
        requestPayload.fillFromString(message);
    }

    // set this to avoid handleRpc's sanity check
    epoch = 0;
}

/**
 * Send a reply for an RPC. This method just logs the contents of
 * the reply message and deletes this reply object.
 */
void
MockTransport::MockServerRpc::sendReply()
{
    transport->appendToOutput(SERVER_REPLY, replyPayload);
    const WireFormat::ResponseCommon* responseHeader =
        replyPayload.getStart<WireFormat::ResponseCommon>();
    transport->status = (responseHeader != NULL) ? responseHeader->status :
            Status(STATUS_MAX_VALUE+1);
    delete this;
}

/**
 * Normally used to return ServiceLocator-like string associated
 * with the client that issued this RPC. It's not currently used
 * for testing, so return a blank string instead.
 */
string
MockTransport::MockServerRpc::getClientServiceLocator()
{
    return "";
}

// - private -

namespace {
/**
 * Returns string form of \a event.
 */
const char*
eventToStr(MockTransport::Event event)
{
    const char* eventStr = "unknown";
    switch (event) {
    case MockTransport::CLIENT_SEND:
        eventStr = "clientSend";
        break;
    case MockTransport::ABORT:
        eventStr = "abort";
        break;
    case MockTransport::CANCEL:
        eventStr = "cancel";
        break;
    case MockTransport::SEND_REQUEST:
        eventStr = "sendRequest";
        break;
    case MockTransport::SERVER_REPLY:
        eventStr = "serverReply";
        break;
    default:
        break;
    }
    return eventStr;
}
}

/**
 * Append an event and string message to the both output logs. One, in string
 * form, is to #outputLog; the other is appended to #output. The Buffer
 * appended to #output simply contains \a message.
 * Used for ABORT and CANCEL;
 */
void
MockTransport::appendToOutput(Event event, const string& message)
{
    if (outputLog.length() != 0) {
        outputLog.append(" | ");
    }
    outputLog.append(format("%s: %s", eventToStr(event),
                            message.c_str()));

    output.emplace_back();
    auto& pair = output.back();
    pair.first = event;
    pair.second.appendCopy(message.c_str(),
            downCast<uint32_t>(message.length() + 1));
}

/**
 * Append an event and buffer to the both output logs. One, in string
 * form, is to #outputLog; the other, which retains a copy of the Buffer,
 * is to #output.
 * Used for CLIEND_SEND, SEND_REQUEST, and SERVER_REPLY.
 */
void
MockTransport::appendToOutput(Event event, Buffer& payload)
{
    if (outputLog.length() != 0) {
        outputLog.append(" | ");
    }
    outputLog.append(format("%s: %s", eventToStr(event),
                            TestUtil::toString(&payload).c_str()));

    output.emplace_back();
    auto& pair = output.back();
    pair.first = event;
    uint32_t length = payload.size();
    if (length > 0) {
        void* chunk = pair.second.alloc(length);
        payload.copy(0, payload.size(), chunk);
    }
}

/**
 * Completely stupid hack to work around C++; outputMatches() must be in the
 * h file since it is a template method, but TestUtil.h cannot be included
 * in MockTransport.h since it must be included first (MockTransport.h is
 * included by TransportManager which includes Common.h first).
 */
string
MockTransport::toStringHack(const char* buf, uint32_t length)
{
    return TestUtil::toString(buf, length);
}

}  // namespace RAMCloud
