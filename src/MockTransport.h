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

#include <string>

#include "Transport.h"

#ifndef RAMCLOUD_MOCKTRANSPORT_H
#define RAMCLOUD_MOCKTRANSPORT_H

namespace RAMCloud {

/**
 * This class defines an implementation of Transport that allows unit
 * tests to run without a network or a remote counterpart (it logs
 * output packets and provides a mechanism for prespecifying input
 * packets).
 */
class MockTransport : public Transport {
  public:
    explicit MockTransport(const ServiceLocator *serviceLocator = NULL);
    virtual ~MockTransport() { }
    virtual ServiceLocator getServiceLocator();

    virtual Transport::SessionRef
    getSession(const ServiceLocator& serviceLocator);

    virtual Transport::SessionRef
    getSession();

    void setInput(const char* message);

    class MockServerRpc : public ServerRpc {
        public:
            explicit MockServerRpc(MockTransport* transport,
                                   const char* message);
            void sendReply();
        private:
            MockTransport* transport;
            DISALLOW_COPY_AND_ASSIGN(MockServerRpc);
    };

    class MockClientRpc : public ClientRpc {
        public:
            explicit MockClientRpc(MockTransport* transport, Buffer* response);
        private:
            Buffer* response;
            DISALLOW_COPY_AND_ASSIGN(MockClientRpc);
    };

    class MockSession : public Session {
        public:
            explicit MockSession(MockTransport* transport)
                : transport(transport),
                serviceLocator(ServiceLocator("mock: anonymous=1")) {}
            MockSession(MockTransport* transport,
                        const ServiceLocator& serviceLocator)
                : transport(transport), serviceLocator(serviceLocator) {}
            virtual ~MockSession();
            virtual ClientRpc* clientSend(Buffer* payload, Buffer* response);
            virtual void release() {
                delete this;
            }
        private:
            MockTransport* transport;
            const ServiceLocator serviceLocator;
            DISALLOW_COPY_AND_ASSIGN(MockSession);
    };

    /**
     * Records information from each call to clientSend and sendReply.
     */
    string outputLog;

    /**
     * Used as the next input message required by wait.
     */
    const char* inputMessage;

    // The following variables count calls to various methods, for use
    // by tests.
    uint32_t serverSendCount;
    uint32_t clientSendCount;
    uint32_t clientRecvCount;

    // The following variable must be static: sessions can get deleted
    // *after* their transport, so can't reference anything in a particular
    // transport.
    static uint32_t sessionDeleteCount;

    // ServiceLocator string passed to constructor, or empty if the
    // constructor argument was NULL.
    string locatorString;

    DISALLOW_COPY_AND_ASSIGN(MockTransport);
};

}  // namespace RAMCloud

#endif
