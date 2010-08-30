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
 * \file
 * This file defines an implementation of Transport that allows unit
 * tests to run without a network or a remote counterpart (it logs
 * output packets and provides a mechanism for prespecifying input
 * packets).
 */

#include <Transport.h>
#include <string>

#ifndef RAMCLOUD_MOCKTRANSPORT_H
#define RAMCLOUD_MOCKTRANSPORT_H

namespace RAMCloud {

class MockTransport : public Transport {
  public:
    MockTransport();
    virtual ~MockTransport() { }
    virtual ServerRpc* serverRecv();
    virtual ClientRpc* clientSend(Service* service, Buffer* payload,
                                  Buffer* response);
    void setInput(const char* message);

    class MockServerRpc : public ServerRpc {
        public:
            explicit MockServerRpc(MockTransport* transport);
            void sendReply();
            void ignore();
        private:
            MockTransport* transport;
            DISALLOW_COPY_AND_ASSIGN(MockServerRpc);
    };

    class MockClientRpc : public ClientRpc {
        public:
            explicit MockClientRpc(MockTransport* transport, Buffer* response);
            void getReply();
        private:
            MockTransport* transport;
            Buffer* response;
            DISALLOW_COPY_AND_ASSIGN(MockClientRpc);
    };

    /**
     * Records information from each call to clientSend and sendReply.
     */
    string outputLog;

    /**
     * Used as the next input message required by either serverRecv
     * or getReply.
     */
    const char* inputMessage;

    // The following variables count calls to various methods, for use
    // by tests.
    uint32_t serverRecvCount;
    uint32_t serverSendCount;
    uint32_t clientSendCount;
    uint32_t clientRecvCount;

    DISALLOW_COPY_AND_ASSIGN(MockTransport);
};

}  // namespace RAMCloud

#endif
