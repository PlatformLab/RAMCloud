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
 * \file MockTransport.h Header file for the TCPTransport class.
 */

#include <Transport.h>

#ifndef RAMCLOUD_MOCKTRANSPORT_H
#define RAMCLOUD_MOCKTRANSPORT_H

namespace RAMCloud {

class MockTransport : public Transport {
  public:
    MockTransport()
            : serverRecvCount(0), serverSendCount(0),
              clientSendCount(0), clientRecvCount(0) { }

    class MockServerRPC : public ServerRPC {
        public:
            explicit MockServerRPC(MockTransport* trans) : trans(trans) {}
            void sendReply() {
                ++trans->serverSendCount;
                delete this;
            }
            void ignore() {
                delete this;
            }
        private:
            MockTransport* trans;
            DISALLOW_COPY_AND_ASSIGN(MockServerRPC);
    };

    class MockClientRPC : public ClientRPC {
        public:
            explicit MockClientRPC(MockTransport* trans) : trans(trans) {}
            void getReply() {
                ++trans->clientRecvCount;
                delete this;
            }
        private:
            MockTransport* trans;
            DISALLOW_COPY_AND_ASSIGN(MockClientRPC);
    };

    virtual ~MockTransport() { }

    virtual ServerRPC* serverRecv() {
        ++serverRecvCount;
        return new MockServerRPC(this);
    }

    virtual ClientRPC* clientSend(Service* service, Buffer* payload,
                                  Buffer* response) {
        ++clientSendCount;
        return new MockClientRPC(this);
    }

    uint32_t serverRecvCount;
    uint32_t serverSendCount;
    uint32_t clientSendCount;
    uint32_t clientRecvCount;

    DISALLOW_COPY_AND_ASSIGN(MockTransport);
};

}  // namespace RAMCloud

#endif
