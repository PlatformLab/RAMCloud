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
 * Implementation of #RAMCloud::FastTransport.
 */

#ifndef RAMCLOUD_FASTTRANSPORT_H
#define RAMCLOUD_FASTTRANSPORT_H

#include <queue.h>
#include <Common.h>
#include <Transport.h>

namespace RAMCloud {


class FastTransport : public Transport {
  public:
    FastTransport() : serverReadyQueue() {
        LIST_INIT(&serverReadyQueue);
    }

    void poll() __attribute__((noreturn)) {
        throw UnrecoverableTransportException("Not implemented");
    }

    class ClientRPC : public Transport::ClientRPC {
        friend class FastTransport;
        friend class FastTransportTest;
    };

    ClientRPC* clientSend(const Service* service, Buffer* request,
                          Buffer* response) __attribute__((noreturn)) {
        throw UnrecoverableTransportException("Not implemented");
    }

    class ServerRPC;
    class ServerRPC : public Transport::ServerRPC {
      public:
        ServerRPC() : readyQueueEntries() {}
        void sendReply() __attribute__((noreturn)) {
            throw UnrecoverableTransportException("Not implemented");
        }
        void ignore() __attribute__((noreturn)) {
            throw UnrecoverableTransportException("Not implemented");
        }
      private:
        LIST_ENTRY(ServerRPC) readyQueueEntries;
        friend class FastTransport;
        friend class FastTransportTest;
    };

    ServerRPC* serverRecv() {
        ServerRPC* rpc;
        while ((rpc = LIST_FIRST(&serverReadyQueue)) == NULL)
            poll();
        LIST_REMOVE(rpc, readyQueueEntries);
        return rpc;
    }

  private:
    LIST_HEAD(ServerReadyQueueHead, ServerRPC) serverReadyQueue;

    friend class FastTransportTest;
    DISALLOW_COPY_AND_ASSIGN(FastTransport);
};

}  // namespace RAMCloud

#endif
