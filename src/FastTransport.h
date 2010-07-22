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

    void poll() {
        while (tryProcessPacket())
            fireTimers();
        fireTimers();
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
    struct Header {
        enum PayloadType {
            DATA                 = 0,
            ACK                  = 1,
            SESSION_OPEN         = 2,
            RESERVED1            = 3,
            BAD_SESSION          = 4,
            RETRY_WITH_NEW_RPCID = 5,
            RESERVED2            = 6,
            RESERVED3            = 7
        };
        enum Direction {
            CLIENT_TO_SERVER = 0,
            SERVER_TO_CLIENT = 1
        };
        uint64_t sessionToken;
        uint32_t rpcId;
        uint32_t clientSessionHint;
        uint32_t serverSessionHint;
        uint16_t fragNumber;
        uint16_t totalFrags;
        uint8_t channelId;
        uint8_t direction:1;
        uint8_t requestAck:1;
        uint8_t pleaseDrop:1;
        uint8_t reserved1:1;
        uint8_t payloadType:4;
    } __attribute__((packed));

    struct SessionOpenResponse {
        uint8_t maxChannelId;
    } __attribute__((packed));

    struct AckResponse {
        uint16_t firstMissingFrag;
        uint32_t stagingVector;
    } __attribute__((packed));

    bool tryProcessPacket() __attribute__((noreturn)) {
        throw UnrecoverableTransportException("Not implemented");
    }

    void fireTimers() {
    }

    LIST_HEAD(ServerReadyQueueHead, ServerRPC) serverReadyQueue;

    friend class FastTransportTest;
    DISALLOW_COPY_AND_ASSIGN(FastTransport);
};

}  // namespace RAMCloud

#endif
