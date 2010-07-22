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

#include <Common.h>
#include <Transport.h>

namespace RAMCloud {


class FastTransport : public Transport {
  public:
    FastTransport() {
    }

    class ClientRPC : public Transport::ClientRPC {
    };

    ClientRPC* clientSend(const Service* service, Buffer* request,
                          Buffer* response) __attribute__((noreturn)) {
        throw UnrecoverableTransportException("Not implemented");
    }

    class ServerRPC : public Transport::ServerRPC {
    };

    ServerRPC* serverRecv() __attribute__((noreturn)) {
        throw UnrecoverableTransportException("Not implemented");
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(FastTransport);
};

}  // namespace RAMCloud

#endif
