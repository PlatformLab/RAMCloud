/* Copyright (c) 2011 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef RAMCLOUD_PINGCLIENT_H
#define RAMCLOUD_PINGCLIENT_H

#include "Client.h"
#include "ServerMetrics.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * This class implements the client-side interface to the ping service.
 */
class PingClient : public Client {
  public:
    /// An asynchronous remote kill operation.
    class Kill {
      public:
        Kill(PingClient& client, const char *serviceLocator);
        void cancel() { state.cancel(); }
      private:
        PingClient& client;
        Buffer requestBuffer;
        Buffer responseBuffer;
        AsyncState state;
        DISALLOW_COPY_AND_ASSIGN(Kill);
    };

    PingClient() {}
    ServerMetrics getMetrics(const char* serviceLocator);
    uint64_t ping(const char* serviceLocator, uint64_t nonce,
            uint64_t timeoutNanoseconds);
    uint64_t proxyPing(const char* serviceLocator1,
            const char* serviceLocator2,
            uint64_t timeoutNanoseconds1,
            uint64_t timeoutNanoseconds2);

  private:
    DISALLOW_COPY_AND_ASSIGN(PingClient);
};
} // namespace RAMCloud

#endif // RAMCLOUD_PINGCLIENT_H
