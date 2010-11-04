/* Copyright (c) 2010 Stanford University
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

#include "Common.h"
#include "Server.h"
#include "Transport.h"

#ifndef RAMCLOUD_BINDTRANSPORT_H
#define RAMCLOUD_BINDTRANSPORT_H

namespace RAMCloud {

/**
 * This class defines an implementation of Transport that allows unit
 * tests to run without a network or a remote counterpart (it injects RPCs
 * directly into a Server instance's #Server::dispatch() method).
 */
class BindTransport : public Transport {
  public:
    explicit BindTransport(Server* server = NULL)
        : servers()
        , waitingRequest(NULL)
    {
        if (server)
            addServer(*server, "mock:");
    }

    ServerRpc* serverRecv() {
        ServerRpc* ret = waitingRequest;
        waitingRequest = NULL;
        return ret;
    }

    void
    addServer(Server& server, const string locator) {
        servers[locator] = &server;
    }

    Transport::SessionRef
    getSession(const ServiceLocator& serviceLocator) {
        const string& locator = serviceLocator.getOriginalString();
        ServerMap::iterator it = servers.find(locator);
        if (it == servers.end()) {
            throw TransportException(HERE, format("Unknown mock host: %s",
                                                  locator.c_str()));
        }
        return new BindSession(*this, *it->second);
    }

    Transport::SessionRef
    getSession() {
        return transportManager.getSession("mock:");
    }

  private:
    class BindServerRpc : public ServerRpc {
        public:
            BindServerRpc() {}
            void sendReply() {}
        private:
            DISALLOW_COPY_AND_ASSIGN(BindServerRpc);
    };

    class BindClientRpc : public ClientRpc {
        public:
            explicit BindClientRpc(BindTransport& transport,
                                   Buffer& request, Buffer& response,
                                   Server& server)
                : transport(transport), request(request), response(response),
                  server(server) {}
            void getReply();
        private:
            BindTransport& transport;
            Buffer& request;
            Buffer& response;
            Server& server;
            DISALLOW_COPY_AND_ASSIGN(BindClientRpc);
    };

    class BindSession : public Session {
        public:
            explicit BindSession(BindTransport& transport, Server& server)
                : transport(transport), server(server) {}
            ClientRpc* clientSend(Buffer* request, Buffer* response) {
                return new BindClientRpc(transport, *request, *response,
                                         server);
            }
            void release() {
                delete this;
            }
        private:
            BindTransport& transport;
            Server& server;
            DISALLOW_COPY_AND_ASSIGN(BindSession);
    };

  public:
    typedef std::map<const string, Server*> ServerMap;
    ServerMap servers;
  private:
    ServerRpc* waitingRequest;
    DISALLOW_COPY_AND_ASSIGN(BindTransport);
};

}  // namespace RAMCloud

#endif
