/* Copyright (c) 2010-2015 Stanford University
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

#include "TestUtil.h"
#include "Common.h"
#include "BitOps.h"
#include "RpcLevel.h"
#include "Service.h"
#include "ServerRpcPool.h"
#include "TransportManager.h"
#include "WorkerManager.h"

#ifndef RAMCLOUD_BINDTRANSPORT_H
#define RAMCLOUD_BINDTRANSPORT_H

namespace RAMCloud {

/**
 * This class defines an implementation of Transport that allows unit
 * tests to run without a network or a remote counterpart (it injects RPCs
 * directly into a Service instance's #dispatch() method).
 */
struct BindTransport : public Transport {
    /**
     * The following class mirrors the ServerRpc class defined in other
     * transports, and allows unit testing of functions that expect a subclass
     * of Transport::ServerRpc to be available, such as
     * MasterService::migrateTablet.
     */
    class ServerRpc : public Transport::ServerRpc {
        public:
            ServerRpc() {}
            void sendReply() {}
            string getClientServiceLocator() {return std::string();}
        private:
            DISALLOW_COPY_AND_ASSIGN(ServerRpc);
    };

    explicit BindTransport(Context* context)
        : context(context), servers(), abortCounter(0), errorMessage(),
          serverRpcPool()
    { }

    string
    getServiceLocator() {
        return "mock:";
    }

    /**
     * Make a collection of services available through this transport.
     *
     * \param context
     *      Defines one or more services (those in context->services).
     * \param locator
     *      Locator to associate with the services: open a session with
     *      this locator, and RPCs will find their way to the services in
     *      context->services.
     */
    void registerServer(Context* context, const string locator) {
        servers[locator] = context;
    }

    Transport::SessionRef
    getSession(const ServiceLocator* serviceLocator, uint32_t timeoutMs = 0) {
        const string& locator = serviceLocator->getOriginalString();
        ServerMap::iterator it = servers.find(locator);
        if (it == servers.end()) {
            throw TransportException(HERE, format("Unknown mock host: %s",
                                                  locator.c_str()));
        }
        return new BindSession(*this, it->second, locator);
    }

    Transport::SessionRef
    getSession() {
        return context->transportManager->getSession("mock:");
    }

    struct BindServerRpc : public ServerRpc {
        BindServerRpc() {}
        void sendReply() {}
        DISALLOW_COPY_AND_ASSIGN(BindServerRpc);
    };

    struct BindSession : public Session {
      public:
        explicit BindSession(BindTransport& transport, Context* context,
                             const string& locator)
            : transport(transport), context(context),
            lastRequest(NULL), lastResponse(NULL), lastNotifier(NULL),
            dontNotify(false)
        {
            setServiceLocator(locator);
        }

        void abort() {}
        void cancelRequest(RpcNotifier* notifier) {}
        string getRpcInfo()
        {
            if (lastNotifier == NULL)
                return "no active RPCs via BindTransport";
            return format("%s via BindTransport",
                    WireFormat::opcodeSymbol(lastRequest));
        }
        void sendRequest(Buffer* request, Buffer* response,
                         RpcNotifier* notifier)
        {
            response->reset();
            lastRequest = request;
            lastResponse = response;
            lastNotifier = notifier;

            // The worker and ServerRpc are included to more fully simulate a
            // real call to Service method, since they are public members of
            // their respective classes.
            ServerRpc* serverRpc = transport.serverRpcPool.construct();
            ServerRpcPoolGuard<ServerRpc> serverRpcKiller(
                    transport.serverRpcPool, serverRpc);
            Worker w(NULL);
            w.rpc = serverRpc;

            Service::Rpc rpc(&w, request, response);
            if (transport.abortCounter > 0) {
                transport.abortCounter--;
                if (transport.abortCounter == 0) {
                    // Simulate a failure of the server to respond.
                    notifier->failed();
                    return;
                }
            }
            if (transport.errorMessage != "") {
                notifier->failed();
                transport.errorMessage = "";
                return;
            }
            Service::handleRpc(context, &rpc);

            if (!dontNotify) {
                notifier->completed();
                lastNotifier = NULL;
            }
        }
        BindTransport& transport;

        // Context to use for dispatching RPCs sent to this session.
        Context* context;

        // The request and response buffers from the last call to sendRequest
        // for this session.
        Buffer *lastRequest, *lastResponse;

        // Notifier from the last call to sendRequest, if that call hasn't
        // yet been responded to.
        RpcNotifier *lastNotifier;

        // If the following variable is set to true by testing code, then
        // sendRequest does not immediately signal completion of the RPC.
        // It does complete the RPC, but returns without calling the
        // notifier, leaving it to testing code to invoke the notifier
        // explicitly to complete the call (the testing code can also
        // modify the response).
        bool dontNotify;

        DISALLOW_COPY_AND_ASSIGN(BindSession);
    };

    // Shared RAMCloud information.
    Context* context;

    // Maps from a service locator to a Context corresponding to a
    // server, which can be used to dispatch RPCs to that server.
    typedef std::map<const string, Context*> ServerMap;
    ServerMap servers;

    // The following value is used to simulate server timeouts.
    int abortCounter;

    /**
     * If this is set to a non-empty value then the next RPC will
     * fail immediately.
     */
    string errorMessage;

    /**
     * This is used to create mock subclasses of Transport::ServerRpc.
     */
    ServerRpcPool<ServerRpc> serverRpcPool;

    DISALLOW_COPY_AND_ASSIGN(BindTransport);
};

}  // namespace RAMCloud

#endif
