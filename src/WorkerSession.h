/* Copyright (c) 2012 Stanford University
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

#ifndef RAMCLOUD_WORKERSESSION_H
#define RAMCLOUD_WORKERSESSION_H

#include <boost/intrusive_ptr.hpp>

#include "Atomic.h"
#include "Dispatch.h"
#include "Transport.h"
#include "DispatchExec.h"

namespace RAMCloud {
/**
 * A WorkerSession wraps a Transport::Session and allows that session to be
 * used in worker threads on servers.  These are needed because "real" Session
 * objects are owned by transports (which run in the dispatch thread) and
 * hence cannot be accessed in worker threads without synchronization.
 * WorkerSession objects forward methods to the actual Session object after
 * synchronizing appropriately with the dispatch thread. In addition,
 * WorkerSessions (and SessionRefs referring to them) are thread-safe.
 */
class WorkerSession : public Transport::Session {
  public:
    explicit WorkerSession(Context* context,
            Transport::SessionRef wrapped);
    ~WorkerSession();
    void abort();
    void cancelRequest(Transport::RpcNotifier* notifier);
    string getRpcInfo();
    void sendRequest(Buffer* request, Buffer* response,
            Transport::RpcNotifier* notifier);
  PRIVATE:
    Context* context;              /// Global RAMCloud state.
    Transport::SessionRef wrapped; /// Methods are forwarded to this object.
    DISALLOW_COPY_AND_ASSIGN(WorkerSession);
};

/**
 * This class wraps a call to session->sendRequest, for later execution by the
 * Dispatch thread.
 */
class SendRequestWrapper : public DispatchExec::Lambda {
  PRIVATE:
    // The following variables all have the same meaning as the arguments to
    // WorkerSession::sendRequest
    Buffer* request;
    Buffer* response;
    Transport::RpcNotifier* notifier;
    Transport::SessionRef session;
  public:
    SendRequestWrapper(Buffer* request, Buffer* response,
                       Transport::RpcNotifier* notifier,
                       Transport::SessionRef session)
        : request(request), response(response), notifier(notifier),
          session(session) { }

    /// @copydoc DispatchExec::Lambda::invoke()
    void invoke() {
        session->sendRequest(request, response, notifier);
        session = NULL;
    }
  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(SendRequestWrapper);
};

/**
 * This class wraps a call to session->cancelRequest, for later execution by the
 * Dispatch thread.
 */
class CancelRequestWrapper : public DispatchExec::Lambda {
  PRIVATE:
    // The following variables all have the same meaning as the arguments to
    // WorkerSession::cancelRequest
    Transport::RpcNotifier* notifier;
    Transport::SessionRef session;
  public:
    CancelRequestWrapper(Transport::RpcNotifier* notifier,
                       Transport::SessionRef session)
        : notifier(notifier), session(session) { }

    /// @copydoc DispatchExec::Lambda::invoke()
    void invoke() {
        session->cancelRequest(notifier);
        session = NULL;
    }

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(CancelRequestWrapper);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_WORKERSESSION_H
