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
 * Header file for the RAMCloud::Transport class and its exceptions.
 */

#ifndef RAMCLOUD_TRANSPORT_H
#define RAMCLOUD_TRANSPORT_H

#include <string>

#include "Common.h"
#include "Service.h"
#include "Buffer.h"

namespace RAMCloud {

/**
 * An exception that is thrown when the Transport class encounters a transient
 * error.
 */
struct TransportException : public Exception {
    TransportException() : Exception() {}
    explicit TransportException(std::string msg) : Exception(msg) {}
    explicit TransportException(int errNo) : Exception(errNo) {}
};

/**
 * An exception that is thrown when the Transport class encounters a fatal
 * error. This is not recoverable unless you have another Transport to fall
 * back to, but it's also not a failed assertion because it depends on the
 * configuration of the outside world (for example, a port number is already in
 * use). In general, you should probably handle this by printing out a message
 * and exiting.
 */
struct UnrecoverableTransportException : public Exception {
    UnrecoverableTransportException() : Exception() {}
    explicit UnrecoverableTransportException(std::string msg)
        : Exception(msg) {}
    explicit UnrecoverableTransportException(int errNo) : Exception(errNo) {}
};

/**
 * An interface for reliable communication across the network.
 *
 * Implementations all send and receive RPC messages reliably over the network.
 * These messages are variable-length and can be larger than a single network
 * frame in size.
 *
 * Implementations differ in the protocol stacks they use and their performance
 * characteristics.
 */
class Transport {

  public:

    /**
     * A RPC call that has been sent and is pending a response from a server.
     * #clientSend() will return one of these, and the caller of that method
     * must later call #getReply() on it.
     */
    class ClientRpc {
      protected:
        /**
         * Constructor for ClientRpc.
         */
        ClientRpc() {}

      public:

        /**
         * Destructor for ClientRpc.
         */
        virtual ~ClientRpc() {}

        /**
         * Wait for the RPC response to arrive.
         *
         * The response will be available in the #::Buffer passed to
         * #clientSend() when this call returns.
         *
         * You should discard all pointers to this #ClientRpc object after this
         * call.
         *
         * \throw TransportException
         *      If the service has crashed.
         */
        virtual void getReply() = 0;

      private:
        DISALLOW_COPY_AND_ASSIGN(ClientRpc);
    };

    /**
     * An RPC request that has been received and is awaiting our response.
     * #serverRecv() will return one of these, and the caller of that method
     * must later call #sendReply() on it.
     */
    class ServerRpc {
      protected:
        /**
         * Constructor for ServerRpc.
         */
        ServerRpc() : recvPayload(), replyPayload() {}

      public:
        /**
         * Destructor for ServerRpc.
         */
        virtual ~ServerRpc() {}

        /**
         * Respond to the RPC with the contents of #replyPayload.
         *
         * You should discard all pointers to this #ServerRpc object after this
         * call.
         *
         * \throw TransportException
         *      If the client has crashed.
         */
        virtual void sendReply() = 0;

        /**
         * The received RPC payload.
         */
        Buffer recvPayload;

        /**
         * The RPC payload to send as a response with #sendReply().
         */
        Buffer replyPayload;

      private:
        DISALLOW_COPY_AND_ASSIGN(ServerRpc);
    };

    /**
     * Constructor for Transport.
     */
    Transport() {}

    /**
     * Destructor for Transport.
     */
    virtual ~Transport() {}

    /**
     * Wait for any RPC request to arrive.
     * \return
     *      The RPC object through which to send a reply. The caller must use
     *      either #Transport::ServerRpc::sendReply() to release the resources
     *      associated with this object.
     */
    virtual ServerRpc* serverRecv() __attribute__((warn_unused_result)) = 0;

    /**
     * Send an RPC request.
     * \param[in] service
     *      The service to which the request shall be sent.
     * \param[in] request
     *      The RPC request payload to send. The caller must not modify or even
     *      access \a request until the corresponding call to
     *      #Transport::ClientRpc::getReply() returns. The Transport may add
     *      new chunks to \a request but will not modify its existing chunks.
     * \param[out] response
     *      An initialized Buffer that will be filled in with the received RPC
     *      response. The caller must not access \a response until the
     *      corresponding call to #Transport::ClientRpc::getReply() returns.
     * \return
     *      The RPC object through which to receive the reply. The caller must
     *      use #Transport::ClientRpc::getReply() to release the resources
     *      associated with this object.
     * \throw TransportException
     *      If \a service is unavailable.
     */
    virtual ClientRpc* clientSend(Service* service, Buffer* request,
                                  Buffer* response)
        __attribute__((warn_unused_result)) = 0;

  private:
    DISALLOW_COPY_AND_ASSIGN(Transport);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_TRANSPORT_H
