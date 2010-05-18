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

#include <Common.h>
#include <Service.h>
#include <Buffer.h>

#include <string>

namespace RAMCloud {

/**
 * An exception that is thrown when the Transport class encounters a transient
 * error.
 */

// This implementation was copied from BackupException.
// TODO(ongaro): We should create some common infrastructure for exceptions.

// TODO(ongaro): Move inside Transport.
struct TransportException {
    explicit TransportException() : message(""), errNo(0) {}
    explicit TransportException(std::string msg)
            : message(msg), errNo(0) {}
    explicit TransportException(int errNo) : message(""), errNo(errNo) {
        message = strerror(errNo);
    }
    explicit TransportException(std::string msg, int errNo)
            : message(msg), errNo(errNo) {}
    TransportException(const TransportException &e)
            : message(e.message), errNo(e.errNo) {}
    TransportException &operator=(const TransportException &e) {
        if (&e == this)
            return *this;
        message = e.message;
        errNo = e.errNo;
        return *this;
    }
    virtual ~TransportException() {}
    std::string message;
    int errNo;
};

/**
 * An exception that is thrown when the Transport class encounters a fatal
 * error. This is not recoverable unless you have another Transport to fall
 * back to, but it's also not a failed assertion because it depends on the
 * configuration of the outside world (for example, a port number is already in
 * use). In general, you should probably handle this by printing out a message
 * and exiting.
 */

// TODO(ongaro): Move inside Transport.

struct UnrecoverableTransportException {
    explicit UnrecoverableTransportException() : message(""), errNo(0) {}
    explicit UnrecoverableTransportException(std::string msg)
            : message(msg), errNo(0) {}
    explicit UnrecoverableTransportException(int errNo)
        : message(""), errNo(errNo) {
        message = strerror(errNo);
    }
    explicit UnrecoverableTransportException(std::string msg, int errNo)
            : message(msg), errNo(errNo) {}
    explicit UnrecoverableTransportException(const TransportException &e)
            : message(e.message), errNo(e.errNo) {}
    UnrecoverableTransportException &operator=
        (const UnrecoverableTransportException &e) {
        if (&e == this)
            return *this;
        message = e.message;
        errNo = e.errNo;
        return *this;
    }
    virtual ~UnrecoverableTransportException() {}
    std::string message;
    int errNo;
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
    class ClientRPC {
      protected:
        /**
         * Constructor for ClientRPC.
         */
        ClientRPC() {}

      public:

        /**
         * Destructor for ClientRPC.
         */
        virtual ~ClientRPC() {}

        /**
         * Wait for the RPC response to arrive.
         * You should discard all pointers to this object after this call.
         * \throw TransportException
         *      If the service has crashed.
         */
        virtual void getReply() = 0;

      private:
        DISALLOW_COPY_AND_ASSIGN(ClientRPC);
    };

    /**
     * An RPC request that has been received and is awaiting our response.
     * #serverRecv() will return one of these, and the caller of that method
     * must later call either #sendReply() or #ignore() on it.
     */
    class ServerRPC {
      protected:
        /**
         * Constructor for ServerRPC.
         */
        ServerRPC() {}

      public:
        /**
         * Destructor for ServerRPC.
         */
        virtual ~ServerRPC() {}

        /**
         * Respond to the RPC.
         * You should discard all pointers to this object after this call.
         * \param[in] payload
         *      The RPC payload to send on the wire.
         * \throw TransportException
         *      If the client has crashed.
         */
        virtual void sendReply(Buffer* payload) = 0;

        /**
         * Ignore the RPC.
         * Call this if you don't want to respond to the RPC.
         * You should discard all pointers to this object after this call.
         */
        virtual void ignore() = 0;

      private:
        DISALLOW_COPY_AND_ASSIGN(ServerRPC);
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
     * \param[out] payload
     *      An initialized Buffer that will be filled in with the RPC payload.
     * \return
     *      The RPC object through which to send a reply. The caller must use
     *      either #Transport::ServerRPC::sendReply() or
     *      #Transport::ServerRPC::ignore() to release the resources associated
     *      with this object.
     */
    // TODO(ongaro): payload should be part of ServerRPC
    virtual ServerRPC* serverRecv(Buffer* payload)
        __attribute__((warn_unused_result)) = 0;

    /**
     * Send an RPC request.
     * \param[in] service
     *      The service to which the request shall be sent.
     * \param[in] request
     *      The RPC request payload to send. The caller must not modify or even
     *      access \a request until the corresponding call to
     *      #Transport::ClientRPC::getReply() returns. The Transport may add
     *      new chunks to \a request but will not modify its existing chunks.
     * \param[out] response
     *      An initialized Buffer that will be filled in with the received RPC
     *      response. The caller must not access \a response until the
     *      corresponding call to #Transport::ClientRPC::getReply() returns.
     * \return
     *      The RPC object through which to receive the reply. The caller must
     *      use #Transport::ClientRPC::getReply() to release the resources
     *      associated with this object.
     * \throw TransportException
     *      If \a service is unavailable.
     */
    virtual ClientRPC* clientSend(const Service* service, Buffer* request,
                                  Buffer* response)
        __attribute__((warn_unused_result)) = 0;

  private:
    DISALLOW_COPY_AND_ASSIGN(Transport);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_TRANSPORT_H
