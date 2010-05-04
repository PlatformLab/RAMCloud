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
 * configuration of the outside world. If you're trying to handle these
 * gracefully, you're probably doing something wrong.
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
 * Best-effort reliable communication with the outside world.
 */
class Transport {

  protected:

    /**
     * Base class for server tokens used in #serverRecv() and #serverSend().
     * This class and its derivatives should always live inside a
     * Transport::ServerToken container.
     */
    class BaseServerToken {
      public:
        BaseServerToken() {}
        virtual ~BaseServerToken() {}
      private:
        DISALLOW_COPY_AND_ASSIGN(BaseServerToken);
    };

    /**
     * Base class for client tokens used in #clientSend() and #clientRecv().
     * This class and its derivatives should always live inside a
     * Transport::ClientToken container.
     */
    class BaseClientToken {
      public:
        BaseClientToken() {}
        virtual ~BaseClientToken() {}
      private:
        DISALLOW_COPY_AND_ASSIGN(BaseClientToken);
    };

  private:

    /**
     * A template for an opaque container that holds a derivative of
     * BaseServerToken or BaseClientToken.
     * The two classes generated from this template are Transport::ServerToken
     * and Transport::ClientToken.
     *
     * The size of the storage for the concrete token must be at least as big
     * as the size of the concrete token. You should assert this using
     * #BUF_SIZE for every concrete token type.
     *
     * \tparam C
     *      Either BaseServerToken or BaseClientToken. This is a concrete token
     *      that doesn't do anything.
     */

    // TODO(ongaro): Move the 7 lines of implementation to a Transport.cc?

    // TODO(ongaro): Unit test this.

    template <class C>
    class Token {
      public:

        /**
         * Constructor for Token.
         * Your token starts out as an instance of \a C, effectively a no-op.
         */
        Token() {
            static_assert(BUF_SIZE >= sizeof(C));
            new(buf) C();
        }

        /**
         * Destructor for Token.
         * Calls the destructor for the concrete token.
         */
        ~Token() {
            reinterpret_cast<C*>(buf)->~C();
        }

        /**
         * Destroy the existing concrete token and reset to an instance of \a C.
         */
        void reinit() {
            reinit<C>();
        }

        /**
         * Destroy the existing concrete token and construct a new one.
         *
         * No arguments will be passed to \a T's constructor.
         *
         * Only concrete Transport implementations should call this.
         *
         * \tparam T
         *      A derivative of \a C. It better fit within #BUF_SIZE bytes.
         * \return
         *      A pointer to the newly constructed concrete token.
         */
        template <typename T>
        T* reinit() {
            reinterpret_cast<C*>(buf)->~C();
            return new(buf) T();
        }

        /**
         * Get a pointer to the concrete token contained within.
         * \tparam T
         *      The type of the concrete token. You'll get very little type
         *      safety here, so be careful.
         * \return
         *      See above.
         */
        template <typename T>
        T* getBuf() {
            return static_cast<T*>(reinterpret_cast<C*>(buf));
        }

        /**
         * The maximum size in bytes for the concrete token contained within.
         */
        enum { BUF_SIZE = 32 };

      private:

        /**
         * The actual storage for the concrete token contained within.
         */
        char buf[BUF_SIZE];

        DISALLOW_COPY_AND_ASSIGN(Token);
    };

  public:

    /**
     * An opaque container for any server token type derived from BaseServerToken.
     * You'll need one of these for #serverRecv() and #serverSend().
     */
    typedef Token<BaseServerToken> ServerToken;

    /**
     * An opaque container for any client token type derived from BaseClientToken.
     * You'll need one of these for #clientSend() and #clientRecv().
     */
    typedef Token<BaseClientToken> ClientToken;

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
     * \param[out] token
     *      A ServerToken that should be passed in to #serverSend() later.
     */
    virtual void serverRecv(Buffer* payload, ServerToken* token) = 0;

    /**
     * Respond to a specific RPC request.
     * \param[in] payload
     *      The RPC payload to send on the wire.
     * \param[in] token
     *      The token from the corresponding previous call to #serverRecv().
     * \throw TransportException
     *      If client has crashed.
     */
    virtual void serverSend(Buffer* payload, ServerToken* token) = 0;

    /**
     * Send an RPC request.
     * \param[in] service
     *      The service to which the request shall be sent.
     * \param[in] payload
     *      The RPC payload to send. The caller must not modify or even access
     *      \a payload until the corresponding call to #clientRecv() with the
     *      same \a token returns. The Transport may add new chunks to \a
     *      payload but will not modify its existing chunks.
     * \param[out] token
     *      A ClientToken that should be passed in to #clientSend() later.
     * \throw TransportException
     *      If \a service is unavailable.
     */
    virtual void clientSend(const Service* service, Buffer* payload,
                            ClientToken* token) = 0;

    /**
     * Wait for a specific RPC response to arrive.
     * \param[in] payload
     *      An initialized Buffer that will be filled in with the RPC payload.
     * \param[in] token
     *      The token from the corresponding previous call to #clientSend().
     * \throw TransportException
     *      If \a service has crashed.
     */
    virtual void clientRecv(Buffer* payload, ClientToken* token) = 0;

  private:
    DISALLOW_COPY_AND_ASSIGN(Transport);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_TRANSPORT_H
