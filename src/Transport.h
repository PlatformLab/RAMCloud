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
 * Header file for the Transport class.
 */

#ifndef RAMCLOUD_TRANSPORT_H
#define RAMCLOUD_TRANSPORT_H

#include <Common.h>
#include <Service.h>
#include <Buffer.h>

#include <string>

namespace RAMCloud {

// stolen from BackupException
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

class Transport {

  protected:

    /**
     * Base class for tokens used in #serverRecv() and #serverSend().
     */
    class BaseServerToken {
      public:
        BaseServerToken() {}
        virtual ~BaseServerToken() {}
      private:
        DISALLOW_COPY_AND_ASSIGN(BaseServerToken);
    };

    /**
     * Base class for tokens used in #clientSend() and #clientRecv().
     */
    class BaseClientToken {
      public:
        BaseClientToken() {}
        virtual ~BaseClientToken() {}
      private:
        DISALLOW_COPY_AND_ASSIGN(BaseClientToken);
    };

  public:

    /**
     * Opaque container for any server token type derived from BaseServerToken.
     */
    class ServerToken
    {
      public:
        ServerToken()
        {
            static_assert(BUF_SIZE >= sizeof(BaseServerToken));
            new(buf) BaseServerToken();
        }

        ~ServerToken()
        {
            reinterpret_cast<BaseServerToken*>(buf)->~BaseServerToken();
        }

        void reinit() {
            reinterpret_cast<BaseClientToken*>(buf)->~BaseClientToken();
            new(buf) BaseClientToken();
        }

        template <typename T>
        T* getBuf() {
            return static_cast<T*>(reinterpret_cast<BaseServerToken*>(buf));
        }

        enum { BUF_SIZE = 32 };

      private:
        char buf[BUF_SIZE];
    };

    /**
     * Opaque container for any client token type derived from BaseClientToken.
     */
    class ClientToken
    {
      public:
        ClientToken()
        {
            static_assert(BUF_SIZE >= sizeof(BaseClientToken));
            new(buf) BaseClientToken();
        }

        ~ClientToken()
        {
            reinterpret_cast<BaseClientToken*>(buf)->~BaseClientToken();
        }

        void reinit() {
            reinterpret_cast<BaseClientToken*>(buf)->~BaseClientToken();
            new(buf) BaseClientToken();
        }

        template <typename T>
        T* getBuf() {
            return static_cast<T*>(reinterpret_cast<BaseClientToken*>(buf));
        }

        enum { BUF_SIZE = 32 };

      private:
        char buf[BUF_SIZE];
    };


    Transport() {}
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
