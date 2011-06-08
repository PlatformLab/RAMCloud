/* Copyright (c) 2010-2011 Stanford University
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

#ifndef RAMCLOUD_TCPTRANSPORT_H
#define RAMCLOUD_TCPTRANSPORT_H

#include <event.h>
#include <queue>

#include "Dispatch.h"
#include "IpAddress.h"
#include "Tub.h"
#include "Syscall.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * A simple transport mechanism based on TCP/IP provided by the kernel.
 * This implementation is unlikely to be fast enough for production use;
 * this class will be used primarily for development and as a baseline
 * for testing.  The goal is to provide an implementation that is about as
 * fast as possible, given its use of kernel-based TCP/IP.
 */
class TcpTransport : public Transport {
  public:
    explicit TcpTransport(const ServiceLocator* serviceLocator = NULL);
    ~TcpTransport();
    SessionRef getSession(const ServiceLocator& serviceLocator) {
        return new TcpSession(serviceLocator);
    }
    ServiceLocator getServiceLocator() {
        return ServiceLocator(locatorString);
    }
    void registerMemory(void* base, size_t bytes) {}

  PRIVATE:
    friend class TcpTransportTest;
    friend class AcceptHandler;
    friend class RequestReadHandler;
    class IncomingMessage;
    class ReplyReadHandler;
    class Socket;
    class TcpSession;
    /**
     * Header for request and response messages: precedes the actual data
     * of the message in all transmissions.
     */
    struct Header {
        /**
         * The size in bytes of the payload (which follows immediately).
         * Must be less than or equal to #MAX_RPC_LEN.
         */
        uint32_t len;
    };

    /**
     * Used to manage the receipt of a message (on either client or server)
     * using an event-based approach.
     */
    class IncomingMessage {
      friend class TcpTransportTest;
      public:
        explicit IncomingMessage(Buffer* buffer);
        void reset(Buffer* buffer);
        bool readMessage(int fd);
      PRIVATE:
        Header header;

        /// The number of bytes of header that have been successfully
        /// received so far; 0 means the header has not yet been received;
        /// sizeof(Header) means the header is complete.
        uint32_t headerBytesReceived;

        /// Counts the number of bytes in the message body that have been
        /// received so far.
        uint32_t messageBytesReceived;

        /// Holds the contents of the actual message (not including header).
        Buffer *buffer;
    };

  public:
    /**
     * The TCP implementation of Transport::ServerRpc.
     */
    class TcpServerRpc : public Transport::ServerRpc {
      friend class TcpTransportTest;
      friend class TcpTransport;
      public:
        void sendReply();
      PRIVATE:
        TcpServerRpc(Socket* socket, int fd, Buffer* buffer)
            : fd(fd), socket(socket), message(buffer) { }

        int fd;                   /// File descriptor of the socket on
                                  /// which the request was received.
        Socket* socket;           /// Transport state corresponding to fd.
        IncomingMessage message;  /// Records state of partially-received
                                  /// request.

        DISALLOW_COPY_AND_ASSIGN(TcpServerRpc);
    };

    /**
     * The TCP implementation of Transport::ClientRpc.
     */
    class TcpClientRpc : public Transport::ClientRpc {
      public:
        friend class TcpTransportTest;
        friend class TcpTransport;
        friend class TcpSession;
        explicit TcpClientRpc(TcpSession* session, Buffer*request,
                Buffer* reply)
            : request(request), reply(reply), session(session) { }
        bool isReady();
        void wait();
      PRIVATE:
        Buffer* request;          /// Contains request message.
        Buffer* reply;            /// Client's buffer for response.
        TcpSession *session;      /// Session used for this RPC.
        DISALLOW_COPY_AND_ASSIGN(TcpClientRpc);
    };

  PRIVATE:
    void closeSocket(int fd);
    static ssize_t recvCarefully(int fd, void* buffer, size_t length);
    static void sendMessage(int fd, Buffer& payload);

    /**
     * An exception that is thrown when a socket has been closed by the peer.
     */
    class TcpTransportEof : public TransportException {
      public:
        explicit TcpTransportEof(const CodeLocation& where)
            : TransportException(where) {}
    };

    /**
     * An event handler that will accept connections on a socket.
     */
    class AcceptHandler : public Dispatch::File {
      public:
        AcceptHandler(int fd, TcpTransport* transport);
        virtual void handleFileEvent();
      PRIVATE:
        // Transport that manages this socket.
        TcpTransport* transport;
        DISALLOW_COPY_AND_ASSIGN(AcceptHandler);
    };

    /**
     * An event handler that reads incoming RPC requests for servers.
     */
    class RequestReadHandler : public Dispatch::File {
      public:
        RequestReadHandler(int fd, TcpTransport* transport);
        virtual void handleFileEvent();
      PRIVATE:
        // The following variables are just copies of constructor arguments.
        int fd;
        TcpTransport* transport;
        DISALLOW_COPY_AND_ASSIGN(RequestReadHandler);
    };

    /**
     * An event handler that reads RPC responses for clients.
     */
    class ReplyReadHandler : public Dispatch::File {
      public:
        ReplyReadHandler(int fd, TcpSession* session);
        virtual void handleFileEvent();
      PRIVATE:
        // The following variables are just copies of constructor arguments.
        int fd;
        TcpSession* session;
        DISALLOW_COPY_AND_ASSIGN(ReplyReadHandler);
    };

    /**
     * The TCP implementation of Sessions.
     */
    class TcpSession : public Session {
      friend class TcpTransportTest;
      friend class TcpClientRpc;
      friend class ReplyReadHandler;
      public:
        explicit TcpSession(const ServiceLocator& serviceLocator);
        ~TcpSession();
        ClientRpc* clientSend(Buffer* request, Buffer* reply)
            __attribute__((warn_unused_result));
        void release() {
            delete this;
        }
      PRIVATE:
        void close();
        static void tryReadReply(int fd, int16_t event, void *arg);

        IpAddress address;        /// Server to which requests will be sent.
        int fd;                   /// File descriptor for the socket that
                                  /// connects to address  -1 means no socket
                                  /// open.
        TcpClientRpc* current;    /// Only one RPC can be outstanding for
                                  /// a session at a time; this identifies
                                  /// the current RPC (NULL if there is none).
        std::queue<TcpClientRpc*> waitingRpcs;
                                  /// Holds requests that arrive when the
                                  /// session is busy (current != NULL).
        IncomingMessage message;  /// Records state of partially-received
                                  /// reply for current.
        Tub<ReplyReadHandler> replyHandler;
                                  /// Used to get notified when response data
                                  /// arrives.
        string errorInfo;         /// If the session is no longer usable,
                                  /// this variable indicates why.
        DISALLOW_COPY_AND_ASSIGN(TcpSession);
    };

    static Syscall* sys;

    /// Service locator used to open server socket (empty string if this
    /// isn't a server). May differ from what was passed to the constructor
    /// if dynamic ports are used.
    string locatorString;

    /// File descriptor used by servers to listen for connections from
    /// clients.  -1 means this instance is not a server.
    int listenSocket;

    /// Used to wait for listenSocket to become readable.
    Tub<AcceptHandler> acceptHandler;

    /// Used to hold information about a file descriptor associated with
    /// a socket, on which RPC requests may arrive.
    class Socket {
        public:
        Socket(int fd, TcpTransport *transport)
                : rpc(NULL), busy(false), readHandler(fd, transport) { }
        TcpServerRpc *rpc;        /// Incoming RPC that is in progress for
                                  /// this fd, or NULL if none.
        bool busy;                /// True means we have received a request
                                  /// and are in the middle of processing it,
                                  /// so no additional requests should arrive.
        RequestReadHandler readHandler;
                                  /// Used to get notified whenever data
                                  /// arrives on this fd.
        DISALLOW_COPY_AND_ASSIGN(Socket);
    };

    /// Keeps track of all of our open client connections. Entry i has
    /// information about file descriptor i (NULL means no client
    /// is currently connected).
    std::vector<Socket*> sockets;

    DISALLOW_COPY_AND_ASSIGN(TcpTransport);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_TCPTRANSPORT_H
