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

#ifndef RAMCLOUD_TCPTRANSPORT_H
#define RAMCLOUD_TCPTRANSPORT_H

#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <map>
#include <list>

#include "Common.h"
#include "IpAddress.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * An inefficient TCP-based Transport implementation.
 * This lets you execute RPCs, but it's not going to be fast.
 */
class TCPTransport : public Transport {
  friend class TCPTransportTest;
  friend class SocketTest;

  public:
    /**
     * Construct a TCPTransport instance.
     * For exceptions, see #ListenSocket().
     * \param serviceLocator
     *      If non-NULL, the address on which to receive RPC requests.
     */
    explicit TCPTransport(const ServiceLocator* serviceLocator = NULL)
        : listenSocket(serviceLocator) {}

    ServerRpc* serverRecv() __attribute__((warn_unused_result));
    SessionRef getSession(const ServiceLocator& serviceLocator) {
        return new TCPSession(serviceLocator);
    }

    /**
     * A layer of indirection for the system calls used by TCPTransport.
     *
     * See the man pages for the identically named Linux/POSIX functions.
     *
     * This is only here for unit testing.
     * This is only public for the static initializers in TCPTransport.cc.
     */
    class Syscalls {
      public:
        Syscalls() {}
        VIRTUAL_FOR_TESTING ~Syscalls() {}

        VIRTUAL_FOR_TESTING
        int accept(int sockfd, struct sockaddr *addr, socklen_t *addrlen) {
            return ::accept(sockfd, addr, addrlen);
        }
        VIRTUAL_FOR_TESTING
        int bind(int sockfd, const struct sockaddr *addr, socklen_t addrlen) {
            return ::bind(sockfd, addr, addrlen);
        }
        VIRTUAL_FOR_TESTING
        int close(int fd) {
            return ::close(fd);
        }
        VIRTUAL_FOR_TESTING
        int connect(int sockfd, const struct sockaddr *addr,
                    socklen_t addrlen) {
            return ::connect(sockfd, addr, addrlen);
        }
        VIRTUAL_FOR_TESTING
        int fcntl(int fd, int cmd, int arg1) {
            return ::fcntl(fd, cmd, arg1);
        }
        VIRTUAL_FOR_TESTING
        int listen(int sockfd, int backlog) {
            return ::listen(sockfd, backlog);
        }
        VIRTUAL_FOR_TESTING
        ssize_t recv(int sockfd, void *buf, size_t len, int flags) {
            return ::recv(sockfd, buf, len, flags);
        }
        VIRTUAL_FOR_TESTING
        ssize_t sendmsg(int sockfd, const struct msghdr *msg, int flags) {
            return ::sendmsg(sockfd, msg, flags);
        }
        VIRTUAL_FOR_TESTING
        int setsockopt(int sockfd, int level, int optname, const void *optval,
                       socklen_t optlen) {
            return ::setsockopt(sockfd, level, optname, optval, optlen);
        }
        VIRTUAL_FOR_TESTING
        int socket(int domain, int type, int protocol) {
            return ::socket(domain, type, protocol);
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(Syscalls);
    };

#if TESTING
  public: // some of the test classes extend these
#else
  private:
#endif

    /**
     * The first application-level header on the wire.
     * This comes after the TCP headers added by the kernel but before the
     * payload passed in from higher level layers.
     */
    struct Header {
        /**
         * The size in bytes of the payload.
         * Must be less than or equal to #MAX_RPC_LEN.
         */
        uint32_t len;
    };

    /**
     * An abstract wrapper for a socket (a file descriptor).
     * Beyond just being a base class, this class will automatically close the
     * file descriptor when it's destroyed (see ~Socket()).
     */
    class Socket {
      friend class TCPTransportTest;
      friend class SocketTest;
      public:
        virtual ~Socket();
      protected:
        Socket();

        /**
         * The file descriptor that is wrapped.
         * This is initialized to -1.
         */
        int fd;

      private:
        DISALLOW_COPY_AND_ASSIGN(Socket);
    };

    /**
     * An abstract Socket on which you can send and receive messages.
     * (This is distinct from a ListenSocket which can only accept connections.)
     *
     * The concrete implementations are ServerSocket and ClientSocket.
     */
    class MessageSocket : public Socket {
      public:
        VIRTUAL_FOR_TESTING void recv(Buffer* payload);
        VIRTUAL_FOR_TESTING void send(const Buffer* payload);
      protected:

        /**
         * Constructor for MessageSocket.
         */
        MessageSocket() {}

      private:
        DISALLOW_COPY_AND_ASSIGN(MessageSocket);
    };

    // forward declaration, see below
    class ListenSocket;

    /**
     * A MessageSocket on which you can service inbound RPCs.
     */
    class ServerSocket : public MessageSocket {
      public:

        /**
         * Constructor for ServerSocket.
         * Be sure to call #init() before using this.
         */
        ServerSocket() {}

        VIRTUAL_FOR_TESTING void init(ListenSocket* listenSocket);
      private:
        DISALLOW_COPY_AND_ASSIGN(ServerSocket);
    };

    /**
     * A Socket which can listen for new connections.
     * This is used for initializing a ServerSocket.
     */
    class ListenSocket : public Socket {
      friend class ServerSocket;
      friend class SocketTest;
      friend class TCPTransportTest;
      public:
        explicit ListenSocket(const ServiceLocator* serviceLocator = NULL);
      private:
        int accept();
        DISALLOW_COPY_AND_ASSIGN(ListenSocket);
    };

    /**
     * A MessageSocket on which you can send outbound RPCs.
     */
    class ClientSocket : public MessageSocket {
      public:

        /**
         * Constructor for ClientSocket.
         * Be sure to call one of the #init() variants before using this.
         */
        ClientSocket() {}

        VIRTUAL_FOR_TESTING void init(const IpAddress& address);
      private:
        DISALLOW_COPY_AND_ASSIGN(ClientSocket);
    };


    /**
     * The TCP implementation of Transport::ServerRpc.
     */
    class TCPServerRpc : public Transport::ServerRpc {
      friend class TCPTransportTest;
      public:

        /**
         * Constructor for TCPServerRpc.
         *
         * Normally this sets #serverSocket to #realServerSocket. If
         * mockServerSocket is not \c NULL, however, it will be set to that
         * instead (used for testing).
         */
        // TODO(ongaro): Move constructor into cc file?
        TCPServerRpc() : realServerSocket(), serverSocket(&realServerSocket) {
#if TESTING
            if (mockServerSocket != NULL)
                serverSocket = mockServerSocket;
#endif
        }

        void sendReply();

      private:

        /**
         * The server socket that is typically used.
         */
        ServerSocket realServerSocket;

      public:

        /**
         * A pointer to the server socket in actual use.
         */
        ServerSocket* CONST_FOR_PRODUCTION serverSocket;

#if TESTING
        static ServerSocket* mockServerSocket;
#endif

      private:
        DISALLOW_COPY_AND_ASSIGN(TCPServerRpc);
    };

    /**
     * The TCP implementation of Transport::ClientRpc.
     */
    class TCPClientRpc : public Transport::ClientRpc {
      friend class TCPTransportTest;
      public:

        /**
         * Constructor for TCPClientRpc.
         *
         * Normally this sets #clientSocket to #realClientSocket. If
         * mockClientSocket is not \c NULL, however, it will be set to that
         * instead (used for testing).
         */
        // TODO(ongaro): Move constructor into cc file?
        TCPClientRpc() : reply(NULL), realClientSocket(),
                         clientSocket(&realClientSocket) {
#if TESTING
            if (mockClientSocket != NULL)
                clientSocket = mockClientSocket;
#endif
        }

        void getReply();

        Buffer* reply;

      private:

        /**
         * The client socket that is typically used.
         */
        ClientSocket realClientSocket;

      public:

        /**
         * A pointer to the client socket in actual use.
         */
        ClientSocket* CONST_FOR_PRODUCTION clientSocket;

#if TESTING
        static ClientSocket* mockClientSocket;
#endif

      private:
        DISALLOW_COPY_AND_ASSIGN(TCPClientRpc);
    };

  private:

    class TCPSession : public Session {
      public:
        explicit TCPSession(const ServiceLocator& serviceLocator)
            : address(serviceLocator) {
        }
        ClientRpc* clientSend(Buffer* request, Buffer* response)
            __attribute__((warn_unused_result));
        void release() {
            delete this;
        }
      private:
        IpAddress address;
    };

    /**
     * The socket on which to listen for new connections.
     * This isn't used for transports that are only acting as a client.
     */
    ListenSocket listenSocket;

    static Syscalls* sys;

    DISALLOW_COPY_AND_ASSIGN(TCPTransport);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_TCPTRANSPORT_H
