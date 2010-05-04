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
 * Header file for the TCPTransport class.
 */

#ifndef RAMCLOUD_TCPTRANSPORT_H
#define RAMCLOUD_TCPTRANSPORT_H

#include <Common.h>
#include <Transport.h>

#include <sys/socket.h>
#include <netinet/in.h>

#include <map>
#include <list>

namespace RAMCloud {

class TCPTransport : public Transport {
  friend class TCPTransportTest;
  friend class SocketTest;
  friend class TestServerSocket;

  public:

    TCPTransport(const char* ip, uint16_t port);
    TCPTransport(uint32_t ip, uint16_t port);

    void serverRecv(Buffer* payload, Transport::ServerToken* token);
    void serverSend(Buffer* payload, Transport::ServerToken* token);
    void clientSend(const Service* service, Buffer* payload,
                    Transport::ClientToken* token);
    void clientRecv(Buffer* payload, Transport::ClientToken* token);

    /**
     * This is only public for the static initializers in TCPTransport.cc.
     */
    class Syscalls {
      public:
        VIRTUAL_FOR_TESTING ~Syscalls() {}
        VIRTUAL_FOR_TESTING
        int accept(int sockfd, struct sockaddr *addr,
                           socklen_t *addrlen) {
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
        int connect(int sockfd, const struct sockaddr *addr, socklen_t addrlen)
        {
            return ::connect(sockfd, addr, addrlen);
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
    };

#if !TESTING
  private:
#endif

    struct Header {
        uint32_t len;
    };

    /**
     * Abstract socket.
     */
    class Socket {
      friend class TCPTransportTest;
      friend class SocketTest;

      public:
        virtual ~Socket();
      protected:
        Socket();
        int fd;
      private:
        DISALLOW_COPY_AND_ASSIGN(Socket);
    };

    /**
     * An abstract socket on which you can send and receive messages.
     */
    class MessageSocket : public Socket {
      public:
        VIRTUAL_FOR_TESTING void recv(Buffer* payload);
        VIRTUAL_FOR_TESTING void send(const Buffer* payload);
      protected:
        MessageSocket() {}
      private:
        DISALLOW_COPY_AND_ASSIGN(MessageSocket);
    };

    class ListenSocket;

    /**
     * A socket on which you can service RPCs.
     */
    class ServerSocket : public MessageSocket {
      public:
        ServerSocket() {}
        VIRTUAL_FOR_TESTING void init(ListenSocket* listenSocket);
      private:
        DISALLOW_COPY_AND_ASSIGN(ServerSocket);
    };

    /**
     * A socket which can listen for new connections.
     */
    class ListenSocket : public Socket {
      friend class ServerSocket;
      friend class SocketTest;
      friend class TCPTransportTest;
      public:
        ListenSocket(uint32_t ip, uint16_t port);
      private:
        void listen();
        int accept();
        struct sockaddr_in addr;
        DISALLOW_COPY_AND_ASSIGN(ListenSocket);
    };

    /**
     * A socket on which you can send RPCs.
     */
    class ClientSocket : public MessageSocket {
      public:
        ClientSocket() {}
        void init(const char* ip, uint16_t port);
        VIRTUAL_FOR_TESTING void init(uint32_t ip, uint16_t port);
      private:
        DISALLOW_COPY_AND_ASSIGN(ClientSocket);
    };

    class TCPServerToken : public BaseServerToken {
      friend class TCPTransportTest;
      public:
        TCPServerToken() : realServerSocket(), serverSocket(&realServerSocket) {
#if TESTING
            if (mockServerSocket != NULL)
                serverSocket = mockServerSocket;
#endif
        }
      private:
        ServerSocket realServerSocket;
      public:
        ServerSocket* CONST_FOR_PRODUCTION serverSocket;
#if TESTING
        static ServerSocket* mockServerSocket;
#endif
      private:
        DISALLOW_COPY_AND_ASSIGN(TCPServerToken);
    };

    class TCPClientToken : public BaseClientToken {
      friend class TCPTransportTest;
      public:
        TCPClientToken() : realClientSocket(), clientSocket(&realClientSocket) {
#if TESTING
            if (mockClientSocket != NULL)
                clientSocket = mockClientSocket;
#endif
        }
      private:
        ClientSocket realClientSocket;
      public:
        ClientSocket* CONST_FOR_PRODUCTION clientSocket;
#if TESTING
        static ClientSocket* mockClientSocket;
#endif
      private:
        DISALLOW_COPY_AND_ASSIGN(TCPClientToken);
    };

  private:
    ListenSocket listenSocket;
    static Syscalls* sys;
    DISALLOW_COPY_AND_ASSIGN(TCPTransport);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_TCPTRANSPORT_H
