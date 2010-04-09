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
  public:
    TCPTransport(const char* ip, uint16_t port);
    TCPTransport(uint32_t ip, uint16_t port);

    void serverRecv(Buffer* payload, Transport::ServerToken* token);
    void serverSend(Buffer* payload, Transport::ServerToken* token);
    void clientSend(const Service* service, Buffer* payload,
                    Transport::ClientToken* token);
    void clientRecv(Buffer* payload, Transport::ClientToken* token);

  private:
    struct Header {
        uint32_t len;
    };

    /**
     * Abstract socket.
     */
    class Socket {
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
        void recv(Buffer* payload);
        void send(const Buffer* payload);
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
        void init(ListenSocket* listenSocket);
      private:
        DISALLOW_COPY_AND_ASSIGN(ServerSocket);
    };

    /**
     * A socket which can listen for new connections.
     */
    class ListenSocket : public Socket {
      friend class ServerSocket;
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
        void init(uint32_t ip, uint16_t port);
      private:
        DISALLOW_COPY_AND_ASSIGN(ClientSocket);
    };

    class TCPServerToken : public BaseServerToken {
      public:
        TCPServerToken() : serverSocket() {}
        ServerSocket serverSocket;
      private:
        DISALLOW_COPY_AND_ASSIGN(TCPServerToken);
    };

    class TCPClientToken : public BaseClientToken {
      public:
        TCPClientToken() : clientSocket() {}
        ClientSocket clientSocket;
      private:
        DISALLOW_COPY_AND_ASSIGN(TCPClientToken);
    };

  private:
    ListenSocket listenSocket;
    DISALLOW_COPY_AND_ASSIGN(TCPTransport);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_TCPTRANSPORT_H
