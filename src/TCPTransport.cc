/* Copyright (c) 2009-2010 Stanford University
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
 * Implementation for the TCPTransport class.
 */

#include <Common.h>

#include <TCPTransport.h>

#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <poll.h>


namespace RAMCloud {

TCPTransport::Syscalls _sys;
TCPTransport::Syscalls* TCPTransport::sys = &_sys;
#ifdef TESTING
TCPTransport::ServerSocket*
    TCPTransport::TCPServerToken::mockServerSocket = NULL;
TCPTransport::ClientSocket*
    TCPTransport::TCPClientToken::mockClientSocket = NULL;
#endif

TCPTransport::Socket::Socket() : fd(-1)
{
}

TCPTransport::Socket::~Socket()
{
    if (fd >= 0) {
        sys->close(fd);
        fd = -1;
    }
}

/**
 * \throw TransportException
 *      There was an error on the connection.
 */
void
TCPTransport::MessageSocket::recv(Buffer* payload)
{
    assert(fd >= 0);

    // receive header
    Header header;
    {
        ssize_t len = sys->recv(fd, &header, sizeof(header), MSG_WAITALL);
        if (len == -1) {
            int e = errno;
            sys->close(fd);
            fd = -1;
            throw TransportException(e);
        } else if (len == 0) {
            sys->close(fd);
            fd = -1;
            throw TransportException("peer performed orderly shutdown");
        }
        assert(len == sizeof(header));
    }

    if (header.len > MAX_RPC_LEN) {
        sys->close(fd);
        fd = -1;
        throw TransportException("peer trying to send too much data");
    }

    if (header.len == 0)
        return;

    // receive RPC data
    void* data = xmalloc(header.len);
    {
        ssize_t len = sys->recv(fd, data, header.len, MSG_WAITALL);
        if (len == -1) {
            int e = errno;
            free(data);
            sys->close(fd);
            fd = -1;
            throw TransportException(e);
        } else if (len == 0) {
            free(data);
            sys->close(fd);
            fd = -1;
            throw TransportException("peer performed orderly shutdown");
        }
        assert(len == header.len);
    }

    // TODO(ongaro): payload should free() this chunk later
    payload->append(data, header.len);
}

/**
 * \throw TransportException
 *      There was an error on the connection.
 */
void
TCPTransport::MessageSocket::send(const Buffer* payload)
{
    assert(fd >= 0);

    Header header;
    header.len = payload->totalLength();

    // one for header, the rest for payload
    uint32_t iovecs = 1 + payload->numberChunks();

    struct iovec iov[iovecs];
    iov[0].iov_base = &header;
    iov[0].iov_len = sizeof(header);

    Buffer::Iterator iter(*payload);
    int i = 1;
    while (!iter.isDone()) {
        iov[i].iov_base = const_cast<void*>(iter.getData());
        iov[i].iov_len = iter.getLength();
        ++i;
        iter.next();
    }

    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = iov;
    msg.msg_iovlen = iovecs;

    ssize_t r = sys->sendmsg(fd, &msg, 0);
    if (r == -1) {
        int e = errno;
        sys->close(fd);
        fd = -1;
        throw TransportException(e);
    }
    assert(static_cast<size_t>(r) == sizeof(header) + header.len);
}

/**
 * \throw UnrecoverableTransportException
 *      Errors from #TCPTransport::ListenSocket::accept().
 */
void
TCPTransport::ServerSocket::init(ListenSocket* listenSocket)
{
    fd = listenSocket->accept();
}

TCPTransport::ListenSocket::ListenSocket(uint32_t ip, uint16_t port) : addr()
{
    this->addr.sin_family = AF_INET;
    this->addr.sin_port = htons(port);
    this->addr.sin_addr.s_addr = ip;
}

/**
 * \exception UnrecoverableTransportException
 *      Errors trying to create, bind, listen to the socket.
 */
void
TCPTransport::ListenSocket::listen()
{
    if (fd >= 0)
        return;

    fd = sys->socket(PF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        throw UnrecoverableTransportException(errno);
    }

    int optval = 1;
    (void) sys->setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optval,
                           sizeof(optval));

    if (sys->bind(fd, (struct sockaddr*) &addr, sizeof(addr)) == -1) {
        int e = errno;
        sys->close(fd);
        fd = -1;
        throw UnrecoverableTransportException(e);
    }

    if (sys->listen(fd, INT_MAX) == -1) {
        int e = errno;
        sys->close(fd);
        fd = -1;
        throw UnrecoverableTransportException(e);
    }
}

/**
 * \throw UnrecoverableTransportException
 *      Errors from #listen(); errors accepting a new connection.
 */
int
TCPTransport::ListenSocket::accept()
{
    listen();
    while (true) {
        int acceptedFd = sys->accept(fd, NULL, NULL);
        if (acceptedFd >= 0)
            return acceptedFd;
        switch (errno) {
            case EHOSTDOWN:
            case EHOSTUNREACH:
            case ENETDOWN:
            case ENETUNREACH:
            case ENONET:
            case ENOPROTOOPT:
            case EOPNOTSUPP:
            case EPROTO:
                // According to the man page, you're supposed to treat these as
                // retry on Linux.
                break;
            default:
                throw UnrecoverableTransportException(errno);
        }
    }
}


/**
 * \throw UnrecoverableTransportException
 *      See #init(uint32_t ip, uint16_t port).
 * \throw TransportException
 *      See #init(uint32_t ip, uint16_t port).
 */
void
TCPTransport::ClientSocket::init(const char* ip, uint16_t port)
{
    init(inet_addr(ip), port);
}

/**
 * \throw UnrecoverableTransportException
 *      Error creating socket or fatal error connecting.
 * \throw TransportException
 *      Server refused connection or timed out.
 */
void
TCPTransport::ClientSocket::init(uint32_t ip, uint16_t port)
{
    fd = sys->socket(PF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        throw UnrecoverableTransportException(errno);
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = ip;

    int r = sys->connect(fd, reinterpret_cast<struct sockaddr*>(&addr),
                      sizeof(addr));
    if (r == -1) {
        int e = errno;
        sys->close(fd);
        fd = -1;
        switch (e) {
            case ECONNREFUSED:
            case ETIMEDOUT:
                throw TransportException(e);
            default:
                throw UnrecoverableTransportException(e);
        }
    }
}

TCPTransport::TCPTransport(const char* ip, uint16_t port)
    : listenSocket(inet_addr(ip), port)
{
}

TCPTransport::TCPTransport(uint32_t ip, uint16_t port)
    : listenSocket(ip, port)
{
}

void
TCPTransport::serverRecv(Buffer* payload, ServerToken* token)
{
    TCPServerToken* tcpToken = token->reinit<TCPServerToken>();

    while (true) {
        tcpToken->serverSocket->init(&listenSocket);
        try {
            tcpToken->serverSocket->recv(payload);
            break;
        } catch (TransportException e) {}
    }
}

void
TCPTransport::serverSend(Buffer* payload, ServerToken* token)
{
    TCPServerToken* tcpToken = token->getBuf<TCPServerToken>();

    tcpToken->serverSocket->send(payload);
    token->reinit(); // not strictly necessary but releases fd earlier
}

void
TCPTransport::clientSend(const Service* service, Buffer* payload,
                         ClientToken* token)
{
    TCPClientToken* tcpToken = token->reinit<TCPClientToken>();

    tcpToken->clientSocket->init(service->getIp(), service->getPort());
    tcpToken->clientSocket->send(payload);
}

void
TCPTransport::clientRecv(Buffer* payload, ClientToken* token)
{
    TCPClientToken* tcpToken = token->getBuf<TCPClientToken>();

    tcpToken->clientSocket->recv(payload);
    token->reinit(); // not strictly necessary but releases fd earlier
}

}  // namespace RAMCloud
