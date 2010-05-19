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

#include <memory>

namespace RAMCloud {

/**
 * The TCPTransport::Syscalls implementation that is normally used.
 */
TCPTransport::Syscalls _sys;

/**
 * A pointer to the TCPTransport::Syscalls implementation in actual use. Used
 * for unit testing. Normally set to #_sys.
 */
TCPTransport::Syscalls* TCPTransport::sys = &_sys;

#if TESTING
/**
 * A pointer to a mock client socket to use temporarily during
 * construction. Used for unit testing. Normally set to \c NULL.
 */
TCPTransport::ServerSocket*
    TCPTransport::TCPServerRPC::mockServerSocket = NULL;

/**
 * A pointer to a mock client socket to use temporarily during
 * construction. Used for unit testing. Normally set to \c NULL.
 */
TCPTransport::ClientSocket*
    TCPTransport::TCPClientRPC::mockClientSocket = NULL;
#endif

/**
 * Constructor for Socket.
 */
TCPTransport::Socket::Socket() : fd(-1)
{
}

/**
 * Destructor for socket. Will close #fd if it's non-negative.
 */
TCPTransport::Socket::~Socket()
{
    if (fd >= 0) {
        sys->close(fd);
        fd = -1;
    }
}

/**
 * Receive a single message.
 * \param payload
 *      An empty buffer to which the message contents will be added.
 *      Keep in mind this may be a message of 0 bytes (which is distinct from
 *      an error).
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
 * Send a single message.
 * \param payload
 *      A buffer containing the contents of the message to send. This may be
 *      empty, in which case a message of 0 bytes will be sent.
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
 * Initialize a ServerSocket.
 * You should call this exactly once before using the object.
 * \throw UnrecoverableTransportException
 *      Errors from #TCPTransport::ListenSocket::accept().
 */
void
TCPTransport::ServerSocket::init(ListenSocket* listenSocket)
{
    assert(fd < 0);
    fd = listenSocket->accept();
}

/**
 * Construct a ListenSocket and begin listening for incoming connections.
 *
 * If you pass \c NULL for \a ip or 0 for \a port, this is a no-op.
 *
 * \param ip
 *      The IP address to listen on in numbers-and-dots notation (as a string).
 *      The caller may modify this buffer following the return of this
 *      constructor.
 * \param port
 *      The port number to listen on in host byte order.
 * \exception UnrecoverableTransportException
 *      Bad IP address.
 * \exception UnrecoverableTransportException
 *      Errors trying to create, bind, listen to the socket.
 */
TCPTransport::ListenSocket::ListenSocket(const char* ip, uint16_t port)
{
    if (ip == NULL || port == 0)
        return;

    fd = sys->socket(PF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        throw UnrecoverableTransportException(errno);
    }

    int optval = 1;
    (void) sys->setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optval,
                           sizeof(optval));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_aton(ip, &addr.sin_addr) == 0)
        throw UnrecoverableTransportException("Bad IP address");

    if (sys->bind(fd, (struct sockaddr*) &addr, sizeof(addr)) == -1) {
        // destructor will close fd
        throw UnrecoverableTransportException(errno);
    }

    if (sys->listen(fd, INT_MAX) == -1) {
        // destructor will close fd
        throw UnrecoverableTransportException(errno);
    }
}

/**
 * Accept a new connection.
 * \return
 *      A non-negative file descriptor for the new connection.
 * \throw UnrecoverableTransportException
 *      Non-transient errors accepting a new connection.
 */
int
TCPTransport::ListenSocket::accept()
{
    // If you opted out of listening in the constructor,
    // you're not allowed to accept now.
    assert(fd > 0);

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
 * Initialize a ClientSocket.
 *
 * This creates a connection with a server.
 *
 * You should call this exactly once before using the object.
 *
 * \param ip
 *      The IP address to connect to in numbers-and-dots notation.
 * \param port
 *      The port to connect to in host byte order.
 * \throw UnrecoverableTransportException
 *      Error creating socket or fatal error connecting.
 * \throw UnrecoverableTransportException
 *      Bad IP address.
 * \throw TransportException
 *      Server refused connection or timed out.
 */
void
TCPTransport::ClientSocket::init(const char* ip, uint16_t port)
{
    fd = sys->socket(PF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        throw UnrecoverableTransportException(errno);
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_aton(ip, &addr.sin_addr) == 0)
        throw UnrecoverableTransportException("Bad IP address");

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

void
TCPTransport::TCPServerRPC::sendReply(Buffer* payload)
{
    // "delete this;" on our way out of the method
    std::auto_ptr<TCPServerRPC> suicide(this);

    serverSocket->send(payload);
}

void
TCPTransport::TCPServerRPC::ignore()
{
    delete this;
}

void
TCPTransport::TCPClientRPC::getReply()
{
    // "delete this;" on our way out of the method
    std::auto_ptr<TCPClientRPC> suicide(this);

    clientSocket->recv(reply);
}

/**
 * Constructor for TCPTransport.
 *
 * If this is a client transport only, use \c NULL for \a ip and 0 for \a port.
 * It is then illegal to call #serverRecv().
 *
 * \param ip
 *      The IP address to listen on in numbers-and-dots notation (as a string).
 *      The caller may modify this buffer following the return of this
 *      constructor. For client transports, see above.
 * \param port
 *      The port number to listen on in host byte order. For client transports,
 *      see above.
 * \exception UnrecoverableTransportException
 *      Errors trying to listen on the IP address and port.
 */
TCPTransport::TCPTransport(const char* ip, uint16_t port)
    : listenSocket(ip, port)
{
}

Transport::ServerRPC*
TCPTransport::serverRecv(Buffer* payload)
{
    std::auto_ptr<TCPServerRPC> rpc(new TCPServerRPC());

    while (true) {
        rpc->serverSocket->init(&listenSocket);
        try {
            rpc->serverSocket->recv(payload);
            break;
        } catch (TransportException e) {}
    }

    return rpc.release();
}

Transport::ClientRPC*
TCPTransport::clientSend(const Service* service, Buffer* request,
                         Buffer* response)
{
    std::auto_ptr<TCPClientRPC> rpc(new TCPClientRPC());

    rpc->clientSocket->init(service->getIp(), service->getPort());
    rpc->clientSocket->send(request);
    rpc->reply = response;

    return rpc.release();
}

}  // namespace RAMCloud
