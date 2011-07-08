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

#include "Common.h"
#include "ServiceManager.h"
#include "TcpTransport.h"

namespace RAMCloud {

int TcpTransport::messageChunks = 0;

/**
 * Default object used to make system calls.
 */
static Syscall defaultSyscall;

/**
 * Used by this class to make all system calls.  In normal production
 * use it points to defaultSyscall; for testing it points to a mock
 * object.
 */
Syscall* TcpTransport::sys = &defaultSyscall;

/**
 * Construct a TcpTransport instance.
 * \param serviceLocator
 *      If non-NULL this transport will be used to serve incoming
 *      RPC requests as well as make outgoing requests; this parameter
 *      specifies the (local) address on which to listen for connections.
 *      If NULL this transport will be used only for outgoing requests.
 */
TcpTransport::TcpTransport(const ServiceLocator* serviceLocator)
        : locatorString(), listenSocket(-1), acceptHandler(), sockets()
{
    if (serviceLocator == NULL)
        return;
    IpAddress address(*serviceLocator);
    locatorString = serviceLocator->getOriginalString();

    listenSocket = sys->socket(PF_INET, SOCK_STREAM, 0);
    if (listenSocket == -1) {
        throw TransportException(HERE,
                "TcpTransport couldn't create listen socket", errno);
    }

    int r = sys->fcntl(listenSocket, F_SETFL, O_NONBLOCK);
    if (r != 0) {
        throw TransportException(HERE,
                "TcpTransport couldn't set nonblocking on listen socket",
                errno);
    }

    int optval = 1;
    if (sys->setsockopt(listenSocket, SOL_SOCKET, SO_REUSEADDR, &optval,
                           sizeof(optval)) != 0) {
        throw TransportException(HERE,
                "TcpTransport couldn't set SO_REUSEADDR on listen socket",
                errno);
    }

    if (sys->bind(listenSocket, &address.address,
            sizeof(address.address)) == -1) {
        // destructor will close listenSocket
        string message = format("TcpTransport couldn't bind to '%s'",
                serviceLocator->getOriginalString().c_str());
        throw TransportException(HERE, message, errno);
    }

    if (sys->listen(listenSocket, INT_MAX) == -1) {
        // destructor will close listenSocket
        throw TransportException(HERE,
                "TcpTransport couldn't listen on socket", errno);
    }

    // Arrange to be notified whenever anyone connects to listenSocket.
    acceptHandler.construct(listenSocket, this);
}

/**
 * Destructor for TcpTransports: close file descriptors and perform
 * any other needed cleanup.
 */
TcpTransport::~TcpTransport()
{
    if (listenSocket >= 0) {
        sys->close(listenSocket);
        listenSocket = -1;
    }
    for (unsigned int i = 0; i < sockets.size(); i++) {
        if (sockets[i] != NULL) {
            closeSocket(i);
        }
    }
}

/**
 * This private method is invoked to close the server's end of a
 * connection to a client and cleanup any related state.
 * \param fd
 *      File descriptor for the socket to be closed.
 */
void
TcpTransport::closeSocket(int fd) {
    delete sockets[fd];
    sockets[fd] = NULL;
    sys->close(fd);
}

/**
 * Destructor for Sockets.
 */
TcpTransport::Socket::~Socket() {
    if (rpc != NULL) {
        delete rpc;
    }
    while (!rpcsWaitingToReply.empty()) {
        TcpServerRpc& rpc = rpcsWaitingToReply.front();
        rpcsWaitingToReply.pop_front();
        delete &rpc;
    }
}


/**
 * Constructor for AcceptHandlers.
 *
 * \param fd
 *      File descriptor for a socket on which the #listen system call has
 *      been invoked.
 * \param transport
 *      The TcpTransport that manages this socket.
 */
TcpTransport::AcceptHandler::AcceptHandler(int fd, TcpTransport* transport)
    : Dispatch::File(fd, Dispatch::FileEvent::READABLE), transport(transport)
{
    // Empty constructor body.
}

/**
 * This method is invoked by Dispatch when a listening socket becomes
 * readable; it tries to open a new connection with a client. If that
 * succeeds then we will begin listening on that socket for RPCs.
 *
 * \param events
 *      Indicates whether the socket was readable, writable, or both
 *      (OR-ed combination of Dispatch::FileEvent bits).
 */
void
TcpTransport::AcceptHandler::handleFileEvent(int events)
{
    int acceptedFd = sys->accept(transport->listenSocket, NULL, NULL);
    if (acceptedFd < 0) {
        switch (errno) {
            // According to the man page for accept, you're supposed to
            // treat these as retry on Linux.
            case EHOSTDOWN:
            case EHOSTUNREACH:
            case ENETDOWN:
            case ENETUNREACH:
            case ENONET:
            case ENOPROTOOPT:
            case EOPNOTSUPP:
            case EPROTO:
                return;

            // No incoming connections are currently available.
            case EAGAIN:
#if EAGAIN != EWOULDBLOCK
            case EWOULDBLOCK:
#endif
                return;
        }

        // Unexpected error: log a message and then close the socket
        // (so we don't get repeated errors).
        LOG(ERROR, "error in TcpTransport::AcceptHandler accepting "
                "connection for '%s': %s",
                transport->locatorString.c_str(), strerror(errno));
        setEvents(0);
        sys->close(transport->listenSocket);
        transport->listenSocket = -1;
        return;
    }

    // At this point we have successfully opened a client connection.
    // Save information about it and create a handler for incoming
    // requests.
    if (transport->sockets.size() <=
            static_cast<unsigned int>(acceptedFd)) {
        transport->sockets.resize(acceptedFd + 1);
    }
    transport->sockets[acceptedFd] = new Socket(acceptedFd, transport);
}

/**
 * Constructor for ServerSocketHandlers.
 *
 * \param fd
 *      File descriptor for a client socket on which RPC requests may arrive.
 * \param transport
 *      The TcpTransport that manages this socket.
 * \param socket
 *      Socket object corresponding to fd.
 */
TcpTransport::ServerSocketHandler::ServerSocketHandler(int fd,
        TcpTransport* transport, Socket* socket)
        : Dispatch::File(fd, Dispatch::FileEvent::READABLE),
        fd(fd), transport(transport), socket(socket)
{
    // Empty constructor body.
}

/**
 * This method is invoked by Dispatch when a client socket becomes readable
 * or writable.  It attempts to read incoming messages from the socket.  If
 * a full message is available, a TcpServerRpc object gets queued for service.
 * It also attempts to write responses to the socket (if there are responses
 * waiting for transmission).
 *
 * \param events
 *      Indicates whether the socket was readable, writable, or both
 *      (OR-ed combination of Dispatch::FileEvent bits).
 */
void
TcpTransport::ServerSocketHandler::handleFileEvent(int events)
{
    Socket* socket = transport->sockets[fd];
    assert(socket != NULL);
    try {
        if (events & Dispatch::FileEvent::READABLE) {
            if (socket->rpc == NULL) {
                socket->rpc = new TcpServerRpc(socket, fd);
            }
            if (socket->rpc->message.readMessage(fd)) {
                // The incoming request is complete; pass it off for servicing.
                TcpServerRpc *rpc = socket->rpc;
                socket->rpc = NULL;
                serviceManager->handleRpc(rpc);
            }
        }
        if (events & Dispatch::FileEvent::WRITABLE) {
            // We should only get here if a reply could not be transmitted in
            // its entirety because it would have blocked on I/O.
            assert(!socket->rpcsWaitingToReply.empty());
            while (true) {
                TcpServerRpc& rpc = socket->rpcsWaitingToReply.front();
                socket->bytesLeftToSend = TcpTransport::sendMessage(fd,
                        rpc.message.header.nonce, rpc.replyPayload,
                        socket->bytesLeftToSend);
                if (socket->bytesLeftToSend != 0) {
                    break;
                }
                // The current reply is finished; start the next one, if
                // there is one.
                socket->rpcsWaitingToReply.pop_front();
                delete &rpc;
                socket->bytesLeftToSend = -1;
                if (socket->rpcsWaitingToReply.empty()) {
                    setEvents(Dispatch::FileEvent::READABLE);
                    break;
                }
            }
        }
    } catch (TcpTransportEof& e) {
        // Close the socket in order to prevent an infinite loop of
        // calls to this method.
        transport->closeSocket(fd);
    } catch (TransportException& e) {
        LOG(ERROR, "TcpTransport::ServerSocketHandler closing client "
                "connection: %s", e.message.c_str());
        transport->closeSocket(fd);
    }
}

/**
 * Transmit an RPC request or response on a socket.  This method uses
 * a nonblocking approach: if entire message cannot be transmitted,
 * entrances as many bytes as possible and returns information about
 * how much more work is still left to do.
 *
 * \param fd
 *      File descriptor to write.
 * \param nonce
 *      Unique identifier for the RPC; must never have been used
 *      before on this socket.
 * \param payload
 *      Message to transmit on fd; this method adds on a header.
 * \param bytesToSend
 *      -1 means the entire message must still be transmitted;
 *      Anything else means that part of the message was transmitted
 *      in a previous call, and the value of this parameter is the
 *      result returned by that call (always greater than 0).
 *
 * \return
 *      The number of (trailing) bytes that could not be transmitted.
 *      0 means the entire message was sent successfully.
 *
 * \throw TransportException
 *      An I/O error occurred.
 */
int
TcpTransport::sendMessage(int fd, uint64_t nonce, Buffer& payload,
        int bytesToSend)
{
    assert(fd >= 0);

    Header header;
    header.nonce = nonce;
    header.len = payload.getTotalLength();
    int totalLength = downCast<int>(sizeof(header) + header.len);
    if (bytesToSend < 0) {
        bytesToSend = totalLength;
    }
    int alreadySent = totalLength - bytesToSend;

    // Use an iovec to send everything in one kernel call: one iov
    // for header, the rest for payload.  Skip parts that have
    // already been sent.
    uint32_t iovecs = 1 + payload.getNumberChunks();
    struct iovec iov[iovecs];
    int offset;
    int iovecIndex;
    if (alreadySent < downCast<int>(sizeof(header))) {
        iov[0].iov_base = reinterpret_cast<char*>(&header) + alreadySent;
        iov[0].iov_len = sizeof(header) - alreadySent;
        iovecIndex = 1;
        offset = 0;
    } else {
        iovecIndex = 0;
        offset = alreadySent - downCast<int>(sizeof(header));
    }
    Buffer::Iterator iter(payload, offset, header.len - offset);
    while (!iter.isDone()) {
        iov[iovecIndex].iov_base = const_cast<void*>(iter.getData());
        iov[iovecIndex].iov_len = iter.getLength();
        ++iovecIndex;
        iter.next();
    }

    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = iov;
    msg.msg_iovlen = iovecIndex;

    int r = downCast<int>(sys->sendmsg(fd, &msg,
            MSG_NOSIGNAL|MSG_DONTWAIT));
    if (r == bytesToSend)
        return 0;
#if TESTING
    if ((r > 0) && (r < totalLength)) {
        messageChunks++;
    }
#endif
    if (r == -1) {
        if ((errno != EAGAIN) && (errno != EWOULDBLOCK)) {
            throw TransportException(HERE,
                    "I/O error in TcpTransport::sendMessage", errno);
        }
        r = 0;
    }
    return bytesToSend - r;
}

/**
 * Read bytes from a socket and generate exceptions for errors and
 * end-of-file.
 *
 * \param fd
 *      File descriptor for socket.
 * \param buffer
 *      Store incoming data here.
 * \param length
 *      Maximum number of bytes to read.
 * \return
 *      The number of bytes read.  The recv is done in non-blocking mode;
 *      if there are no bytes available then 0 is returned (0 does *not*
 *      mean end-of-file).
 *
 * \throw TransportException
 *      An I/O error occurred.
 * \throw TcpTransportEof
 *      The other side closed the connection.
 */

ssize_t
TcpTransport::recvCarefully(int fd, void* buffer, size_t length) {
    ssize_t actual = sys->recv(fd, buffer, length, MSG_DONTWAIT);
    if (actual > 0) {
        return actual;
    }
    if (actual == 0) {
        throw TcpTransportEof(HERE);
    }
    if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
        return 0;
    }
    throw TransportException(HERE, "I/O read error in TcpTransport", errno);
}

/**
 * Constructor for IncomingMessages.
 * \param buffer
 *      If non-NULL, specifies a buffer in which to place the body of
 *      the incoming message; the caller should ensure that the buffer
 *      is empty.  This parameter is used on servers, where the buffer
 *      is known before any part of the message has been received.
 * \param session
 *      If non-NULL, specifies a TcpSession whose findRpc method should
 *      be invoked once the header for the message has been received.
 *      FindRpc will provide a buffer to use for the body of the message.
 *      This argument is typically used on clients.
 */
TcpTransport::IncomingMessage::IncomingMessage(Buffer* buffer,
        TcpSession* session)
    : header(), headerBytesReceived(0), messageBytesReceived(0),
      messageLength(0), buffer(buffer), session(session)
{
}

/**
 * Attempt to read part or all of a message from an open socket.
 *
 * \param fd
 *      File descriptor to use for reading message info.
 * \return
 *      True means the message is complete (it's present in the
 *      buffer provided to the constructor); false means we still need
 *      more data.
 *
 * \throw TransportException
 *      An I/O error occurred.
 * \throw TcpTransportEof
 *      The other side closed the connection.
 */

bool
TcpTransport::IncomingMessage::readMessage(int fd) {
    // First make sure we have received the header (it may arrive in
    // multiple chunks).
    if (headerBytesReceived < sizeof(Header)) {
        ssize_t len = TcpTransport::recvCarefully(fd,
                reinterpret_cast<char*>(&header) + headerBytesReceived,
                sizeof(header) - headerBytesReceived);
        headerBytesReceived += downCast<uint32_t>(len);
        if (headerBytesReceived < sizeof(Header))
            return false;

        // Header is complete; check for various errors and set up for
        // reading the body.
        messageLength = header.len;
        if (header.len > MAX_RPC_LEN) {
            LOG(WARNING, "TcpTransport received oversize message (%d bytes); "
                    "discarding extra bytes", header.len);
            messageLength = MAX_RPC_LEN;
        }

        if ((buffer == NULL) && (session != NULL)) {
            buffer = session->findRpc(header);
        }
        if (buffer == NULL)
            messageLength = 0;
    }

    // We have the header; now receive the message body (it may take several
    // calls to this method before we get all of it).
    if (messageBytesReceived < messageLength) {
        void *dest;
        if (buffer->getTotalLength() == 0) {
            dest = new(buffer, APPEND) char[messageLength];
        } else {
            buffer->peek(messageBytesReceived,
                    const_cast<const void**>(&dest));
        }
        ssize_t len = TcpTransport::recvCarefully(fd, dest,
                messageLength - messageBytesReceived);
        messageBytesReceived += downCast<uint32_t>(len);
        if (messageBytesReceived < messageLength)
            return false;
    }

    // We have the header and the message body, but we may have to discard
    // extraneous bytes.
    if (messageBytesReceived < header.len) {
        char buffer[4096];
        uint32_t maxLength = header.len - messageBytesReceived;
        if (maxLength > sizeof(buffer))
            maxLength = sizeof(buffer);
        ssize_t len = TcpTransport::recvCarefully(fd, buffer, maxLength);
        messageBytesReceived += downCast<uint32_t>(len);
        if (messageBytesReceived < header.len)
            return false;
    }
    return true;
}

/**
 * Construct a TcpSession object for communication with a given server.
 *
 * \param serviceLocator
 *      Identifies the server to which RPCs on this session will be sent.
 */
TcpTransport::TcpSession::TcpSession(const ServiceLocator& serviceLocator)
        : address(serviceLocator), fd(-1), serial(1), rpcsWaitingToSend(),
          bytesLeftToSend(0), rpcsWaitingForResponse(), current(NULL),
          message(), clientIoHandler(), errorInfo()
{
    fd = sys->socket(PF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        throw TransportException(HERE,
                "TcpTransport couldn't open socket for session", errno);
    }

    int r = sys->connect(fd, &address.address, sizeof(address.address));
    if (r == -1) {
        sys->close(fd);
        fd = -1;
        throw TransportException(HERE,
                "Session connect error in TcpTransport", errno);
    }

    /// Arrange for notification whenever the server sends us data.
    Dispatch::Lock lock;
    clientIoHandler.construct(fd, this);
    message.construct(static_cast<Buffer*>(NULL), this);
}

/**
 * Destructor for TcpSession objects.
 */
TcpTransport::TcpSession::~TcpSession()
{
    close();
}

/**
 * Close the socket associated with a session.
 */
void
TcpTransport::TcpSession::close()
{
    if (fd >= 0) {
        sys->close(fd);
        fd = -1;
    }
    if (errorInfo.size() == 0) {
        errorInfo = "session closed";
    }
    while (!rpcsWaitingForResponse.empty()) {
        rpcsWaitingForResponse.front().cancel(errorInfo);
    }
    while (!rpcsWaitingToSend.empty()) {
        rpcsWaitingToSend.front().cancel(errorInfo);
    }
    if (clientIoHandler) {
        Dispatch::Lock lock;
        clientIoHandler.destroy();
    }
}

// See Transport::Session::clientSend for documentation.
TcpTransport::ClientRpc*
TcpTransport::TcpSession::clientSend(Buffer* request, Buffer* reply)
{
    if (fd == -1) {
        throw TransportException(HERE, errorInfo);
    }
    TcpClientRpc* rpc = new(reply, MISC) TcpClientRpc(this, request,
            reply, serial);
    serial++;
    if (!rpcsWaitingToSend.empty()) {
        // Can't transmit this request yet; there are already other
        // requests that haven't yet been sent.
        rpcsWaitingToSend.push_back(*rpc);
        return rpc;
    }

    // Try to transmit the request.
    bytesLeftToSend = TcpTransport::sendMessage(fd, rpc->nonce, *request, -1);
    if (bytesLeftToSend == 0) {
        // The whole request was sent immediately (this should be the
        // common case).
        rpcsWaitingForResponse.push_back(*rpc);
        rpc->sent = true;
    } else {
        rpcsWaitingToSend.push_back(*rpc);
        clientIoHandler->setEvents(Dispatch::FileEvent::READABLE |
                Dispatch::FileEvent::WRITABLE);
    }
    return rpc;
}

/**
 * This method is invoked once the header has been received for an RPC response.
 * It uses information in the header to locate the corresponding TcpClientRpc
 * object, and returns the Buffer to use for the response.
 *
 * \param header
 *      The header from the incoming RPC.
 *
 * \return
 *      If the nonce in the header refers to an active RPC, then the return
 *      value is the reply payload for that RPC.  If no matching RPC can be
 *      found (perhaps the RPC was canceled?) then NULL is returned to indicate
 *      that the input message should be dropped.
 */
Buffer*
TcpTransport::TcpSession::findRpc(Header& header) {
    foreach (TcpClientRpc& rpc, rpcsWaitingForResponse) {
        if (rpc.nonce == header.nonce) {
            current = &rpc;
            return rpc.reply;
        }
    }
    return NULL;
}

/**
 * Constructor for ClientSocketHandlers.
 *
 * \param fd
 *      File descriptor for a socket on which the the response for
 *      an RPC will arrive.
 * \param session
 *      The TcpSession that is controlling this request and its response.
 */
TcpTransport::ClientSocketHandler::ClientSocketHandler(int fd,
        TcpSession* session)
        : Dispatch::File(fd, Dispatch::FileEvent::READABLE),
        fd(fd), session(session)
{
    // Empty constructor body.
}

/**
 * This method is invoked when the socket connecting to a server becomes
 * readable or writable. This method reads or writes the socket as
 * appropriate.
 *
 * \param events
 *      Indicates whether the socket was readable, writable, or both
 *      (OR-ed combination of Dispatch::FileEvent bits).
 */
void
TcpTransport::ClientSocketHandler::handleFileEvent(int events)
{
    try {
        if (events & Dispatch::FileEvent::READABLE) {
            if (session->message->readMessage(fd)) {
                // This RPC is finished.
                if (session->current != NULL) {
                    session->current->markFinished();
                    session->rpcsWaitingForResponse.erase(
                            session->rpcsWaitingForResponse.iterator_to(
                            *session->current));
                    session->current = NULL;
                }
                session->message.construct(static_cast<Buffer*>(NULL), session);
            }
        }
        if (events & Dispatch::FileEvent::WRITABLE) {
            // We should only get here if an RPC could not be transmitted in
            // its entirety because it would have blocked on I/O.
            assert(!session->rpcsWaitingToSend.empty());
            while (true) {
                TcpClientRpc& rpc = session->rpcsWaitingToSend.front();
                session->bytesLeftToSend = TcpTransport::sendMessage(
                        session->fd, rpc.nonce, *(rpc.request),
                        session->bytesLeftToSend);
                if (session->bytesLeftToSend != 0) {
                    break;
                }
                // The current RPC is finished; start the next one, if
                // there is one.
                session->rpcsWaitingToSend.pop_front();
                session->rpcsWaitingForResponse.push_back(rpc);
                rpc.sent = true;
                session->bytesLeftToSend = -1;
                if (session->rpcsWaitingToSend.empty()) {
                    setEvents(Dispatch::FileEvent::READABLE);
                    break;
                }
            }
        }
    } catch (TcpTransportEof& e) {
        // Close the session's socket in order to prevent an infinite loop of
        // calls to this method.
        session->errorInfo = "socket closed by server";
        session->close();
    } catch (TransportException& e) {
        LOG(ERROR, "TcpTransport::ClientSocketHandler closing session "
                "socket: %s", e.message.c_str());
        session->errorInfo = e.message;
        session->close();
    }
}

// See Transport::ServerRpc::sendReply for documentation.
void
TcpTransport::TcpServerRpc::sendReply()
{
    if (!socket->rpcsWaitingToReply.empty()) {
        // Can't transmit the response yet; the socket is backed up.
        socket->rpcsWaitingToReply.push_back(*this);
        return;
    }

    // Try to transmit the response.
    socket->bytesLeftToSend = TcpTransport::sendMessage(fd,
            message.header.nonce, replyPayload, -1);
    if (socket->bytesLeftToSend > 0) {
        socket->rpcsWaitingToReply.push_back(*this);
        socket->ioHandler.setEvents(Dispatch::FileEvent::READABLE |
                Dispatch::FileEvent::WRITABLE);
        return;
    }
    // The whole response was sent immediately (this should be the
    // common case).  Delete the RPC object on the way out of this method.
    std::auto_ptr<TcpServerRpc> suicide(this);
}

// See Transport::ClientRpc::cancelCleanup for documentation.
void
TcpTransport::TcpClientRpc::cancelCleanup()
{
    if (sent) {
        session->rpcsWaitingForResponse.erase(
                session->rpcsWaitingForResponse.iterator_to(*this));
    } else {
        session->rpcsWaitingToSend.erase(
                session->rpcsWaitingToSend.iterator_to(*this));
    }
}

}  // namespace RAMCloud
