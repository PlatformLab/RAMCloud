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

#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "Common.h"
#include "UDPDriver.h"

namespace RAMCloud {

/**
 * Construct a UDPDriver that is unbound to any particular UDP address.
 *
 * For use by clients.
 */
UDPDriver::UDPDriver()
    : socketFd(-1), packetBufsUtilized(0)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1)
        throw UnrecoverableDriverException(errno);
    socketFd = fd;
}

/**
 * Construct a UDPDriver that is bound to a particular UDP address.
 *
 * For use by servers.
 *
 * \param addr
 *      The address to bind to.
 * \param addrlen
 *      The length of addr.
 */
UDPDriver::UDPDriver(const sockaddr *addr, socklen_t addrlen)
    : socketFd(-1), packetBufsUtilized(0)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1) {
        LOG(ERROR, "Couldn't create socket");
        throw UnrecoverableDriverException(errno);
    }

    int r = bind(fd, addr, addrlen);
    if (r == -1) {
        int e = errno;
        close(fd);
        LOG(ERROR, "Couldn't bind socket");
        throw UnrecoverableDriverException(e);
    }

    socketFd = fd;
}

/// Close the UDP socket.
UDPDriver::~UDPDriver()
{
    if (packetBufsUtilized != 0)
        LOG(WARNING, "packetBufsUtilized: %d",
            packetBufsUtilized);
    close(socketFd);
}

/// The maximum number bytes we can stuff in a UDP packet payload.
uint32_t
UDPDriver::getMaxPayloadSize()
{
    return MAX_PAYLOAD_SIZE;
}

// See Driver::release().
void
UDPDriver::release(char *payload, uint32_t len)
{
    packetBufsUtilized--;
    assert(packetBufsUtilized >= 0);
    delete[] payload;
}

// See Driver::sendPacket().
// UDPDriver::sendPacket currently guarantees that the caller is free to
// discard or reuse the memory associated with payload and header once
// on return from this method.
void
UDPDriver::sendPacket(const sockaddr *addr,
                      socklen_t addrlen,
                      void *header,
                      uint32_t headerLen,
                      Buffer::Iterator *payload)
{
    uint32_t totalLength = headerLen +
                           (payload ? payload->getTotalLength() : 0);
    assert(totalLength <= getMaxPayloadSize());

    // one for header, the rest for payload
    uint32_t iovecs = 1 + (payload ? payload->getNumberChunks() : 0);

    struct iovec iov[iovecs];
    iov[0].iov_base = header;
    iov[0].iov_len = headerLen;

    uint32_t i = 1;
    while (payload && !payload->isDone()) {
        iov[i].iov_base = const_cast<void*>(payload->getData());
        iov[i].iov_len = payload->getLength();
        ++i;
        payload->next();
    }

    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = iov;
    msg.msg_iovlen = iovecs;

    msg.msg_name = const_cast<sockaddr *>(addr);
    msg.msg_namelen = addrlen;

    ssize_t r = sendmsg(socketFd, &msg, 0);
    if (r == -1) {
        int e = errno;
        close(socketFd);
        socketFd = -1;
        LOG(ERROR, "Couldn't sendmsg");
        throw UnrecoverableDriverException(e);
    }
    assert(static_cast<size_t>(r) == totalLength);
}

// See Driver::tryRecvPacket().
bool
UDPDriver::tryRecvPacket(Received *received)
{
    char *payload = new char[getMaxPayloadSize()];

    received->addrlen = sizeof(received->addr);
    int r = recvfrom(socketFd, payload, getMaxPayloadSize(),
                     MSG_DONTWAIT,
                     &received->addr, &received->addrlen);
    if (r == -1) {
        delete[] payload;
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return false;
        // TODO(stutsman) We could probably recover from a lot of errors here.
        throw UnrecoverableDriverException(errno);
    }
    received->len = r;

    packetBufsUtilized++;
    received->payload = payload;
    received->driver = this;

    return true;
}

} // namespace RAMCloud
