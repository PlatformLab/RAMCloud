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
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "Common.h"
#include "UdpDriver.h"
#include "ServiceLocator.h"

namespace RAMCloud {

int UdpDriver::packetBufsFreed = 0;

/**
 * Construct a UdpDriver.
 *
 * \param localServiceLocator
 *      Specifies a particular socket on which this driver will listen
 *      for incoming packets. Must include "host" and "port" options
 *      identifying the desired socket.  If NULL then a port will be
 *      chosen by system software. Typically the socket is specified
 *      explicitly for server-side drivers but not for client-side
 *      drivers.
 */
UdpDriver::UdpDriver(const ServiceLocator* localServiceLocator)
    : socketFd(-1), freePacketBufs(), packetBufsUtilized(0)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1) {
        throw UnrecoverableDriverException(errno);
    }

    if (localServiceLocator != NULL) {
        IpAddress ipAddress(*localServiceLocator);
        int r = bind(fd, &ipAddress.address, sizeof(ipAddress.address));
        if (r == -1) {
            int e = errno;
            close(fd);
            throw UnrecoverableDriverException(e);
        }
    }

    socketFd = fd;
}

/**
 * Destroy a UdpDriver. The socket associated with this driver is
 * closed.
 */
UdpDriver::~UdpDriver()
{
    if (packetBufsUtilized != 0)
        LOG(WARNING, "packetBufsUtilized: %d",
            packetBufsUtilized);
    for (int i = freePacketBufs.size()-1; i >= 0; i--) {
        free(freePacketBufs[i]);
        packetBufsFreed++;
    }
    close(socketFd);
}

// See docs in Driver class.
uint32_t
UdpDriver::getMaxPacketSize()
{
    return MAX_PAYLOAD_SIZE;
}

// See docs in Driver class.
void
UdpDriver::release(char *payload, uint32_t len)
{
    packetBufsUtilized--;
    assert(packetBufsUtilized >= 0);
    freePacketBufs.push_back(reinterpret_cast<PacketBuf*>
            (payload - sizeof(PacketBuf)));
}

// See Driver::sendPacket().
// UdpDriver::sendPacket currently guarantees that the caller is free to
// discard or reuse the memory associated with payload and header once
// this method returns.
void
UdpDriver::sendPacket(const Address *addr,
                      const void *header,
                      uint32_t headerLen,
                      Buffer::Iterator *payload)
{
    uint32_t totalLength = headerLen +
                           (payload ? payload->getTotalLength() : 0);
    assert(totalLength <= MAX_PAYLOAD_SIZE);

    // one for header, the rest for payload
    uint32_t iovecs = 1 + (payload ? payload->getNumberChunks() : 0);

    struct iovec iov[iovecs];
    iov[0].iov_base = const_cast<void*>(header);
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

    const sockaddr* a = &(static_cast<const IpAddress*>(addr)->address);
    msg.msg_name = const_cast<sockaddr *>(a);
    msg.msg_namelen = sizeof(*a);

    ssize_t r = sendmsg(socketFd, &msg, 0);
    if (r == -1) {
        int e = errno;
        close(socketFd);
        socketFd = -1;
        throw UnrecoverableDriverException(e);
    }
    assert(static_cast<size_t>(r) == totalLength);
}

// See docs in Driver class.
bool
UdpDriver::tryRecvPacket(Received *received)
{
    PacketBuf* buffer;
    if (freePacketBufs.size() > 0) {
        buffer = freePacketBufs.back();
        freePacketBufs.pop_back();
    } else {
        buffer = new(malloc(sizeof(PacketBuf) + MAX_PAYLOAD_SIZE))
                PacketBuf();
    }
    socklen_t addrlen = sizeof(&buffer->ipAddress.address);
    int r = recvfrom(socketFd, buffer->payload, MAX_PAYLOAD_SIZE,
                     MSG_DONTWAIT,
                     &buffer->ipAddress.address, &addrlen);
    if (r == -1) {
        free(buffer);
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return false;
        // TODO(stutsman) We could probably recover from a lot of errors here.
        throw UnrecoverableDriverException(errno);
    }
    received->len = r;

    packetBufsUtilized++;
    received->payload = buffer->payload;
    received->sender = &buffer->ipAddress;
    received->driver = this;

    return true;
}

} // namespace RAMCloud
