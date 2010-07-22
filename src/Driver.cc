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
 * Implementation for the Driver classes.
 */

#include <Driver.h>

#include <errno.h>

namespace RAMCloud {

Driver::~Driver()
{
}

void
Driver::release(Received *received)
{
    delete[] received->payload;
}


void
UDPDriver::release(Received *received)
{
    packetBufsUtilized--;
}

UDPDriver::UDPDriver(const sockaddr *addr, socklen_t addrlen)
    : socketFd(-1), packetBufsUtilized(0)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1)
        throw UnrecoverableDriverException(errno);

    int r = bind(fd, addr, addrlen);
    if (r == -1) {
        int e = errno;
        close(fd);
        throw UnrecoverableDriverException(e);
    }

    socketFd = fd;
}

UDPDriver::~UDPDriver()
{
    if (packetBufsUtilized != 0)
        fprintf(stderr, "WARNING: packetBufsUtilized: %d\n",
                packetBufsUtilized);
    close(socketFd);
}

uint32_t
UDPDriver::getMaxPayloadSize()
{
    return MAX_PAYLOAD_SIZE;
}

void
UDPDriver::sendPacket(const sockaddr *addr,
                      socklen_t addrlen,
                      Buffer::Iterator *payload)
{
    assert(payload->getTotalLength() <= getMaxPayloadSize());

    uint32_t iovecs = payload->getNumberChunks();
    struct iovec iov[iovecs];

    int i = 0;
    while (!payload->isDone()) {
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
        throw UnrecoverableDriverException(e);
    }
    assert(static_cast<size_t>(r) == payload->getTotalLength());
}

bool
UDPDriver::tryRecvPacket(Received *received)
{
    char *payload = new char[getMaxPayloadSize()];

    received->len = recvfrom(socketFd, payload, getMaxPayloadSize(),
                             MSG_DONTWAIT,
                             &received->addr, &received->addrlen);
    if (received->len == -1) {
        delete[] payload;
        return false;
    }

    packetBufsUtilized++;
    received->payload = payload;
    received->driver = this;

    return true;
}

} // namespace RAMCloud
