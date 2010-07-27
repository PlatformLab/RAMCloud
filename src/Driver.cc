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
Driver::release(char *payload, uint32_t len)
{
    delete[] payload;
}


void
UDPDriver::release(char *payload, uint32_t len)
{
    packetBufsUtilized--;
}

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
    while (i < iovecs) {
        iov[i].iov_base = 0;
        iov[i].iov_len = 0;
        i++;
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
        return false;
    }
    received->len = r;

    packetBufsUtilized++;
    received->payload = payload;
    received->driver = this;

    return true;
}

} // namespace RAMCloud
