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
#include "UDPDriver.h"
#include "ServiceLocator.h"

namespace RAMCloud {

/**
 * Construct a UDPDriver that is bound to a particular UDP address.
 *
 * For use by servers.
 */
UDPDriver::UDPDriver(const ServiceLocator* localServiceLocator)
    : socketFd(-1), packetBufsUtilized(0)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1) {
        LOG(ERROR, "Couldn't create socket");
        throw UnrecoverableDriverException(errno);
    }

    if (localServiceLocator != NULL) {
        const char* ip = localServiceLocator->getOption<const char*>("ip");
        uint16_t port = localServiceLocator->getOption<uint16_t>("port");

        sockaddr addrStorage;
        sockaddr_in *addr = const_cast<sockaddr_in*>(
            reinterpret_cast<const sockaddr_in*>(&addrStorage));
        addr->sin_family = AF_INET;
        addr->sin_port = htons(port);
        if (inet_aton(ip, &addr->sin_addr) == 0)
            throw UnrecoverableDriverException("inet_aton failed");

        int r = bind(fd, &addrStorage, sizeof(*addr));
        if (r == -1) {
            int e = errno;
            close(fd);
            LOG(ERROR, "Couldn't bind socket");
            throw UnrecoverableDriverException(e);
        }
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
    AddressPayload* addressPayload =
        reinterpret_cast<AddressPayload*>(payload - sizeof(AddressPayload));
    free(addressPayload);
}

// See Driver::sendPacket().
// UDPDriver::sendPacket currently guarantees that the caller is free to
// discard or reuse the memory associated with payload and header once
// on return from this method.
void
UDPDriver::sendPacket(const Address *addr,
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

    const sockaddr* a = &(static_cast<const UDPAddress*>(addr)->address);
    msg.msg_name = const_cast<sockaddr *>(a);
    msg.msg_namelen = sizeof(*a);

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
    void* storage = malloc(sizeof(AddressPayload) + getMaxPayloadSize());
    AddressPayload* addressPayload = new(storage) AddressPayload();
    socklen_t addrlen = sizeof(&addressPayload->udpAddress.address);
    int r = recvfrom(socketFd, addressPayload->payload, getMaxPayloadSize(),
                     MSG_DONTWAIT,
                     &addressPayload->udpAddress.address, &addrlen);
    if (r == -1) {
        free(addressPayload);
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return false;
        // TODO(stutsman) We could probably recover from a lot of errors here.
        throw UnrecoverableDriverException(errno);
    }
    received->len = r;

    packetBufsUtilized++;
    received->payload = addressPayload->payload;
    received->sender = &addressPayload->udpAddress;
    received->driver = this;

    return true;
}

UDPDriver::UDPAddress::UDPAddress(const ServiceLocator* serviceLocator)
    : address()
{
    sockaddr_in *addr = const_cast<sockaddr_in*>(
        reinterpret_cast<const sockaddr_in*>(&address));
    addr->sin_family = AF_INET;
    addr->sin_port = htons(serviceLocator->getOption<uint16_t>("port"));
    if (inet_aton(serviceLocator->getOption<const char*>("ip"),
                  &addr->sin_addr) == 0) {
        throw Exception("inet_aton failed");
    }
}

} // namespace RAMCloud
