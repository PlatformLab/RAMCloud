/* Copyright (c) 2010-2016 Stanford University
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
#include "Cycles.h"
#include "ShortMacros.h"
#include "UdpDriver.h"
#include "ServiceLocator.h"
#include "TimeTrace.h"

namespace RAMCloud {

/**
 * Default object used to make system calls.
 */
static Syscall defaultSyscall;

/**
 * Used by this class to make all system calls.  In normal production
 * use it points to defaultSyscall; for testing it points to a mock
 * object.
 */
Syscall* UdpDriver::sys = &defaultSyscall;

/**
 * Construct a UdpDriver.
 *
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param localServiceLocator
 *      Specifies a particular socket on which this driver will listen
 *      for incoming packets. Must include "host" and "port" options
 *      identifying the desired socket.  If NULL then a port will be
 *      chosen by system software. Typically the socket is specified
 *      explicitly for server-side drivers but not for client-side
 *      drivers.
 */
UdpDriver::UdpDriver(Context* context,
        const ServiceLocator* localServiceLocator)
    : context(context)
    , socketFd(-1)
    , messageHeaders()
    , buffers()
    , packetBufPool()
    , packetBufsUtilized(0)
    , locatorString()
    , bandwidthGbps(10)                   // Default bandwidth = 10 gbs
    , queueEstimator(0)
    , maxTransmitQueueSize(0)
{
    if (localServiceLocator != NULL) {
        locatorString = localServiceLocator->getOriginalString();
        try {
            bandwidthGbps = localServiceLocator->getOption<int>("gbs");
        } catch (ServiceLocator::NoSuchKeyException& e) {}
    }
    queueEstimator.setBandwidth(1000*bandwidthGbps);
    maxTransmitQueueSize = (uint32_t) (static_cast<double>(bandwidthGbps)
            * MAX_DRAIN_TIME / 8.0);
    uint32_t maxPacketSize = getMaxPacketSize();
    if (maxTransmitQueueSize < 2*maxPacketSize) {
        // Make sure that we advertise enough space in the transmit queue to
        // prepare the next packet while the current one is transmitting.
        maxTransmitQueueSize = 2*maxPacketSize;
    }
    LOG(NOTICE, "UdpDriver bandwidth: %d Gbits/sec, maxTransmitQueueSize: "
            "%u bytes", bandwidthGbps, maxTransmitQueueSize);

    for (int i = 0; i < MAX_PACKETS_AT_ONCE; i++) {
        buffers[i] = NULL;
    }

    int fd = sys->socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1) {
        throw DriverException(HERE, "UdpDriver couldn't create socket",
                              errno);
    }

    if (localServiceLocator != NULL) {
        IpAddress ipAddress(localServiceLocator);
        int r = sys->bind(fd, &ipAddress.address, sizeof(ipAddress.address));
        if (r == -1) {
            int e = errno;
            sys->close(fd);
            throw DriverException(HERE,
                    format("UdpDriver couldn't bind to locator '%s'",
                    localServiceLocator->getOriginalString().c_str()), e);
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
    for (int i = 0; i < MAX_PACKETS_AT_ONCE; i++) {
        if (buffers[i] != NULL) {
            release(buffers[i]->payload);
            buffers[i] = NULL;
        }
    }
    if (packetBufsUtilized != 0)
        LOG(ERROR, "UdpDriver deleted with %d packets still in use",
            packetBufsUtilized);
    close();
}

/**
 * Shuts down this driver: closes the socket, turns off event handlers, etc.
 */
void
UdpDriver::close()
{
    if (socketFd != -1) {
        sys->close(socketFd);
        socketFd = -1;
    }
}

// See docs in Driver class.
uint32_t
UdpDriver::getMaxPacketSize()
{
    return MAX_PAYLOAD_SIZE;
}

// See docs in Driver class.
int
UdpDriver::getTransmitQueueSpace(uint64_t currentTime)
{
    return maxTransmitQueueSize - queueEstimator.getQueueSize(currentTime);
}

// See docs in Driver class.
void
UdpDriver::receivePackets(int maxPackets,
            std::vector<Received>* receivedPackets)
{
    if (maxPackets > MAX_PACKETS_AT_ONCE) {
        maxPackets = MAX_PACKETS_AT_ONCE;
    }

    // Initialize the arguments that will be passed to the kernel call.
    // Typically, some number of the initial buffers will be invalid
    // because packets were received in them during the last call to
    // this method.
    for (int i = 0; i < MAX_PACKETS_AT_ONCE; i++) {
        if (buffers[i] != NULL) {
            break;
        }
        struct mmsghdr* header = &messageHeaders[i];
        PacketBuf* buffer = packetBufPool.construct();
        packetBufsUtilized++;
        buffers[i] = buffer;
        header->msg_hdr.msg_name = &buffer->ipAddress.address;
        header->msg_hdr.msg_namelen = sizeof(buffer->ipAddress.address);
        header->msg_hdr.msg_iov = &buffer->iovec;
        header->msg_hdr.msg_iovlen = 1;
        header->msg_hdr.msg_control = NULL;
        header->msg_hdr.msg_controllen = 0;
        header->msg_hdr.msg_flags = 0;
    }
    ssize_t numPackets = sys->recvmmsg(socketFd, messageHeaders,
            maxPackets, MSG_DONTWAIT, NULL);
    if (numPackets <= 0) {
        if ((numPackets < 0) && (errno != EAGAIN) && (errno != EWOULDBLOCK)) {
            LOG(WARNING, "UdpDriver error receiving from socket: %s",
                    strerror(errno));
        }
        return;
    }
    for (int i = 0; i < numPackets; i++) {
        struct mmsghdr* header = &messageHeaders[i];
        PacketBuf* buffer = buffers[i];
        receivedPackets->emplace_back(&buffer->ipAddress, this,
                header->msg_len, buffer->payload);
        buffers[i] = NULL;
    }
}

// See docs in Driver class.
void
UdpDriver::release(char *payload)
{
    // Must sync with the dispatch thread, since this method could potentially
    // be invoked in a worker.
    Dispatch::Lock _(context->dispatch);

    // Note: the payload is actually contained in a PacketBuf structure,
    // which we return to a pool for reuse later.
    packetBufsUtilized--;
    assert(packetBufsUtilized >= 0);
    packetBufPool.destroy(
        reinterpret_cast<PacketBuf*>(payload - OFFSET_OF(PacketBuf, payload)));
}

// See docs in Driver class.
void
UdpDriver::sendPacket(const Address *addr,
                      const void *header,
                      uint32_t headerLen,
                      Buffer::Iterator *payload)
{
    if (socketFd == -1)
        return;
    uint32_t totalLength = headerLen +
                           (payload ? payload->size() : 0);
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

    ssize_t r = sys->sendmsg(socketFd, &msg, 0);
    if (r == -1) {
        LOG(WARNING, "UdpDriver error sending to socket: %s", strerror(errno));
        return;
    }
    queueEstimator.packetQueued(totalLength, Cycles::rdtsc());
    assert(static_cast<size_t>(r) == totalLength);
}

// See docs in Driver class.
string
UdpDriver::getServiceLocator()
{
    return locatorString;
}

} // namespace RAMCloud
