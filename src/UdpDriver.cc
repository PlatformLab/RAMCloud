/* Copyright (c) 2010-2017 Stanford University
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
#include "Fence.h"
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
    : Driver(context)
    , socketFd(-1)
    , packetBatches()
    , currentBatch(0)
    , packetBufPool()
    , mutex("UdpDriver::packetBufPool")
    , locatorString("udp:")
    , bandwidthGbps(10)                   // Default bandwidth = 10 gbs
    , readerThread()
    , readerThreadExit(false)
{
    if (localServiceLocator != NULL) {
        locatorString = localServiceLocator->getDriverLocatorString();
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
                    locatorString.c_str()), e);
        }
    } else {
        struct sockaddr_in address;
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_ANY);
        address.sin_port = HTONS(0);
        int r = sys->bind(fd, reinterpret_cast<struct sockaddr*>(&address),
                sizeof(address));
        if (r == -1) {
            int e = errno;
            sys->close(fd);
            throw DriverException(HERE,
                    "UdpDriver couldn't bind client socket", e);
        }
        LOG(NOTICE, "UdpDriver using port %d", NTOHS(address.sin_port));
    }

    socketFd = fd;
    readerThread.construct(readerThreadMain, this);

    LOG(NOTICE, "Locator for UdpDriver: %s", locatorString.c_str());
}

/**
 * Destroy a UdpDriver. The socket associated with this driver is
 * closed.
 */
UdpDriver::~UdpDriver()
{
    close();
    for (int batch = 0; batch < 2; batch++) {
        for (int i = 0; i < PacketBatch::MAX_PACKETS; i++) {
            if (packetBatches[batch].buffers[i] != NULL) {
                // No need to sync before acessing packetBufPool since we have
                // joined readerThread in close().
                char* payload = packetBatches[batch].buffers[i]->payload;
                packetBufPool.destroy(reinterpret_cast<PacketBuf*>(
                        payload - OFFSET_OF(PacketBuf, payload)));
            }
        }
    }
}

/**
 * Shuts down this driver: closes the socket, stops the reader thread, etc.
 */
void
UdpDriver::close()
{
    if (readerThread) {
        stopReaderThread();
        readerThread->join();
        readerThread.destroy();
    }
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
void
UdpDriver::receivePackets(uint32_t maxPackets,
            std::vector<Received>* receivedPackets)
{
    PacketBatch* batch = &packetBatches[currentBatch];
    int available = batch->packetsAvailable.load();
    if (available == 0) {
        return;
    }
    Fence::enter();
    int limit = batch->packetsRemoved + maxPackets;
    if (limit > available) {
        limit = available;
    }

    for (int i = batch->packetsRemoved; i < limit; i++) {
        struct mmsghdr* header = &batch->messageHeaders[i];
        PacketBuf* buffer = batch->buffers[i];
        receivedPackets->emplace_back(buffer->sender.get(), this,
                header->msg_len, buffer->payload);
        batch->buffers[i] = NULL;
    }
    if (limit < available) {
        batch->packetsRemoved = limit;
    } else {
        // We're done with this batch; mark it as available to the reader
        // thread and switch to the other batch.
        batch->packetsRemoved = 0;
        Fence::leave();
        batch->packetsAvailable = 0;
        currentBatch ^= 1;
    }
}

// See docs in Driver class.
void
UdpDriver::release()
{
    SpinLock::Guard guard(mutex);

    while (!packetsToRelease.empty()) {
        // Note: the payload is actually contained in a PacketBuf structure,
        // which we return to a pool for reuse later.
        char* payload = packetsToRelease.back();
        packetsToRelease.pop_back();
        packetBufPool.destroy(reinterpret_cast<PacketBuf*>(
                payload - OFFSET_OF(PacketBuf, payload)));
    }
}

// See docs in Driver class.
void
UdpDriver::sendPacket(const Address* addr,
                      const void* header,
                      uint32_t headerLen,
                      Buffer::Iterator* payload,
                      int priority,
                      TransmitQueueState* txQueueState)
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
    lastTransmitTime = Cycles::rdtsc();
    queueEstimator.packetQueued(totalLength, lastTransmitTime, txQueueState);
    assert(static_cast<size_t>(r) == totalLength);
}

/**
 * Notify the reader thread that it should exit. Don't actually wait for the
 * thread to return here, though.
 */
void
UdpDriver::stopReaderThread()
{
    readerThreadExit = true;

    struct sockaddr socketAddress;
    socklen_t addressLength = sizeof(socketAddress);
    if (sys->getsockname(socketFd, &socketAddress, &addressLength) < 0) {
        RAMCLOUD_LOG(ERROR, "getsockname returned error: %s",
                strerror(errno));
        return;
    }
    IpAddress address(&socketAddress);
    sendPacket(&address, "Please exit now", 15, NULL);
}

// See docs in Driver class.
string
UdpDriver::getServiceLocator()
{
    return locatorString;
}

/**
 * The main program for a thread that runs in the background, issuing
 * blocking kernel calls to wait for incoming packets.
 * \param driver
 *      The UdpDriver on behalf of which this thread is operating.
 */
void
UdpDriver::readerThreadMain(UdpDriver* driver)
{
    // Index within driver->packetBatches where we will read the next
    // batch of packets.
    int currentBatch = 0;

    // Each iteration through the following loop makes one kernel call
    // to receive packets.
    while (1) {
        PacketBatch* batch = &driver->packetBatches[currentBatch];

        // Make sure that the dispatch thread isn't still working on the
        // current batch.
        while (batch->packetsAvailable != 0) {
            // If we get here, it means that the dispatch thread hasn't
            // yet handed off all the packets in a previous batch. This
            // shouldn't happen very often, so, for simplicity, we just
            // use a polling approach to wait (sleep, in case there's
            // other useful work that can be done with this core).
            RAMCLOUD_CLOG(NOTICE, "dispatch thread not keeping up with "
                    "UdpDriver packet reader");
            usleep(10);
            if (driver->readerThreadExit) {
                TEST_LOG("reader thread exited");
                return;
            }
        }
        Fence::enter();

        // Initialize the arguments that will be passed to the kernel call.
        // Typically, some number of the initial buffers will be invalid
        // because packets were received if
        {
            SpinLock::Guard guard(driver->mutex);
            for (int i = 0; i < PacketBatch::MAX_PACKETS; i++) {
                if (batch->buffers[i] != NULL) {
                    break;
                }
                struct mmsghdr* header = &batch->messageHeaders[i];
                PacketBuf* buffer = driver->packetBufPool.construct();
                buffer->sender.construct();
                batch->buffers[i] = buffer;
                header->msg_hdr.msg_name = &buffer->sender->address;
                header->msg_hdr.msg_namelen = sizeof(buffer->sender->address);
                header->msg_hdr.msg_iov = &buffer->iovec;
                header->msg_hdr.msg_iovlen = 1;
                header->msg_hdr.msg_control = NULL;
                header->msg_hdr.msg_controllen = 0;
                header->msg_hdr.msg_flags = 0;
            }
        }

        // Wait for one or more incoming packets
        ssize_t numPackets = sys->recvmmsg(driver->socketFd,
                batch->messageHeaders, PacketBatch::MAX_PACKETS,
                MSG_WAITFORONE, NULL);
        if (driver->readerThreadExit) {
            TEST_LOG("reader thread exited");
            return;
        }
        if (numPackets < 0) {
            if ((errno != EAGAIN) && (errno != EWOULDBLOCK)) {
                LOG(WARNING, "UdpDriver error receiving from socket: %s",
                        strerror(errno));
            }
            continue;
        }

        // Hand this batch off to the dispatch thread.
        Fence::leave();
        batch->packetsAvailable = downCast<int>(numPackets);
        currentBatch ^= 1;
    }
}

} // namespace RAMCloud
