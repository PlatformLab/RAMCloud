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
 * Contains the implementation of the E1000Driver class.
 */
#include <errno.h>

#include <E1000Driver.h>
#include <NetUtil.h>

#include <string>

// TODO(aravindn): free ring buffers? buf . inuse?

namespace RAMCloud {

/**
 * The constructor for the E1000Driver class. This calls the necessary functions
 * to initialize the NIC.
 */
E1000Driver::E1000Driver() : rxRingBuffers(NULL), numRxRingBuffers(100*1024),
                             handle(NULL), device("0000:13:00.0"),
                             ringSize(256), ringBufferSize(1 * 1024 * 1024),
                             mtu(ETHERMTU), numFilters(0), noBroadcast(true),
                             sendiovLen(0) {
    uint32_t err;

    handle = e1000_open(const_cast<char*>(device.c_str()), ringSize, 128,
                        ringBufferSize);
    if (!handle) throw DriverException("Could not open device" + device);

    // e1000_set_flushcb(handle, flushCB, flushCB, 0);

    err = e1000_set_mtu(handle, mtu);
    if (err) throw DriverException("Failed to set MTU");

    err = e1000_set_rafilter(handle, numFilters, filter);
    if (err) throw DriverException("Failed to set RA filter");

    err = e1000_enable_rx(handle);
    if (err) throw DriverException("Failed to re-enable RX path");

    // done enabling rx. enabling tx now - only once
    err = e1000_enable_tx(handle);
    if (err) throw DriverException("Error enabling tx in rx.c\n");

    if (noBroadcast) e1000_set_broadcast(handle, 0);

    e1000_get_macaddr(handle, myEthAddr);

    allocRingBuffers();
    waitForLink();
    e1000_trigger_interrupt(handle, E1000_ICS_GPI_EN1);
}

E1000Driver::~E1000Driver() {
    e1000_close(handle);
}

/**
 * Allocate RingBuffers, and map each RingBuffer to one of the E1000's in memory
 * buffers.
 */
void E1000Driver::allocRingBuffers() {
  uint64_t size,  rxRingBufferLen, i;
  void *mem;

  rxRingBuffers = reinterpret_cast<RingBuffer *>(
      malloc(sizeof(RingBuffer) * numRxRingBuffers));
  assert(rxRingBuffers);

  mem = e1000_get_buffer(handle, &size);
  rxRingBufferLen = e1000_get_rxbuflen(handle);

  memset(mem, 0, size);
  /*
  printf("Number of rx descriptors: %lu\n", e1000_get_rxroom(h));
  printf("Available buffer memory %lu rxbuflen %lu\n", size, buflen);
  */
  for (i = 0; i < numRxRingBuffers && size > rxRingBufferLen; i++) {
    rxRingBuffers[i].data = mem;
    rxRingBuffers[i].size = rxRingBufferLen;

    size -= rxRingBufferLen;
    mem = (reinterpret_cast<uint8_t *>(mem) + rxRingBufferLen);
  }

  numRxRingBuffers = i;

  // printf("Using %u buffers of %lu size\n", nbuffers, buflen);
}

/**
 * Find and return an available RingBuffer.
 *
 * \return  A pointer to the first available RingBuffer.
 */
E1000Driver::RingBuffer* E1000Driver::getRingBuffer() {
    for (uint64_t i = 0; i < numRxRingBuffers; ++i)
        if (!rxRingBuffers[i].inUse)
            return &rxRingBuffers[i];

    return NULL;
}

/**
 * Waits for the NIC's link to become active.
 */
void E1000Driver::waitForLink() {
    struct e1000_link link;
    uint64_t flush, err;

    // printf("Waiting for link...\n");

    while (1) {
        e1000_enable_interrupts(handle, ~0);

        err = e1000_check_link(handle, &link, reinterpret_cast<int *>(&flush));
        if (err) throw DriverException("Failed to check link state");

        // if (terminated) exit(0);

        if (link.speed)
            break;
        else
            sleep(1);
    }

    // printf("Link is up!\n");
}

/**
 * Checks whether the NIC's link is active. If not, it waits until it becomes
 * active.
 */
void E1000Driver::checkLink() {
    struct e1000_link link;
    uint64_t flush, err;

    err = e1000_check_link(handle, &link, reinterpret_cast<int *>(&flush));
    if (err) throw E1000Driver::DriverException("Failed to get link status");

    if (!link.speed) {
        // printf("Link is down\n");

        if (flush) {
            // printf("Flush FIFOs\n");
            e1000_reset(handle);
            e1000_flush_rx(handle);
        }

        waitForLink();
    }
}

// TODO(aravindn): Unused as of now.
void E1000Driver::flushCB(uint32_t udata, uint32_t cdata) {
    RingBuffer* ringBuf = reinterpret_cast<RingBuffer *>(cdata);
    // uint8_t *ptr;
    // uint64_t len;
    // TODO(aravindn): What?
    // printf("Flushed: cdata 0x%lx\n", cdata);
    ringBuf->inUse = false;
}


/**
 * Tells the NIC to start waiting for a frame to arrive on the wire.
 */
void E1000Driver::doRecv() {
    RingBuffer* ringBuf;
    uint64_t err;

    if ((ringBuf = getRingBuffer())) {
        // TODO(aravindn): What do we do if ringBuf is NULL?
        ringBuf->inUse = true;
        err = e1000_recv(handle, ringBuf->data, ringBuf->size,
                         (unsigned long) ringBuf);  // NOLINT
        if (err) {
            if (errno != ENOSPC) throw DriverException("Receive failed\n");
            ringBuf->inUse = false;
        }
    }
}

/**
 * Process a newly received ethernet frame represented by the supplied
 * RingBuffer. Inserts a newly received ethernet frame into the supplied Buffer.
 *
 * \param[in]  cdata    A pointer to the RingBuffer representing an ethernet
 *                      frame.
 * \param[in]  status   The status of the RingBuffer represented by cdata.
 * \param[in]  payload  The payload into which we insert the newly received
 *                      ethernet frame
 */
void E1000Driver::rxComplete(uint64_t cdata, int64_t status, Buffer* payload) {
    RingBuffer* ringBuf = reinterpret_cast<RingBuffer*>(cdata);
    ringBuf->inUse = false;

    uint8_t *ptr = reinterpret_cast<uint8_t *>(cdata);
    uint32_t len = (uint32_t) status;

    payload->append(ptr, len);
}

/**
 * Waits for the NIC to DMA an ethernet frame into memory, and populates the
 * supplied buffer with this frame.
 *
 * \param[in]  payload  The Buffer to which we insert the newly received frame.
 */
bool E1000Driver::doRecvCompletion(Buffer* payload) {
    bool retVal = false;
    uint64_t cdata;
    while (true) {
        int64_t len = e1000_complete_rx(handle, &cdata);
        if (!len) break;
        rxComplete(cdata, len, payload);
        retVal = true;
    }
    // TODO(aravindn): restructure
    return retVal;
}
/**
 * Tells the NIC to send the an ethernet frame represented by the class' iovec
 * variable out on the wire.
 */
void E1000Driver::doSend() {
    int err;

    // TOOD(aravindn): Optimize the case where the iovec only contains one
    // chunk.

    if (sendiovLen == 1) {
        err = e1000_send(handle, sendiov[0].iov_base, sendiov[0].iov_len, 0);
        if (err) throw DriverException("Send failed\n");
    } else {
        err = e1000_sendv(handle, sendiov, sendiovLen, 0);
        if (err) throw DriverException("IOV Send failed\n");
    }
}

/**
 * Wait for the NIC to finish sending a frame.
 */
void E1000Driver::doSendCompletion() {
    uint64_t cdata;

    while (true) {
        int64_t status = e1000_complete_tx(handle, &cdata);
        if (!status)
            break;
    }
}

/**
 * Puts the given payload on the wire. Doesn't add any extra headers. All
 * required headers should have been added by users of this function.
 *
 * \param[in]  payload  The Buffer representing the bytes to be put on the
 *                      wire.
 */
void E1000Driver::send(Buffer* payload) {
    // This function constructs an iovec from the given Buffer, and calls
    // doSend. The iovec is a class variable so that it is not allocated again
    // on every call to this function.

    uint32_t offset = 0;
    uint32_t byte = 0;
    int i = 0;
    for ( ; offset < payload->totalLength(); ++i) {
        byte = payload->peek(offset, &(sendiov[i].iov_base));
        offset += byte;
    }
    sendiovLen = i;
    // Not passing iov eto send because this is in the fast path and we want to
    // amke it as fast asp ossible
    doSend();
    doSendCompletion();
}

/**
 * Reads an ethernet frame from the NIC and adds it to the supplied Buffer.
 *
 * \param[in]  payload  A pointer to the Buffer in which we insert the new
 *                      frame.
 */
void E1000Driver::recv(Buffer* payload) {
    doRecv();
    doRecvCompletion(payload);
}

}  // namespace RAMCloud
