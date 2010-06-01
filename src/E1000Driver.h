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
 * Header file for the E1000's driver class.
 */

#ifndef RAMCLOUD_E1000DRIVER_H
#define RAMCLOUD_E1000DRIVER_H

#include <libe1000/e1000_defs.h>
#include <libe1000/e1000_hw.h>
#include <libe1000/e1000_lib.h>

#include <Buffer.h>
#include <Common.h>

#include <string>

namespace RAMCloud {

// TODO(aravindn): Move into separate file, in case we need it for different
// drivers?
/**
 * \class Driver
 * A generic Driver class, to be used as a model in case we need to write
 * drivers for other NICs in the future.
*/
class Driver {
  public:
    virtual void send(Buffer* payload) = 0;

    virtual void recv(Buffer* payload) = 0;

    Driver() { }

    virtual ~Driver() { }

    // TODO(aravindn): Copied from existing Exceptions. Must change.
    struct DriverException {
        explicit DriverException() : message(""), errNo(0) { }
        explicit DriverException(std::string msg) : message(msg), errNo(0) { }
        explicit DriverException(int errNo) : message(""), errNo(errNo) {
            message = strerror(errNo);
        }
        explicit DriverException(std::string msg, int errNo) :
                message(msg), errNo(errNo) { }
        DriverException(const DriverException &e) :
                message(e.message), errNo(e.errNo) { }
        DriverException &operator==(const DriverException &e) {
            if (&e == this)
                return *this;
            message = e.message;
            errNo = e.errNo;
            return *this;
        }
        virtual ~DriverException() { }
        std::string message;
        int errNo;
    };
};

/**
 * \class E1000Driver
 *
 * A driver for the E1000 class NIC. Libereal amounts of code borrowed from the
 * open source libe1000 package.
 */
class E1000Driver : public Driver {
  public:
    void send(Buffer* payload);

    void recv(Buffer* payload);

    E1000Driver();
    ~E1000Driver();

  private:
    /**
     * A RingBuffer contains information about one of the buffers used by the
     *  NIC to send and receive packets.
     */
    struct RingBuffer {
        void *data;     // A pointer to the memory associated with this
                        // RingBuffer.
        uint64_t size;  // The size of the memory region associated with this
                        // RingBuffer.
        bool inUse;     // Whether or not the memeory region associated with
                        // this RingBuffer contains valid data.
    };

    void allocRingBuffers();
    RingBuffer* getRingBuffer();

    void waitForLink();
    void checkLink();

    void flushCB(uint32_t udata, uint32_t cdata);

    void doRecv();
    void rxComplete(uint64_t cdata, int64_t status, Buffer* payload);
    bool doRecvCompletion(Buffer* payload);

    void doSend();
    void doSendCompletion();

    RingBuffer* rxRingBuffers;         // The receive side RingBuffers.
    uint64_t numRxRingBuffers;         // The number of receive side
                                       // RingBuffers.
    struct e1000_handle *handle;       // The handle representing the NIC.
    std::string device;                // The PCI id of the NIC. (Obtained from
                                       // lspci). TODO(aravindn): Read it from
                                       // config.h.
    uint32_t ringSize;                 // The size of the tx and rx rings.
    uint32_t ringBufferSize;           // The size, in bytes, of each buffer on
                                       // the ring.
    const uint32_t mtu;                // The MTU of the NIC.
    struct e1000_rafilter filter[32];  // Filters used by the
                                       // NIC. TODO(aravindn): #define
                                       // MAX_FILTERES
    uint32_t numFilters;               // The number of the filters present in
                                       // the filter array.
    const bool noBroadcast;            // Allow broadcasts.
    uint8_t myEthAddr[6];              // The ethernet address of this NIC.

    struct iovec sendiov[20];          // The iovec used by doSend to put
                                       // packets on the wire.
    uint32_t sendiovLen;               // The number of iovecs the sendiov array
                                       // contains.


    friend class E1000DriverTest;

    DISALLOW_COPY_AND_ASSIGN(E1000Driver);
};

};  // namespace RAMCloud

#endif  // RAMCLOUD_E1000_DRIVER_h
