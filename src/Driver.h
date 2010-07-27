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
 * Header file for the Driver classes.
 */

#ifndef RAMCLOUD_DRIVER_H
#define RAMCLOUD_DRIVER_H

#include <Common.h>

#include <Buffer.h>

#include <sys/types.h>
#include <sys/socket.h>

#undef CURRENT_LOG_MODULE
#define CURRENT_LOG_MODULE TRANSPORT_MODULE

namespace RAMCloud {

struct UnrecoverableDriverException: public Exception {
    UnrecoverableDriverException() : Exception() {}
    explicit UnrecoverableDriverException(std::string msg)
        : Exception(msg) {}
    explicit UnrecoverableDriverException(int errNo) : Exception(errNo) {}
};

class Driver {
  public:
    struct Received {
        Driver *driver;
        sockaddr addr;
        socklen_t addrlen;
        uint32_t len;
        char *payload;
        Received() : driver(0), addr(), addrlen(0), len(0), payload(0) {}
        ~Received() {
            if (driver)
                driver->release(payload, len);
        }
        void* getRange(uint32_t offset, uint32_t length) {
           if (offset + length > len)
               return NULL;
           return static_cast<void*>(payload + offset);
        }
        template<typename T>
        T* getOffset(uint32_t offset) {
           return static_cast<T*>(getRange(offset, sizeof(T)));
        }
        char *steal(uint32_t *len) {
            char *p = payload;
            payload = NULL;
            *len = this->len;
            this->len = 0;
            return p;
        }
      private:
        DISALLOW_COPY_AND_ASSIGN(Received);
    };

    virtual uint32_t getMaxPayloadSize() = 0;
    /// Blocks until the NIC has the packet data.
    virtual void sendPacket(const sockaddr *addr,
                            socklen_t addrlen,
                            void *header,
                            uint32_t headerLen,
                            Buffer::Iterator *payload) = 0;
    /**
     * Try to receive a packet off the network.
     * Returns (payload, length, address), or None if there was no packet
     * available. The caller must call release() with this payload and
     * length later.
     */
    virtual bool tryRecvPacket(Received *received) = 0;
    virtual void release(char *payload, uint32_t len);
    virtual ~Driver();
};

class UDPDriver : public Driver {
  public:
    static const uint32_t MAX_PAYLOAD_SIZE = 1400;
    virtual uint32_t getMaxPayloadSize();
    virtual void sendPacket(const sockaddr *addr,
                            socklen_t addrlen,
                            void *header,
                            uint32_t headerLen,
                            Buffer::Iterator *payload);
    virtual bool tryRecvPacket(Received *received);
    virtual void release(char *payload, uint32_t len);
    UDPDriver();
    UDPDriver(const sockaddr *addr, socklen_t addrlen);
    virtual ~UDPDriver();
  private:
    void send(const Buffer* payload);
    int socketFd;
    int packetBufsUtilized;
    DISALLOW_COPY_AND_ASSIGN(UDPDriver);
};

}  // namespace RAMCloud

#endif
