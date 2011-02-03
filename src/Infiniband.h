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

#include <infiniband/verbs.h>

#include "Common.h"
#include "Driver.h"
#include "ObjectTub.h"
#include "Transport.h"

#ifndef RAMCLOUD_INFINIBAND_H
#define RAMCLOUD_INFINIBAND_H

namespace RAMCloud {

/**
 * A collection of Infiniband helper functions and classes, which can be shared
 * across different Infiniband transports and drivers.
 */
class RealInfiniband {
    typedef RealInfiniband Infiniband;
  PUBLIC:

    static const char*
    wcStatusToString(int status);

    // this class exists simply for passing queue pair handshake information
    // back and forth.
    class QueuePairTuple {
      public:
        QueuePairTuple() : qpn(0), psn(0), lid(0), nonce(0)
        {
            static_assert(sizeof(QueuePairTuple) == 18,
                              "QueuePairTuple has unexpected size");
        }
        QueuePairTuple(uint16_t lid, uint32_t qpn, uint32_t psn,
                       uint64_t nonce)
            : qpn(qpn), psn(psn), lid(lid), nonce(nonce) {}
        uint16_t getLid() const { return lid; }
        uint32_t getQpn() const { return qpn; }
        uint32_t getPsn() const { return psn; }
        uint64_t getNonce() const { return nonce; }

      private:
        uint32_t qpn;            // queue pair number
        uint32_t psn;            // initial packet sequence number
        uint16_t lid;            // infiniband address: "local id"
        uint64_t nonce;          // random nonce used to confirm replies are
                                 // for received requests
    } __attribute__((packed));

    // wrap an RX or TX buffer registered with the HCA
    // TODO(ongaro): memory management on mr member
    struct BufferDescriptor {
        char *          buffer;         // buf of ``bytes'' length
        uint32_t        bytes;          // length of buffer in bytes
        uint32_t        messageBytes;   // byte length of message in the buffer
        ibv_mr *        mr;             // memory region of the buffer

        BufferDescriptor(char *buffer, uint64_t bytes, ibv_mr *mr) :
            buffer(buffer), bytes(bytes), messageBytes(0), mr(mr)
        {
        }
        BufferDescriptor() : buffer(NULL), bytes(0), messageBytes(0),
            mr(NULL) {}
    };

    explicit RealInfiniband(const char* deviceName);
    ~RealInfiniband();

    void dumpStats();

    class DeviceList {
      public:
        DeviceList()
            : devices(ibv_get_device_list(NULL))
        {
            if (devices == NULL) {
                throw TransportException(HERE,
                    "Could not open infiniband device list", errno);
            }
        }
        ~DeviceList() {
            ibv_free_device_list(devices);
        }
        ibv_device*
        lookup(const char* name) {
            if (name == NULL)
                return devices[0];
            for (int i = 0; devices[i] != NULL; i++) {
                if (strcmp(devices[i]->name, name) == 0)
                    return devices[i];
            }
            return NULL;
        }
      private:
        ibv_device** const devices;
        DISALLOW_COPY_AND_ASSIGN(DeviceList);
    };

    class Device {
      public:
        explicit Device(const char* name)
            : ctxt(NULL)
        {
            // The lifetime of the device list needs to extend
            // through the call to ibv_open_device.
            DeviceList deviceList;

            auto dev = deviceList.lookup(name);
            if (dev == NULL) {
                throw TransportException(HERE,
                    format("failed to find infiniband device: %s",
                           name == NULL ? "any" : name), errno);
            }

            ctxt = ibv_open_device(dev);
            if (ctxt == NULL) {
                throw TransportException(HERE,
                    format("failed to open infiniband device: %s",
                           name == NULL ? "any" : name), errno);
            }
        }

        ~Device() {
            int rc = ibv_close_device(ctxt);
            if (rc != 0)
                LOG(WARNING, "ibv_close_device failed");
        }

        ibv_context* ctxt; // const after construction
        DISALLOW_COPY_AND_ASSIGN(Device);
    };

    class ProtectionDomain {
      public:
        explicit ProtectionDomain(Device& device)
            : pd(ibv_alloc_pd(device.ctxt))
        {
            if (pd == NULL) {
                throw TransportException(HERE,
                    "failed to allocate infiniband protection domain", errno);
            }
        }
        ~ProtectionDomain() {
            int rc = ibv_dealloc_pd(pd);
            if (rc != 0) {
                // TODO(ongaro): Change to WARNING after closing RAM-195.
                LOG(DEBUG, "ibv_dealloc_pd failed");
            }
        }
        ibv_pd* const pd;
        DISALLOW_COPY_AND_ASSIGN(ProtectionDomain);
    };

    // this class encapsulates the creation, use, and destruction of an RC
    // queue pair.
    //
    // the constructor will create a qp and bring it to the INIT state.
    // after obtaining the lid, qpn, and psn of a remote queue pair, one
    // must call plumb() to bring the queue pair to the RTS state.
    class QueuePair {
      public:
        QueuePair(Infiniband& infiniband,
                  ibv_qp_type type,
                  int ibPhysicalPort,
                  ibv_srq *srq,
                  ibv_cq *txcq,
                  ibv_cq *rxcq,
                  uint32_t maxSendWr,
                  uint32_t maxRecvWr,
                  uint32_t QKey = 0);
        // exists solely as superclass constructor for MockQueuePair derivative
        explicit QueuePair(Infiniband& infiniband)
            : infiniband(infiniband), type(0), ctxt(NULL), ibPhysicalPort(-1),
            pd(NULL), srq(NULL), qp(NULL), txcq(NULL), rxcq(NULL),
            initialPsn(-1) {}
        ~QueuePair();
        uint32_t getInitialPsn() const;
        uint32_t getLocalQpNumber() const;
        uint32_t getRemoteQpNumber() const;
        uint16_t getRemoteLid() const;
        int      getState() const;
        void     plumb(QueuePairTuple *qpt);
        void     activate();

      //private: XXXXX- move send/recv functionality into the queue pair shit
        Infiniband&  infiniband;     // Infiniband to which this QP belongs
        int          type;           // QP type (IBV_QPT_RC, etc.)
        ibv_context* ctxt;           // device context of the HCA to use
        int          ibPhysicalPort; // physical port number of the HCA
        ibv_pd*      pd;             // protection domain
        ibv_srq*     srq;            // shared receive queue
        ibv_qp*      qp;             // infiniband verbs QP handle
        ibv_cq*      txcq;           // transmit completion queue
        ibv_cq*      rxcq;           // receive completion queue
        uint32_t     initialPsn;     // initial packet sequence number

        DISALLOW_COPY_AND_ASSIGN(QueuePair);
    };

    /**
     * This class translates between ServiceLocators and native Infiniband
     * addresses (LIDs, QueuePair numbers, etc), providing a standard mechanism
     * for use in Transport and Driver classes.
     */
    class Address : public Driver::Address {
      public:
        /**
         * Exception that is thrown when a ServiceLocator can't be
         * parsed into an Infiniband address.
         */
        class BadAddressException : public Exception {
          public:
            /**
             * Construct a BadAddressException.
             * \param where
             *      Pass #HERE here.
             * \param msg
             *      String describing the problem; should start with a
             *      lower-case letter.
             * \param serviceLocator
             *      The ServiceLocator that couldn't be parsed: used to
             *      generate a prefix message containing the original locator
             *      string.
             */
            explicit BadAddressException(const CodeLocation& where,
                                            std::string msg,
                    const ServiceLocator& serviceLocator) : Exception(where,
                    "Service locator '" + serviceLocator.getOriginalString() +
                    "' couldn't be converted to Infiniband address: " + msg) {}
        };
        Address* clone() const {
            return new Address(*this);
        }
        string toString() const;

        Address(Infiniband& infiniband, int physicalPort,
                   const ServiceLocator& serviceLocator);
        Address(Infiniband& infiniband, int physicalPort,
                   uint16_t lid, uint32_t qpn)
            : Driver::Address()
            , infiniband(infiniband)
            , physicalPort(physicalPort)
            , lid(lid)
            , qpn(qpn)
            , ah(NULL)
        {
        }
        Address(const Address& other)
            : Driver::Address(other)
            , infiniband(other.infiniband)
            , physicalPort(other.physicalPort)
            , lid(other.lid)
            , qpn(other.qpn)
            , ah(NULL) // don't want multiple ibv_destroy_ah calls
        {
        }
        ~Address();

        int getPhysicalPort() const { return physicalPort; }
        uint16_t getLid() const { return lid; }
        uint32_t getQpn() const { return qpn; }
        ibv_ah* getHandle() const;

        void operator=(Address&) = delete;

      private:
        Infiniband& infiniband; // Infiniband instance under which this address
                                // is valid.
        int physicalPort;   // physical port number on local device
        uint16_t lid;       // local id (address)
        uint32_t qpn;       // queue pair number
        mutable ibv_ah* ah; // address handle, may be NULL
    };

    QueuePair*
    createQueuePair(ibv_qp_type type,
                    int ibPhysicalPort,
                    ibv_srq *srq,
                    ibv_cq *txcq,
                    ibv_cq *rxcq,
                    uint32_t maxSendWr,
                    uint32_t maxRecvWr,
                    uint32_t QKey = 0);

    int
    getLid(int port);

    BufferDescriptor*
    tryReceive(QueuePair* qp, ObjectTub<Address>* sourceAddress = NULL);

    BufferDescriptor*
    receive(QueuePair* qp, ObjectTub<Address>* sourceAddress = NULL);

    void
    postReceive(QueuePair* qp, BufferDescriptor* bd);

    void
    postSrqReceive(ibv_srq* srq, BufferDescriptor* bd);

    void
    postSend(QueuePair* qp,
             BufferDescriptor* bd,
             uint32_t length,
             const Address* address = NULL,
             uint32_t remoteQKey = 0);

    void
    postSendAndWait(QueuePair* qp,
                    BufferDescriptor* bd,
                    uint32_t length,
                    const Address* address = NULL,
                    uint32_t remoteQKey = 0);

    BufferDescriptor
    allocateBufferDescriptorAndRegister(size_t bytes);

    ibv_cq*
    createCompletionQueue(int minimumEntries);

    ibv_ah*
    createAddressHandle(ibv_ah_attr* attr);

    void
    destroyAddressHandle(ibv_ah *ah);

    ibv_srq*
    createSharedReceiveQueue(uint32_t maxWr, uint32_t maxSge);

    int
    pollCompletionQueue(ibv_cq *cq,
                        int numEntries,
                        ibv_wc *retWcArray);

  PRIVATE:
    Device device;
    ProtectionDomain pd;
    uint64_t totalAddressHandleAllocCalls;
    uint64_t totalAddressHandleAllocTime;
    static const uint32_t MAX_INLINE_DATA = 400;
};

} // namespace RAMCloud

#endif // !RAMCLOUD_INFINIBAND_H
