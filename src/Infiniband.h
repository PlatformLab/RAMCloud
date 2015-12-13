/* Copyright (c) 2010-2015 Stanford University
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

#include <arpa/inet.h>
#include <netinet/in.h>
#include <infiniband/verbs.h>

#include "Common.h"
#include "Driver.h"
#include "MacAddress.h"
#include "Memory.h"
#include "ServiceLocator.h"
#include "Tub.h"
#include "Transport.h"

#ifndef RAMCLOUD_INFINIBAND_H
#define RAMCLOUD_INFINIBAND_H

namespace RAMCloud {

/**
 * A collection of Infiniband helper functions and classes, which can be shared
 * across different Infiniband transports and drivers.
 */
class Infiniband {
  PUBLIC:

    static const char*
    wcStatusToString(int status);

    // this class exists simply for passing queue pair handshake information
    // back and forth.
    class QueuePairTuple {
      public:
        QueuePairTuple() : qpn(0), psn(0), lid(0), nonce(0)
        {
            static_assert(sizeof(QueuePairTuple) == 68,
                              "QueuePairTuple has unexpected size");
        }
        QueuePairTuple(uint16_t lid, uint32_t qpn, uint32_t psn,
                       uint64_t nonce, const char* peerName = "?unknown?")
            : qpn(qpn), psn(psn), lid(lid), nonce(nonce)
        {
            snprintf(this->peerName, sizeof(this->peerName), "%s",
                peerName);
        }
        uint16_t    getLid() const      { return lid; }
        uint32_t    getQpn() const      { return qpn; }
        uint32_t    getPsn() const      { return psn; }
        uint64_t    getNonce() const    { return nonce; }
        const char* getPeerName() const { return peerName; }

      private:
        uint32_t qpn;            // queue pair number
        uint32_t psn;            // initial packet sequence number
        uint16_t lid;            // infiniband address: "local id"
        uint64_t nonce;          // random nonce used to confirm replies are
                                 // for received requests
        char peerName[50];       // Optional name for the sender (intended for
                                 // use in error messages); null-terminated.
    } __attribute__((packed));

    explicit Infiniband(const char* deviceName);
    ~Infiniband();

    void dumpStats();

    class DeviceList {
      public:
        DeviceList()
            : devices(ibv_get_device_list(NULL))
        {
            if (devices == NULL) {
                RAMCLOUD_LOG(WARNING, "Could not open infiniband device list");
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
                RAMCLOUD_LOG(WARNING, "failed to find infiniband device: %s",
                        name == NULL ? "any" : name);
                throw TransportException(HERE,
                    format("failed to find infiniband device: %s",
                           name == NULL ? "any" : name), errno);
            }

            ctxt = ibv_open_device(dev);
            if (ctxt == NULL) {
                RAMCLOUD_LOG(WARNING, "failed to open infiniband device: %s",
                        name == NULL ? "any" : name);
                throw TransportException(HERE,
                    format("failed to open infiniband device: %s",
                           name == NULL ? "any" : name), errno);
            }
        }

        ~Device() {
            int rc = ibv_close_device(ctxt);
            if (rc != 0)
                RAMCLOUD_LOG(WARNING, "ibv_close_device failed");
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
                RAMCLOUD_LOG(DEBUG, "ibv_dealloc_pd failed");
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
            initialPsn(-1), handshakeSin() {}
        ~QueuePair();
        uint32_t    getInitialPsn() const;
        uint32_t    getLocalQpNumber() const;
        uint32_t    getRemoteQpNumber() const;
        uint16_t    getRemoteLid() const;
        int         getState() const;
        bool        isError() const;
        void        plumb(QueuePairTuple *qpt);
        void        setPeerName(const char *peerName);
        const char* getPeerName() const;
        void        activate(const Tub<MacAddress>& localMac);
        const string  getSinName() const; // Name of handshakeSin

      //private:
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
        sockaddr_in  handshakeSin;   // UDP address of the remote end used to
                                     // handshake when using RC queue pairs.
        char         peerName[50];   // Optional name for the sender
                                     // (intended for use in error messages);
                                     // null-terminated.

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
                    const ServiceLocator* serviceLocator) : Exception(where,
                    "Service locator '" + serviceLocator->getOriginalString() +
                    "' couldn't be converted to Infiniband address: " + msg) {}
        };
        Address* clone() const {
            return new Address(*this);
        }
        string toString() const;

        Address(Infiniband& infiniband, int physicalPort,
                   const ServiceLocator* serviceLocator);
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

    // wrap an RX or TX buffer registered with the HCA
    // Possible memory management issues with mr member?
    struct BufferDescriptor {
        char *          buffer;         // buf of ``bytes'' length
        uint32_t        bytes;          // length of buffer in bytes
        uint32_t        messageBytes;   // byte length of message in the buffer
        ibv_mr *        mr;             // memory region of the buffer

        BufferDescriptor(char *buffer, uint32_t bytes, ibv_mr *mr)
            : buffer(buffer), bytes(bytes), messageBytes(0), mr(mr) {}
        BufferDescriptor()
            : buffer(NULL), bytes(0), messageBytes(0), mr(NULL) {}

      private:
        DISALLOW_COPY_AND_ASSIGN(BufferDescriptor);
    };

    /**
     * A region of memory registered with the HCA for DMA, split
     * into fixed-size buffers with easy-to-access BufferDescriptors
     * for each of them.
     *
     * RegisteredBuffers's purpose is twofold:
     *  1) It performs a single allocation and registration of a large set
     *     of buffers -- operations that have high per-call overheads.
     *  2) It provides a mapping from pointers into the buffers back to
     *     the BufferDescriptor which contains the details of the buffer.
     *     This is important, for example, in InfUdDriver::release where a
     *     pointer into the packet buffer is given to the driver which must then
     *     reclaim the buffer (see getDescriptor()).
     */
    class RegisteredBuffers {
      public:
        /**
         * Allocate BufferDescriptors and register the backing memory with the
         * HCA. Note that the memory will be wired (i.e. cannot be swapped out)!
         *
         * \param pd
         *      The ProtectionDomain this registered memory should be a part of.
         * \param bufferSize
         *      Size in bytes of each of the registered buffers this
         *      allocator manages.
         * \param bufferCount
         *      The number of registered buffers of #bufferSize to allocate.
         * \throw
         *      TransportException if allocation or registration failed.
         */
        RegisteredBuffers(ProtectionDomain& pd,
                          const uint32_t bufferSize,
                          const uint32_t bufferCount)
            : bufferSize(bufferSize)
            , bufferCount(bufferCount)
            , basePointer(NULL)
            , descriptors()
        {
            const size_t bytes = bufferSize * bufferCount;
            basePointer = Memory::xmemalign(HERE, 4096, bytes);

            ibv_mr *mr = ibv_reg_mr(pd.pd, basePointer, bytes,
                IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
            if (mr == NULL)
                throw TransportException(HERE,
                                         "failed to register buffer",
                                         errno);

            descriptors.reset(new BufferDescriptor[bufferCount]);
            char* buffer = static_cast<char*>(basePointer);
            for (uint32_t i = 0; i < bufferCount; ++i) {
                descriptors[i].~BufferDescriptor();
                new(&descriptors[i]) BufferDescriptor(buffer, bufferSize, mr);
                buffer += bufferSize;
            }
        }

        ~RegisteredBuffers()
        {
            free(basePointer);
        }

        /**
         * Get the BufferDescriptor associated with the buffer that #buffer
         * points into.
         *
         * \param buffer
         *       A pointer into a buffer allocated from this RegisteredBuffers.
         *       If #buffer does not point into a buffer from this
         *       RegisteredBuffers the result is undefined.
         * \return
         *      The BufferDescriptor for the buffer which #buffer points into.
         */
        BufferDescriptor&
        getDescriptor(const void* buffer)
        {
            size_t descriptorIndex = (static_cast<const char*>(buffer) -
                                      static_cast<char*>(basePointer)) /
                                     bufferSize;
            return descriptors[descriptorIndex];
        }

        typedef BufferDescriptor* iterator;
        typedef const BufferDescriptor* const_iterator;

        /**
         * Returns an iterator to the start of the BufferDescriptors in
         * this RegisteredBuffers.
         */
        BufferDescriptor*
        begin()
        {
            return descriptors.get();
        }

        /**
         * Returns an iterator to the end of the BufferDescriptors in
         * this RegisteredBuffers.
         */
        BufferDescriptor*
        end()
        {
            return descriptors.get() + bufferCount;
        }

      private:

        /// The size in bytes of each of the buffers.
        const uint32_t bufferSize;

        /// The count of buffers.
        const uint32_t bufferCount;

        /**
         * Points to the start of the first registered buffer.
         * Buffers are contiguous in memory, which means given a pointer
         * to a registered buffer its index in the consective sequence of
         * buffers is (ptr - basePointer) / bufferSize.
         */
        void* basePointer;

        /// BufferDescriptors for each of the buffers.
        std::unique_ptr<BufferDescriptor[]> descriptors;

        DISALLOW_COPY_AND_ASSIGN(RegisteredBuffers);
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
    tryReceive(QueuePair* qp, Tub<Address>* sourceAddress = NULL);

    BufferDescriptor*
    receive(QueuePair* qp, Tub<Address>* sourceAddress = NULL);

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

//  Keep public for 0-copy hack. nothing to see here, move along.
//  PRIVATE:
    Device device;
    ProtectionDomain pd;

    // A cache of address handles for all of the hosts we have ever
    // communicated with. Keys are lids. Needed so that we don't have to
    // call ibv_create_ah for every UD packet in order to send a response
    // (as of 11/2015, these calls take 40-50us!). These entries are never
    // garbage collected, but there will be only one entry per host (and the
    // lids are only 16 bits), so the memory usage should be tolerable.
    typedef std::unordered_map<uint16_t, ibv_ah*> AddressHandleMap;
    AddressHandleMap ahMap;

    uint64_t totalAddressHandleAllocCalls;
    uint64_t totalAddressHandleAllocTime;
    static const uint32_t MAX_INLINE_DATA = 400;

    // The following variables keep track of queue pair creations and
    // deletions; this information is printed when queue pair creation fails,
    // to help provide more information.
    int totalQpCreates;
    int totalQpDeletes;
};

} // namespace RAMCloud

#endif // !RAMCLOUD_INFINIBAND_H
