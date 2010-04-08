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
 * \file Header file for the Service class.
 */

#ifndef RAMCLOUD_SERVICE_H
#define RAMCLOUD_SERVICE_H

#include <Common.h>

namespace RAMCloud {

const uint8_t ETH_ALEN = 6;

/**
 * \class
 *
 * This class represents a logical Service identified by a unique service
 * id. The current address information for this Service is also part of this
 * class. While the service id is unique, the address information may change
 * from time to time as a Service moves between physical machines.
 */       
class Service {
  public:
    /**
     * Get the Service Id of this Service.
     *
     * \return See comment
     */
    inline uint64_t getServiceId() const { return serviceId; }

    /**
     * Give this Service a new Service Id.
     *
     * \param[in] newServiceId The new Service Id of this Service.
     */
    inline void setServiceId(uint64_t newServiceId) {
        serviceId = newServiceId;
    }

    /**
     * Get the IP address currently associated with this Service.
     * Note: The IP Address may change after a call to refreshAddress().
     *
     * \return See comment.
     */
    inline uint32_t getIp() const { return ip; }

    /**
     * Change the IP address currently associated with this Service
     * Note: This function is a temporary necessity, until we implement a proper
     * refreshAddress which actually talks to the coordinator to get the new IP.
     *
     * \param[in] newIp The new IP address.
     */
    inline void setIp(uint32_t newIp) { ip = newIp; }

    /**
     * Get the MAC address currently associated with this Service.
     * Note: The MAC addres may change after a call to refreshAddress().
     *
     * \return See comment.
     */
    inline const char* getMac() const { return mac; }

    /**
     * Change the MAC address currently associated with this Service.
     * Note: This function is a temporary necessity, until we implement a proper
     * refreshAddress which actually talks to the coordinator to get the new MAC
     * address.
     *
     * \param[in] newMac The new MAC address.
     */
    inline void setMac(char* newMac) { memcpy(mac, newMac, 6); }

    /**
     * An unimplmeneted placeholder function. When we have a coordinator, this
     * will talk to it to get the IP and MAC addresses currently associated with
     * this Service Id.
     */
    void refreshAddress();

    Service () : serviceId(0), ip(0) { bzero(mac, 6); }

  private:
    uint64_t serviceId;  // Unique identification number for this Service.
    uint32_t ip;         // Current IP address of this service.
    char mac[6];         // Current ethernet MAC address of this Service.

    friend class ServiceTest;
    
    DISALLOW_COPY_AND_ASSIGN(Service);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_SERVICE_H
