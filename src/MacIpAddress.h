/* Copyright (c) 2014-2015 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef RAMCLOUD_MACIPADDRESS_H
#define RAMCLOUD_MACIPADDRESS_H

#include "Common.h"
#include "Driver.h"
#include "ServiceLocator.h"
#include "IpAddress.h"
#include "MacAddress.h"

namespace RAMCloud {

/**
 * This class provides a way for the SolarFlareDriver and transport to deduce
 * the address of the destination or source(local address) of a packet.
 */
class MacIpAddress : public Driver::Address {
  public:
    explicit MacIpAddress(const ServiceLocator* serviceLocator);
    explicit MacIpAddress(const uint32_t ip,
                               const uint16_t port,
                               const uint8_t mac[6] = NULL);


    explicit MacIpAddress(const MacIpAddress& other)
        : Address()
        , ipAddress(other.ipAddress)
        , macAddress(other.macAddress)
        , macProvided(other.macProvided)
    {}

    MacIpAddress* clone() const {
        return new MacIpAddress(*this);
    }

    string toString() const;

    // A RAMCloud::IpAddress object that hold the layer 3 address
    Tub<IpAddress> ipAddress;

    // A RAMCloud::MacAddress object that holds the layer 2 address
    Tub<MacAddress> macAddress;

    // True if MAC address is provided and not equal to 00:00:00:00:00:00
    bool macProvided;
};

}// end RAMCloud
#endif //RAMCLOUD_MACIPADDRESS_H
