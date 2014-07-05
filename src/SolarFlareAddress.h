/* Copyright (c) 2014 Stanford University
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

#ifndef RAMCLOUD_SOLARFLAREADDRESS_H
#define RAMCLOUD_SOLARFLAREADDRESS_H

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
class SolarFlareAddress : public Driver::Address {
  public:
    explicit SolarFlareAddress(const ServiceLocator& serviceLocator);
    explicit SolarFlareAddress(const uint8_t mac[6]
                               , const uint32_t ip
                               , const uint16_t port);
    explicit SolarFlareAddress(const SolarFlareAddress& other)
        : Address()
        , ipAddress(other.ipAddress)
        , macAddress(other.macAddress)
    {}
    SolarFlareAddress* clone() const {
        return new SolarFlareAddress(*this);
    }
    string toString() const;
    //a RAMCloud::IpAddress object that hold the layer 3 address
    IpAddress ipAddress;
    //a RAMCloud::MacAddress object that holds the layer 2 address
    MacAddress macAddress;
};

}// end RAMCloud
#endif //RAMCLOUD_SOLARFLAREADDRESS_H
