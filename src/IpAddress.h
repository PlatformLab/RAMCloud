/* Copyright (c) 2010 Stanford University
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

#ifndef RAMCLOUD_IPADDRESS_H
#define RAMCLOUD_IPADDRESS_H

#include <netinet/in.h>

#include "Common.h"
#include "Driver.h"
#include "ServiceLocator.h"

namespace RAMCloud {

/**
 * This class translates between ServiceLocators and IP sockaddr structs,
 * providing a standard mechanism for use in Transport and Driver classes.
 */
class IpAddress : public Driver::Address {
  public:
    /**
     * Exception that is thrown when a ServiceLocator can't be
     * parsed into an IP address.
     */
    class BadIpAddressException : public Exception {
      public:
        /**
         * Construct a BadIpAddressException.
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
        explicit BadIpAddressException(const CodeLocation& where,
                                       std::string msg,
                const ServiceLocator& serviceLocator) : Exception(where,
                "Service locator '" + serviceLocator.getOriginalString() +
                "' couldn't be converted to IP address: " + msg) {}
    };

    IpAddress() : address() {}
    explicit IpAddress(const ServiceLocator& serviceLocator);
    explicit IpAddress(const sockaddr *address);
    explicit IpAddress(const uint32_t ip, const uint16_t port);
    IpAddress(const IpAddress& other)
        : Address(other), address(other.address) {}
    IpAddress* clone() const {
        return new IpAddress(*this);
    }
    string toString() const;
    sockaddr address;
    private:
    void operator=(IpAddress&);
};

} // end RAMCloud

#endif  // RAMCLOUD_IPADDRESS_H
