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

#include <errno.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "Common.h"
#include "IpAddress.h"

namespace RAMCloud {

/**
 * Construct an IpAddress from the information in a ServiceLocator.
 * \param serviceLocator
 *      The "host" and "port" options describe the desired address.
 * \throw BadIpAddress
 *      The serviceLocator couldn't be converted to an IpAddress
 *      (e.g. a required option was missing, or the host name
 *      couldn't be parsed).
 */
IpAddress::IpAddress(const ServiceLocator& serviceLocator)
    : address()
{
    try {
        sockaddr_in *addr = reinterpret_cast<sockaddr_in*>(&address);
        addr->sin_family = AF_INET;

        std::string hostName = serviceLocator.getOption("host");
        hostent* host = gethostbyname(hostName.c_str());
        if (host == NULL) {
            throw BadIpAddressException(std::string("couldn't find host '") +
                                        hostName + "'", serviceLocator);
        }
        memcpy(&addr->sin_addr, host->h_addr, sizeof(addr->sin_addr));
        addr->sin_port = htons(serviceLocator.getOption<uint16_t>("port"));
    } catch (ServiceLocator::NoSuchKeyException& e) {
        throw BadIpAddressException(e.message, serviceLocator);
    } catch (boost::bad_lexical_cast& e) {
        throw BadIpAddressException(e.what(), serviceLocator);
    }
}

/**
 * Return a string describing the contents of this IpAddress (host
 * address & port).
 */
string
IpAddress::toString() const {
    char buffer[50];
    const sockaddr_in *addr = reinterpret_cast<const sockaddr_in*>(&address);
    uint32_t ip = ntohl(addr->sin_addr.s_addr);
    uint32_t port = ntohs(addr->sin_port);
    snprintf(buffer, sizeof(buffer), "%d.%d.%d.%d:%d", (ip>>24)&0xff,
            (ip>>16)&0xff, (ip>>8)&0xff, ip&0xff, port);
    return string(buffer);
}

} // namespace RAMCloud
