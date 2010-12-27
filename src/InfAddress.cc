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

#include "InfAddress.h"
#include "Common.h"

namespace RAMCloud {

/**
 * Construct an InfAddress from the information in a ServiceLocator.
 * \param serviceLocator
 *      The "lid" and "qpn" options describe the desired address.
 * \throw BadInfAddress
 *      The serviceLocator couldn't be converted to an InfAddress
 *      (e.g. a required option was missing, or the host name
 *      couldn't be parsed).
 */
InfAddress::InfAddress(const ServiceLocator& serviceLocator)
    : address()
{
    try {
        address.lid = serviceLocator.getOption<uint16_t>("lid");
    } catch (NoSuchKeyException &e) {
        throw BadInfAddressException(HERE,
            std::string("Mandatory option ``lid'' missing from infiniband "
            " ServiceLocator."), serviceLocator);
    } catch (...) {
        throw BadInfAddressException(HERE,
            std::string("Could not parse lid. Invalid or out of range."),
            serviceLocator);
    }

    try {
        address.qpn = serviceLocator.getOption<uint32_t>("qpn");
    } catch (NoSuchKeyException &e) {
        throw BadInfAddressException(HERE,
            std::string("Mandatory option ``qpn'' missing from infiniband "
            " ServiceLocator."), serviceLocator);
    } catch (...) {
        throw BadInfAddressException(HERE,
            std::string("Could not parse qpn. Invalid or out of range."),
            serviceLocator);
    }
}

/**
 * Return a string describing the contents of this InfAddress (host
 * address & port).
 */
string
InfAddress::toString() const {
    return format("%u:%u", address.lid, address.qpn);
}

} // namespace RAMCloud
