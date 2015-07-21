/* Copyright (c) 2012-2015 Stanford University
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

#ifndef RAMCLOUD_SERVICEMASK_H
#define RAMCLOUD_SERVICEMASK_H

#include <initializer_list>
#include <sstream>

#include "Logger.h"
#include "WireFormat.h"

namespace RAMCloud {

/**
 * Describes which services are provided by a particular server.
 * Instances are extremely cheap to copy.
 */
class ServiceMask {
  PUBLIC:
    /**
     * Create a ServiceMask that describes a server with no services.
     */
    ServiceMask()
        : mask(0)
    {
    }

    /**
     * Create a ServiceMask from a list of ServiceTypes. This is constructor
     * is typically invoked through C++11 initializer_list syntax, e.g.:
     * c->enlistServer({MASTER_SERVICE, BACKUP_SERVICE}, "mock:");
     *
     * \param services
     *      Which services should be marked as available on this server.
     *      INVALID_SERVICE is ignored if it is pass as an element of services.
     */
    ServiceMask(std::initializer_list<WireFormat::ServiceType>
            services) // NOLINT
        : mask(0)
    {
        for (auto it = services.begin(); it != services.end(); ++it) {
            if (*it == WireFormat::INVALID_SERVICE)
                continue;
            mask |= (1 << *it);
        }
    }

    /**
     * Returns true if a service of type \a service is available on the server
     * described by this ServiceMask, false otherwise. has(INVALID_SERVICE)
     * should always be false by construction.
     */
    bool has(WireFormat::ServiceType service) const
    {
        return mask & (1 << service);
    }

    /**
     * Returns true if this ServiceMask includes all of the services
     * specified by the "services" argument, false otherwise.
     *
     * \param services
     *      Specifies zero or more service types.  If this mask is empty,
     *      then true is returned.
     */
    bool hasAll(ServiceMask services) const
    {
        return (mask & services.mask) == services.mask;
    }

    /**
     * Returns true if this ServiceMask includes any of the services
     * specified by the "services" argument, false otherwise.
     *
     * \param services
     *      Specifies zero or more service types. If this mask is empty,
     *      false is returned.
     */
    bool hasAny(ServiceMask services) const
    {
        return (mask & services.mask) != 0;
    }

    /**
     * Return a comma-separated, human-readable list of the services marked as
     * available in this ServiceMask.  Example output:
     * "MASTER_SERVICE, PING_SERVICE, MEMBERSHIP_SERVICE"
     */
    string toString() const
    {
        std::stringstream s;
        bool first = true;
        for (size_t bit = 0; bit < sizeof(mask) * 8; ++bit) {
            WireFormat::ServiceType service =
                    static_cast<WireFormat::ServiceType>(bit);
            if (has(service)) {
                if (!first)
                    s << ", ";
                s << WireFormat::serviceTypeSymbol(service);
                first = false;
            }
        }
        return s.str();
    }

    /**
     * Return a serialized format for rpcs.  All uses of SerializedServiceMask other
     * than to call serialize/deserialize on this class should be avoided like
     * the plague.
     */
    WireFormat::SerializedServiceMask
    serialize() const
    {
        return mask;
    }

    /**
     * Return a ServiceMask from a SerializedServiceMask that was produced by
     * serialize().  Used to send ServiceMasks in rpcs.  All uses of
     * SerializedServiceMask other than to call serialize/deserialize on this class
     * should be avoided like the plague.
     */
    static ServiceMask deserialize(WireFormat::SerializedServiceMask mask)
    {
        // Check to make sure all bits correspond to valid ServiceTypes.
        const WireFormat::SerializedServiceMask validBits =
                (1 << WireFormat::INVALID_SERVICE) - 1;
        if ((mask & ~validBits) != 0) {
            RAMCLOUD_LOG(WARNING,
                "Unexpected high-order bits set in SerializedServiceMask "
                "being deserialized which do not correspond to a valid "
                "ServiceType; ignoring the extra bits; you might want to "
                "check your code closely; something is wrong.");
            mask &= validBits;
        }
        ServiceMask m;
        m.mask = mask;
        return m;
    }

  PRIVATE:
    /**
     * Internally the mask already stored in its serialized form, which is
     * simply as a bitfield. The bit at offset n represents the availabilty of
     * the ServiceType whose integer conversion is n.
     */
    WireFormat::SerializedServiceMask mask;
};

} // namespace RAMCloud

#endif
