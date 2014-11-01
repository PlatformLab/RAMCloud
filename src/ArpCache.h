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

#ifndef RAMCLOUD_ARPCACHE_H
#define RAMCLOUD_ARPCACHE_H

#include <net/if.h>
#include <array>
#include "Syscall.h"
#include "Dispatch.h"
#include "Tub.h"
#include "Driver.h"
#include "NetUtil.h"

namespace RAMCloud {

/**
 * ArpCache provides a local table for IP-MAC translations. This table is
 * basically a cache to resolve MAC address corresponding to an IP address.
 * Layer 3 drivers codes keep an instance of this ArpCache objects for the
 * purpose of fast IP-MAC resolutions.
 */
class ArpCache {

  public:
    typedef NetUtil::MacAddress MacAddress;
    typedef std::array<uint8_t, NetUtil::MAC_ADDR_LEN> MacArray;

    explicit ArpCache(Context* context, const uint32_t localIp,
        const char* ifName, const char* routeFile = "/proc/net/route");
    ~ArpCache();
    bool arpLookup(const uint32_t destIp, MacAddress destMac);

    /// The maximum length of a NIC name as defined in linux header files.
    static const int MAX_IFACE_LEN = IFNAMSIZ;

    /// Number of allowed retries to read Kernel's ARP Cache when the Kernel
    /// Cache is busy and not available to be accessed by our code.
    static const int ARP_RETRIES = 10;

    /// Number of micro seconds to wait before retrying to read Kernel's ARP
    /// cache when the Kernel cache is busy. The experiments showed that in some
    /// cases it takes roughly 1.5ms to get accessed to updated Kernel ARP
    /// entries.
    static const int ARP_WAIT = 200;

    /**
     * This class helps to find out if the destination IP is on the same subnet
     * of the local machine and if not, helps us to figure the gateway IP
     * address (gateways are the routers on the same subnet as the local machine
     * that are capable of forwarding the local machine packets to the
     * destinations outside that subnet.)
     *
     */
    class RouteTable {
      public:
        friend class ArpCache;
        explicit RouteTable(const char* ifName, const char* routeFile);
        uint32_t getNextHopIp(const uint32_t destIp);

        /**
         * Each entry provides a gateway for a range of destination IP addresses
         * that can be reached from a particular interface. If the gateway is
         * not specified (ie. Zero), then that range of IP address are directly
         * accessible from the interface without needing a gateway.
         */
        struct RouteEntry {

            /// This value along with the #netmask defines the range of
            /// destination IP addresses that are accessible through #gateway.
            /// This value is in  network byte order.
            uint32_t destNetworkIp;

            /// Defines the destination subnet of #destNetworkIp. Every other IP
            /// address, that is on this subnet of #destNetworkIp, will also be
            /// accessible through #gateway. If netMask is 0xFFFFFFFF, then
            /// destNetworkIp refers to a single host. Otherwise, a long with
            /// netmask, it refers to a network. netMask in network byte order.
            uint32_t netMask;

            /// Gateway IP address is the IP address of a router on subnet of
            /// local machine that is capable of forwarding packets from local
            /// machine to the addresses on the subnet of destination network
            /// (specified by #netmask and #destNetworkIp). This value is in
            /// network byte order.
            uint32_t gatewayIp;
        };

      PRIVATE:

        /// Contains all of the routing table entries that corresponds to
        /// interface name #ArpCache::ifName.
        std::vector<RouteEntry> routeVector;

        DISALLOW_COPY_AND_ASSIGN(RouteTable);
    };

  PRIVATE:
    bool lookupKernelArpCache(const uint32_t destIp, MacAddress destMac);
    void sendUdpPkt(struct sockaddr* destAddress);

    /// Defines a map from 32bit IP addresses to Mac addresses. The local
    /// IP-MAC cache (local ARP cache) is an instance of this IpMacMap.
    /// It will contain the MAC addresses of gateways and/or the destination
    /// machines that are on the same subnet as the local machine.
    typedef std::unordered_map<uint32_t, MacArray> IpMacMap;
    IpMacMap ipMacMap;

    /// A socket, used to send UDP packets to force the kernel to create a new
    /// entry in its ARP table.
    int fd;

    /// Socket file descriptor for performing ioctl on Kernel's ARP cache.
    int fdArp;

    /// Keeps a local route table for fast gateway resolution.
    RouteTable routeTable;

    /// RAMCloud shared information.
    Context* context;

    /// Name of our local network interface corresponding to the local IP. This
    /// is the interface on which the driver (that owns ArpCache object) will
    /// use to transmit and receive packets.
    char ifName[MAX_IFACE_LEN];

    /// A socket address of type sockaddr_in that contains local IP of the
    /// driver.
    struct sockaddr localAddress;
    static Syscall* sys;

    DISALLOW_COPY_AND_ASSIGN(ArpCache);
};

/**
 * Thrown if a ArpCache cannot be initialized properly. 
 */
struct ArpCacheException: public Exception {
    explicit ArpCacheException(const CodeLocation& where)
        : Exception(where) {}
    ArpCacheException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
    ArpCacheException(const CodeLocation& where, int errNo)
        : Exception(where, errNo) {}
    ArpCacheException(const CodeLocation& where, string msg, int errNo)
        : Exception(where, msg, errNo) {}
};
} //namespace RAMCloud
#endif //RAMCLOUD_ARPCACHE_H
