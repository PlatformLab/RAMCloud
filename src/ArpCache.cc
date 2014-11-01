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

#include <net/if_arp.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <fstream>
#include "ArpCache.h"
#include "NetUtil.h"
#include "Common.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Default object used to make system calls.
 */
static Syscall defaultSyscall;

/**
 * Used by this class to make all system calls.  In normal production
 * use it points to defaultSyscall; for testing it points to a mock
 * object.
 */
Syscall* ArpCache::sys = &defaultSyscall;

/**
 * Constructor for ArpCache object.
 *
 * \param context
 *      Overall and shared information about RAMCloud client or server.
 * \param localIp
 *      The 32 bit IP address in network byte order needed to send UDP packets
 *      for triggering the ARP module in kernel. It corresponds to the interface
 *      that will be used to send packets (ie. `ifName').
 * \param ifName
 *      The name of network interface in the local machine that owns localIp
 *      address. The ARP translations provided by this module are only valid for
 *      this interface.
 * \param routeFile
 *      The full name (including absolute path) for Kernel's route table file.
 *      This file, in most of Linux distros, is "/proc/net/route"
 */
ArpCache::ArpCache(Context* context, const uint32_t localIp,
        const char* ifName, const char* routeFile)
    : ipMacMap()
    , fd(-1)
    , fdArp(-1)
    , routeTable(ifName, routeFile)
    , context(context)
    , ifName()
    , localAddress()
{
    snprintf(this->ifName, MAX_IFACE_LEN, "%s", ifName);
    struct sockaddr_in *local = reinterpret_cast<sockaddr_in*>(&localAddress);
    local->sin_family = AF_INET;
    local->sin_addr.s_addr = localIp;

    // By setting port to 0, we let the Kernel choose a port when we bind the
    // socket.
    local->sin_port = HTONS(0);
    fd = sys->socket(AF_INET, SOCK_DGRAM, 0);
    if ((fd = sys->socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        string msg =
            "ArpCache could not create a socket for resolving mac addresses!";
        LOG(WARNING, "%s", msg.c_str());
        throw ArpCacheException(HERE, msg.c_str(), errno);
    }

    int r = sys->bind(fd, &localAddress, sizeof(localAddress));
    if (r < 0) {
        string msg = format("ArpCache could not bind the socket to address %s",
            inet_ntoa(local->sin_addr));
        LOG(WARNING, "%s", msg.c_str());
        throw ArpCacheException(HERE, msg.c_str(), errno);
    }

    if ((fdArp = sys->socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        string msg =
            "ArpCache could not create socket for performing IO control";
        LOG(WARNING, "%s", msg.c_str());
        throw ArpCacheException(HERE, msg.c_str(), errno);
    }
}

/**
 * Destructor of ArpCache object. 
 */
ArpCache::~ArpCache()
{
    sys->close(fd);
    fd = -1;
    sys->close(fdArp);
    fdArp = -1;
}

/**
 * Given a destination IP address this method returns the corresponding MAC
 * address. (ie. The packet must be sent to this MAC address to reach the
 * destination IP address).
 * \param destIp 
 *      32 bit destination IP (in network byte order) for which we want to
 *      resolve the next hop MAC address.
 * \param destMac 
 *      Pointer to the destination MAC address to be resolved by this method.
 *      If this method returns true, the value that this pointer points to will
 *      contain the 6 bytes MAC address corresponding to destIp address or
 *      the gateway MAC address needed to reach destIp.
 *  \return
 *      True means that we have successfully resolved the MAC address and copied
 *      it over to the address destMac points to. False, means that the function
 *      could not resolve the MAC address and presumably the higher level code
 *      will eventually time out and retry.
 */
bool
ArpCache::arpLookup(const uint32_t destIp, MacAddress destMac)
{

    // First see if we have a valid cached translation for this address.
    IpMacMap::iterator mapEntry = ipMacMap.find(destIp);
    if (mapEntry != ipMacMap.end()) {
        memcpy(destMac, mapEntry->second.data(), NetUtil::MAC_ADDR_LEN);
#if TESTING
        sockaddr_in remote;
        remote.sin_addr.s_addr = destIp;
        TEST_LOG("Resolved MAC address for host %s through local cache!",
            inet_ntoa(remote.sin_addr));
#endif
        return true;
    }

    sockaddr destAddress;
    struct sockaddr_in* destAddr =
        reinterpret_cast<sockaddr_in*>(&destAddress);
    destAddr->sin_addr.s_addr = destIp;

    // No valid cached translation was found in the local cache; See if we can
    // get a translation from Kernel's cache.
    if (lookupKernelArpCache(destIp, destMac)) {
        TEST_LOG("Resolved MAC address through kernel calls for host at"
            " %s!", inet_ntoa(destAddr->sin_addr));
        return true;
    }

    // The kernel doesn't seem to have a translation either. From Kernel socket,
    // Send a UDP packets to the destination. This will trigger the ARP module
    // in the Kernel and cause the Kernel to make a new entry in its ARP cache.
    destAddr->sin_family = AF_INET;

    // Could be any port. It doesn't matter which port to choose.
    destAddr->sin_port = HTONS(80);
    sendUdpPkt(&destAddress);
    if (lookupKernelArpCache(destIp, destMac)) {

        // Successfully resolved the MAC address from Kernel cache.
        TEST_LOG("Resolved MAC address through kernel calls and ARP"
            " packets for host at %s!", inet_ntoa(destAddr->sin_addr));
        return true;
    } else {
        LOG(WARNING, "No success in resolving MAC address for host at %s!",
            inet_ntoa(destAddr->sin_addr));
        return false;
    }
}

/**
 * This method looks up MAC address for a destination IP address from Kernel's
 * ARP cache. If the corresponding MAC address is found, it will returns the
 * MAC address and also updates the local ARP cache.
 *
 * \param destIp 
 *      32 bit destination IP (in network byte order) for which we want to
 *      resolve the MAC address.
 * \param destMac 
 *      Pointer to the destination MAC address to be resolved by this method.
 *      If this method returns true, the value that this pointer points to will
 *      contain either the 6 bytes MAC address corresponding to destIp address
 *      or the gateway MAC address needed to reach destIp.
 * \return
 *      True means that we have successfully resolved the MAC address and copied
 *      it over to `destMac' param. False, means that the method was not able
 *      to resolve the MAC address in a timely manner. In some cases retrying
 *      later will resolve the address but in other cases (ie. The destIp is
 *      bogus) the MAC address wont be resolved at all.
 */
bool
ArpCache::lookupKernelArpCache(const uint32_t destIp, MacAddress destMac)
{

    // We first need to figure out if the destIp is on the same subnet as the
    // local machine; If not, we have to resolve gateway IP address since the
    // Kernel ARP cache only describes the MAC addresses of the machines on the
    // same subnet.
    uint32_t gatewayIp = routeTable.getNextHopIp(destIp);
    struct arpreq arpReq;
    memset(&arpReq, 0, sizeof(arpReq));

    struct sockaddr_in *sin =
        reinterpret_cast<sockaddr_in*>(&arpReq.arp_pa);

    sin->sin_family = AF_INET;
    sin->sin_addr.s_addr = gatewayIp;
    snprintf(arpReq.arp_dev, sizeof(arpReq.arp_dev), "%s", this->ifName);

    int atfFlagCompleted = ATF_COM;

    for (int r = 0; r < ARP_RETRIES; r++) {

        if (sys->ioctl(fdArp, SIOCGARP, &arpReq) == -1) {
            LOG(WARNING, "Can't perform ioctl on kernel's ARP Cache for"
                " host at %s!", inet_ntoa(sin->sin_addr));
            return false;
        }

        if (arpReq.arp_flags & atfFlagCompleted) {

            uint8_t* mac =
                reinterpret_cast<uint8_t*>(&arpReq.arp_ha.sa_data[0]);
            memcpy(destMac, mac, NetUtil::MAC_ADDR_LEN);

            // Update the local ARP cache too.
            MacArray& macArray = ipMacMap[destIp];
            memcpy(macArray.data(), destMac, NetUtil::MAC_ADDR_LEN);
            return true;
        } else {

            // If the ioctl failed, we wait for some time and try again.
            LOG(WARNING, "Kernel ARP cache entry for host at %s is in use!"
                " Sleeping for %d us then retry for %dth time",
                inet_ntoa(sin->sin_addr), ARP_WAIT, r+1);
            usleep(ARP_WAIT);
        }
    }
    return false;
}

/**
 * This method sends a dummy UDP packet to destAddress. The content of the
 * packet that is sent by this method is a constant string.
 *
 * \param destAddress
 *      Poniter to the sockaddr object that contains the destinations IP
 *      address.
 */
void
ArpCache::sendUdpPkt(struct sockaddr* destAddress)
{
    string udpPkt = "dummy msg!";
    int sentBytes =
        downCast<int>(sys->sendto(fd, udpPkt.c_str(), udpPkt.length(),
                0, destAddress, sizeof(struct sockaddr)));

    if (sentBytes != downCast<int>(udpPkt.length())) {
        LOG(WARNING, "Tried to send UDP packet with %d bytes but only %d bytes"
                " was sent!", downCast<int>(udpPkt.length()), sentBytes);
    }
}

/**
 * Constructor for RouteTable class. This basically iterates over the Kernel's
 * route table and fills out the internal route table structure of this class
 * based on the information in Kernel's route file.
 *
 * \param ifName
 *      The interface name for which we want to keep the route information
 *      provided in the Kernel's route table.
 * \param routeFile
 *      Name (including the full path) to the file that contains Kernel's route
 *      table. In most Linux ditstros this file is located at /proc/net/route.
 */
ArpCache::RouteTable::RouteTable(const char* ifName, const char* routeFile)
    :routeVector()
{

    //  In layer 3 networking, the Kernel ARP cache only contains the MAC
    //  address of the machines that are on the same subnet as the local
    //  machine.  The purpose of route file in Kernel and also RouteTable class
    //  here is to provide a way to resolve the gateway IP for any arbitrary
    //  destination IP address. Then using the ARP table and the resolved
    //  gateway IP, we can resolve the next hop MAC address in the network for
    //  that destination IP.

    std::ifstream routeStream(routeFile);
    string line;
    std::vector<string> routeVec;
    RouteEntry routeEntry;

    // The Kernel's route file in linux is formatted as below:
    // -------------------------------------------------------------------------
    // Iface  Destination  Gateway  Flags RefCnt Use Metric Mask    MTU Win IRTT
    //
    //  eth2   0064A8C0    00000000  0001   0     0     0  00FFFFFF  0   0    0
    // -------------------------------------------------------------------------
    // First line is the header and the rest of the lines contain the values
    // for different field in the header. The lines provide routing
    // information for different destination networks.
    // All the IP values in the route file are recorded as 32bit values in
    // network byte order like the above example.
    //
    // Terminal command "route -n" in Linux, prints out the Kernel's route file
    // in human readable format as below:
    // ------------------------------------------------------------------------
    //  Destination     Gateway      Genmask         Flags Metric Ref  Use Iface
    //  192.168.100.0   0.0.0.0      255.255.255.0   U     0      0      0  eth2
    // ------------------------------------------------------------------------
    // Please refer to any Linux networking manual for complete explanation of
    // different fields in Kernel's route file.

    // The first line of the routeFile is the header. So we read it out and
    // discard it.
    getline(routeStream, line);

    // Iterate over remaining lines in the routeFile and parse out the
    // parameters in the file and writes the parsed parameters in routeVec.
    while (getline(routeStream, line)) {
        routeVec.clear();
        while (line.size()) {

            // As long as there are still non-empty string words left in the
            // line, parse out that word and push it to the back of routeVec.
            for (size_t pos = 0; pos < line.size(); ++pos) {
                if (isspace(line[pos])) {
                    if (pos == 0) {

                        // Erase the spaces at beginning of the line.
                        line.erase(0, 1);
                    } else {

                        // Push back the word that comes before the current
                        // space.
                        routeVec.push_back(line.substr(0, pos));
                        line.erase(0, pos);
                    }
                    break;
                }

                if (pos == line.size() - 1) {

                    // There is a word at the end of the file with no space
                    // after it. Push that to routeVec too.
                    routeVec.push_back(line);
                    line.erase();
                }
            }
        }

        // The expected number of fields in each line is 11, where the first
        // field is the interface name, the 2nd field is destination network IP,
        // the 3rd field is the gateway IP for that destination IP and the
        // 8th field is the netmask for that destination IP.
        if (routeVec.size() != 11) {
            LOG(WARNING, "The parsed line from routeFile has %zu line however"
            " we expect 11 fields in standard Linux Kernel's routeFile!",
            routeVec.size());
        }

        // If the interface name for the line that we just parsed out from
        // routeFile matches the ifName argument, then we will keep that
        // route entry in our local route table.
        if (strcmp(routeVec[0].c_str(), ifName) == 0) {
            routeEntry.destNetworkIp =
                downCast<uint32_t>(strtoul(routeVec[1].c_str(), NULL, 16));
            routeEntry.gatewayIp =
                downCast<uint32_t>(strtoul(routeVec[2].c_str(), NULL, 16));
            routeEntry.netMask =
                downCast<uint32_t>(strtoul(routeVec[7].c_str(), NULL, 16));
            routeVector.push_back(routeEntry);
        }
    }

    if (!routeVector.size()) {
        LOG(WARNING, "RouteTable contains no entry for %s interface!", ifName);
    }
}

/**
 * This method returns the IP address of a machine on a local subnet where we
 * should send the packet to reach a desired destination. (ie. Either the
 * destination itself or a gateway)
 *
 * \param destIp
 *      32 bit IP address (in network byte order) of the destination that we
 *      want to gateway IP for it.
 * \return
 *      32 bit IP address (in network byte order) of the next hop that the
 *      packet must be sent to in order to reach destIp address. If function
 *      finds a nonzero gateway, then it returns it. Otherwise, it will return
 *      the input argument destIp as the next hop IP address.
 */
uint32_t
ArpCache::RouteTable::getNextHopIp(const uint32_t destIp)
{
    uint32_t mask = 0;
    uint32_t gateway = 0;
    for (size_t i = 0; i < routeVector.size(); ++i) {
        RouteEntry& entry = routeVector[i];

        // Performing longest prefix matching as the matching rule for finding
        // the gateway IP (next hop in the network).
        if ((entry.destNetworkIp & entry.netMask) == (destIp & entry.netMask) &&
                entry.netMask >= mask) {
            mask = entry.netMask;
            gateway = entry.gatewayIp;
        }
    }
    return gateway ? gateway : destIp;
}

} //namespace RAMCloud
