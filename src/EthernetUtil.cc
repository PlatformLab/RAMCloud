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

#include <net/if.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "EthernetUtil.h"
#include "Common.h"
#include "ShortMacros.h"
#include "FastTransport.h"

namespace RAMCloud {
namespace EthernetUtil {

/**
 * This method uses IO control commands to find the mac address of an ethernet
 * interface.
 *
 * \param ifName
 *      this is the ethernet interface name as it appears in the output of the
 *      ifconfig command. For example it could be "eth0"
 * \return
 *      The mac address corrsponding to the interface name you give as the
 *      input of this function. The return mac address is a string formatted as
 *      xx:xx:xx:xx:xx:xx
 */
const string
getLocalMac(const char* ifName)
{

    // The way we find mac address is by sending an IO control request of type
    // SIOCGIFHWADDR. The sturct ifreq will hold the response to that request.
    struct ifreq ifRequest;
    size_t if_name_len = strlen(ifName);

    // Check if the length of ifName is not too large
    if (if_name_len < sizeof(ifRequest.ifr_name)) {
        memcpy(ifRequest.ifr_name, ifName, if_name_len + 1);
    } else {
        LOG(ERROR, "The interface name %s is too long!", ifName);
    }

    // For sending IO control request, we need to open a socket. Any type of
    // socket would suffice. If we can't open a socket, we'll die.
    int fd;
    if ((fd = socket(AF_UNIX, SOCK_DGRAM, 0)) == -1) {
        DIE("Can't open socket for doing IO control");
    }

    // Upon the successful return of the IO control, the ifreq struct will hold
    // the mac address.
    if (ioctl(fd, SIOCGIFHWADDR, &ifRequest) == -1) {
        close(fd);
        DIE("IO control request failed. Couldn't find the mac"
            " address of the interface");
    }
    close(fd);
    const unsigned char* mac =
        reinterpret_cast<unsigned char*>(ifRequest.ifr_hwaddr.sa_data);

    return format("%02X:%02X:%02X:%02X:%02X:%02X",
                   mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
}

/**
 * This method uses IO control commands to find the IP address of an ethernet
 * interface.
 *
 * \param ifName
 *      this is the ethernet interface name as it appears in the output of the
 *      ifconfig command. For example it could be "eth0"
 * \return
 *      The IP address corrsponding to the interface name you give as the 
 *      input of this function. The return IP address is string formatted as  
 *      X.X.X.X
 */
const string
getLocalIp(const char* ifName)
{
    // The way we find IP address is by sending an IO control request of type
    // SIOCGIFADDR. The sturct ifreq will hold the response to that request.
    struct ifreq ifRequest;
    size_t if_name_len = strlen(ifName);

    // Check if the length of ifName is not too large
    if (if_name_len < sizeof(ifRequest.ifr_name)) {
        memcpy(ifRequest.ifr_name, ifName, if_name_len + 1);
    } else {
        LOG(ERROR, "The interface name %s is too long!", ifName);
    }

    // For sending IO control request, we need to open a socket of type AF_INET.
    // If we can't open a socket, we'll die.
    int fd;
    if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) == -1) {
        DIE("Can't open socket for doing IO control");
    }

    // Upon the successful return of the IO control, the ifreq struct will hold
    // the IP address.
    if (ioctl(fd, SIOCGIFADDR, &ifRequest) == -1) {
        close(fd);
        DIE("IO control request failed. Couldn't find the mac"
            " address of the interface");
    }
    close(fd);

    struct sockaddr_in* ipaddr = (struct sockaddr_in*)&ifRequest.ifr_addr;
    return format("%s", inet_ntoa(ipaddr->sin_addr));
}

/**
 * This method makes a formatted string object from the ethernet header.
 * A helper method which is used for debugging purposes.
 *
 * \param ethHeader
 *      pointer to the ethernet header. The header fields are assumed to be
 *      in network order.
 * \return
 *      The contents of Ethernet header in formatted string.
 */
const string
ethernetHeaderToStr(const void* ethHeader)
{
    const EthernetHeader* ethHdr =
        reinterpret_cast<const EthernetHeader*>(ethHeader);
    return format("\n ***** Ethernet Header ***** \n"
                  "| destAddr: %02X:%02X:%02X:%02X:%02X:%02X |"
                  " srcAddr: %02X:%02X:%02X:%02X:%02X:%02X | etherType:0x%X",
                   ethHdr->destAddress[0],
                   ethHdr->destAddress[1], ethHdr->destAddress[2],
                   ethHdr->destAddress[3], ethHdr->destAddress[4],
                   ethHdr->destAddress[5], ethHdr->srcAddress[0],
                   ethHdr->srcAddress[1], ethHdr->srcAddress[2],
                   ethHdr->srcAddress[3], ethHdr->srcAddress[4],
                   ethHdr->srcAddress[5], NTOHS(ethHdr->etherType));
}

/**
 * This method makes a formatted string object from the contents of IP header.
 * A helper method that can be used for debugging purposes. 
 *
 * \param ipHeader 
 *      pointer to the IP header. The header fields are assumed to be
 *      in network order.
 * \return
 *      The contents of the IP header in a formatted string.
 */

const string
ipHeaderToStr(const void* ipHeader)
{
    const IpHeader* ipHdr =
        reinterpret_cast<const IpHeader*>(ipHeader);
    uint32_t srcIp = ntohl(ipHdr->ipSrcAddress);
    uint32_t destIp = ntohl(ipHdr->ipDestAddress);
    return format("\n ***** IP Header ***** \n"
                    "|  ver: %d, IHL: %d | tos: %d |total len: %d |\n"
                    "| id: %d | frag offset: %d |\n"
                    "| ttl: %d | Protocol: 0X%02X | Checksum: %d |\n"
                    "| src IP:%d.%d.%d.%d |\n"
                    "| dest IP: %d.%d.%d.%d |\n",
                    (ipHdr->ipIhlVersion) >> 4u, (ipHdr->ipIhlVersion) & 0xf,
                    ipHdr->tos, NTOHS(ipHdr->totalLength), NTOHS(ipHdr->id),
                    NTOHS(ipHdr->fragmentOffset), ipHdr->ttl, ipHdr->ipProtocol,
                    NTOHS(ipHdr->headerChecksum),
                    (srcIp >> 24) & 0xff, (srcIp >> 16) & 0xff,
                    (srcIp >> 8) & 0xff, srcIp & 0xff, (destIp >> 24) & 0xff,
                    (destIp >> 16) & 0xff, (destIp >> 8) & 0xff, destIp & 0xff);
}

/**
 * This method makes a formatted string object from the contents of a UDP header.
 * A helper method that can be used debugging purposes. 
 *
 * \param udpHeader 
 *      pointer to the UDP header. The header fields are assumed to be
 *      in network order.
 * \return
 *      The contents of the UDP header in a formatted string.
 *      
 */
const string
udpHeaderToStr(const void* udpHeader)
{
    const UdpHeader* udpHdr =
        reinterpret_cast<const UdpHeader*>(udpHeader);
    return format("\n ***** UDP Header ***** \n"
                  "| src port: %d | dest port: %d |\n"
                  "| total len: %d | checksum: %d |\n",
                  NTOHS(udpHdr->srcPort), NTOHS(udpHdr->destPort),
                  NTOHS(udpHdr->totalLength), NTOHS(udpHdr->totalChecksum));
}

} //namespace EthernetUtil
} //namespcae RAMCloud
