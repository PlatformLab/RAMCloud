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

#include "Common.h"

#ifndef RAMCLOUD_ETHERNETUTIL_H
#define RAMCLOUD_ETHERNETUTIL_H

/**
 * \file 
 * This header file contains Header structs that could be potetially shared 
 * between all of the ethernet drivers. The scope of this file is limitted to 
 * EtherUtil because member structs should only be used inside driver software
 * specific to ethernet.
 */
namespace RAMCloud {

namespace EthernetUtil {

// This enum define various ethernet payload types as it must be specified
// in EthernetHeader field etherType.
enum EthernetType {
    IP_V4 = 0x0800 // Standard ethernet type when the payload is an ip packet.
};

/**
 * Standard Ethernet Header structure. 
 */
struct EthernetHeader {
    uint8_t destAddress[6]; // Destination MAC address.
    uint8_t srcAddress[6];  // Source MAC address.
    uint16_t etherType;     // The payload type of the ethernet frame which
                            // follows right after this header.
}__attribute__((packed));

/**
 * Standard IP version 4 Header structure.
 */
struct IpHeader {
    uint8_t ipIhlVersion;   // Bits 0 to 3 contain size, in 32 bit words, of IP
                            // header and bits 4 to 7 contain IP version.
    uint8_t tos;            // Type of service of this IP packet.
    uint16_t totalLength;   // Total IP packet size including both header
                            // and payload.
    uint16_t id;            // Unique identifer for the group of fragments of
                            // a single IP datagram
    uint16_t fragmentOffset;// Offset of this fragment relative to the beginnig
                            // of unfragmented IP datagram.
    uint8_t ttl;            // Time ot live.
    uint8_t ipProtocol;     // Defines the protocol used in the data portion
                            // of the IP datagram
    uint16_t headerChecksum;// Checksum value calculatd over the header of IP
                            // datagram.
    uint32_t ipSrcAddress;  // IP address of the source of this packet.
    uint32_t ipDestAddress; // IP address of the destination of this packet.
    /*...options...*/
}__attribute__((packed));

/**
 * Standard UDP Header structure.
 */
struct UdpHeader {
    uint16_t srcPort;       // UDP port number for the sender of this packet.
    uint16_t destPort;      // UDP port number for the receiver of this packet.
    uint16_t totalLength;   // The length in byte of UDP header and payload.
    uint16_t totalChecksum; // Checksum calculated over both UDP header and
                            // payload.
}__attribute__((packed));

const string getLocalMac(const char* ifName);
const string getLocalIp(const char* ifName);
const string ethernetHeaderToStr(const void* ethHeader);
const string ipHeaderToStr(const void* ipHeader);
const string udpHeaderToStr(const void* udpHeader);

} //namespace EthernetUtil

} //namespace RAMCloud

#endif
