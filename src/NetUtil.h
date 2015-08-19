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

#include "Common.h"
#include "Syscall.h"

#ifndef RAMCLOUD_NETUTIL_H
#define RAMCLOUD_NETUTIL_H

/**
 * \file 
 *      This header file contains data structure and function definitions that
 *      could potentially be shared between all of the layer 2 and layer 3
 *      network drivers.  The scope of this file is limited to NetUtil because
 *      member structs should only be used inside driver software specific to
 *      Ethernet, IP, UDP, etc.
 */
namespace RAMCloud {

namespace NetUtil {

// This enum define various ethernet payload types as it must be specified
// in EthernetHeader field `etherType'.
enum EthPayloadType {
    IP_V4 = 0x0800, // Standard ethernet type when the payload is an ip packet.
#ifdef DPDK
    FAST  = 0x88b5  // FAST+DPDK
#endif
};

// Standard size of an Ethernet frame (not including Ethernet header).
static const uint32_t ETHERNET_MAX_DATA_LEN = 1500;

/// The MAC address length in bytes.
static const int MAC_ADDR_LEN = 6;

/// Definining of MAC address as an array of lenght MAC_ADDR_LEN.
typedef uint8_t MacAddress[MAC_ADDR_LEN];

/**
 * Standard Ethernet Header structure. 
 */
struct EthernetHeader {
    MacAddress destAddress; // Destination MAC address.
    MacAddress srcAddress;  // Source MAC address.
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

/**
 * A helper class that defines a syscall object for doing system calls. It
 * extends the scope of the sycall object beyond the compilation units and at
 * the same time limits the scope to the NetUtil namespace. This extended scope
 * is requiered for unit testsing.
 */
class SysCallWrapper {
  public:
    SysCallWrapper()
    {}

    /// Static sys call object that is used to invoke all system calls in
    /// NetUtil namespace in normal production code. In tests mode, this will
    /// point to a `MockSyscall' object.
    static Syscall* sys;
};

const string getLocalMac(const char* ifName);
const string getLocalIp(const char* ifName);
const string ethernetHeaderToStr(const void* ethHeader);
const string ipHeaderToStr(const void* ipHeader);
const string udpHeaderToStr(const void* udpHeader);
const string macToStr(const uint8_t* mac);

} //namespace NetUtil

} //namespace RAMCloud

#endif
