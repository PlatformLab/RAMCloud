/* Copyright (c) 2011 Stanford University
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

#ifndef RAMCLOUD_PCAPFILE_H
#define RAMCLOUD_PCAPFILE_H

#include <iostream>
#include <fstream>

#include "Common.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * A utility to log raw packet data in pcap format for analysis with tcpdump.
 *
 * See libpcap documentation for details of the pcap file format.
 */
class PcapFile {
  public:
    /// See http://www.tcpdump.org/linktypes.html
    enum class LinkType : uint32_t {
        ETHERNET = 1,
        RAW = 101,      // Starts with raw IPv4 or IPv6 header.
        USER0 = 147,    // 15 ids starting from here for custom formats.
    };

    /**
     * See libpcap for information about the the pcap file format and its fields.
     * This header occurs once at the start of the file.
     */
    struct FileHeader {
        FileHeader(const uint32_t maxSavedLength, const LinkType linkType)
            : magic(0xa1b2c3d4)
            , majorVersion(2)
            , minorVersion(4)
            , secsDiffToUtc(0)
            , sigFigures(0)
            , maxSavedLength(maxSavedLength)
            , linkType(static_cast<uint32_t>(linkType))
        {
        }

        uint32_t magic;          /// To determine endianness, always 0xa1b2c3d4.
        uint16_t majorVersion;   /// 2.
        uint16_t minorVersion;   /// 4.
        int32_t secsDiffToUtc;   /// Number of seconds this zone is off UTC.
        uint32_t sigFigures;     /// Always 0.
        uint32_t maxSavedLength; /// Max of savedLength from all PacketHeaders.
        uint32_t linkType;       /// See LinkType.
    };

    /**
     * See libpcap for information about the the pcap file format and its fields.
     * This header occurs once for each logged packet.  In the file it is
     * followed by savedLength bytes of data from the packet.
     */
    struct PacketHeader {
        //struct timeval timeStamp; /// 16 bytes
        uint32_t secs;
        uint32_t usecs;
        uint32_t savedLength;     /// Number of bytes saved from this packet.
        uint32_t packetLength;    /// Number of bytes in original packet.
    };

    explicit PcapFile(const char* filePath,
                      LinkType linkType,
                      uint32_t maxSavedLength = 65535);
    void append(const char* rawPacket, uint32_t length);
    void append(const Buffer& rawPacket);

 private:
  std::ofstream file;            /// The file where packets are logged.
  const uint32_t maxSavedLength; /// Max of savedLength from all PacketHeaders.
};

/**
 * A globally available pcapFile.
 * Construct it for easy packet logging in clients, or using the --pcapFile
 * flag in OptionParser.
 */
extern Tub<PcapFile> pcapFile;

} // namespace RAMCloud

#endif
