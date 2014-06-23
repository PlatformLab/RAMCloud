/* Copyright (c) 2011-2014 Stanford University
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

#include <sys/time.h>
#include <cerrno>

#include "Buffer.h"
#include "ShortMacros.h"
#include "PcapFile.h"

namespace RAMCloud {

Tub<PcapFile> pcapFile;

/**
 * Open a file for logging packets for use with pcap based tools like tcpdump.
 *
 * \param filePath
 *      Path of a file to which packet log entries will be appended.
 * \param linkType
 *      What type of data is in the packets that are captured. See LinkType.
 * \param maxSavedLength
 *      Maximum amount of data to store for each packet appended.  Defaults
 *      to 64 kB.
 */
PcapFile::PcapFile(const char* const filePath,
                   const LinkType linkType,
                   const uint32_t maxSavedLength)
    : file(filePath, std::ios::out | std::ios::trunc | std::ios::binary)
    , maxSavedLength(maxSavedLength)
{
    FileHeader fileHeader{maxSavedLength, linkType};
    file.write(reinterpret_cast<const char*>(&fileHeader), sizeof(fileHeader));
}

/**
 * Append a log entry to the pcap file.
 *
 * \param rawPacket
 *      Pointer to the start of the raw packet data (including all headers).
 * \param length
 *      The number of bytes in the packet at \a rawPacket (if \a length <
 *      #maxSavedLength then only #maxSavedLength bytes will be saved).
 */
void
PcapFile::append(const char* const rawPacket, const uint32_t length)
{
    Buffer buffer;
    buffer.appendExternal(rawPacket, length);
    append(buffer);
}

/**
 * Append a log entry to the pcap file.
 *
 * \param rawPacket
 *     A Buffer containing the raw packet data (including all headers).
 */
void
PcapFile::append(const Buffer& rawPacket)
{
    timeval timeStamp{0, 0};
    if (gettimeofday(&timeStamp, NULL)) {
        LOG(WARNING, "Failed to get time while appending to pcap file: %s",
            strerror(errno));
    }
    const uint32_t length = rawPacket.size();
    const uint32_t savedLength =
        length < maxSavedLength ? length : maxSavedLength;
    PacketHeader header{static_cast<uint32_t>(timeStamp.tv_sec),
                        static_cast<uint32_t>(timeStamp.tv_usec),
                        savedLength, length};
    file.write(reinterpret_cast<const char*>(&header), sizeof(header));
    Buffer::Iterator it(&rawPacket);
    uint32_t toTake = savedLength;
    while (toTake && !it.isDone()) {
        const uint32_t fromThisChunk =
            it.getLength() < toTake ? it.getLength() : toTake;
        file.write(static_cast<const char*>(it.getData()), fromThisChunk);
        toTake -= fromThisChunk;
        it.next();
    }
    file.flush();
}

} // namespace RAMCloud
