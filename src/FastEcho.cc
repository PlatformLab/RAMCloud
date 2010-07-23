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

#include <Common.h>
#include <Buffer.h>
#include <TCPTransport.h>

#include <Driver.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/**
 * \file
 * An echo server over FastTransport.
 */

int
main()
try
{
    using namespace RAMCloud; // NOLINT

    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(12121);
    const char *ip = "127.0.0.1";
    if (inet_aton(ip, &addr.sin_addr) == 0)
        throw Exception("inet_aton failed");

    UDPDriver d(reinterpret_cast<const sockaddr *>(&addr),
                static_cast<socklen_t>(sizeof(addr)));

    sockaddr_in dstaddr;
    dstaddr.sin_family = AF_INET;
    dstaddr.sin_port = htons(12122);
    const char *dstip = "127.0.0.1";
    if (inet_aton(dstip, &dstaddr.sin_addr) == 0)
        throw Exception("inet_aton failed");

    Buffer buffer;
    char *msg = new(&buffer, APPEND) char[100];
    memset(msg, '\0', 100);
    strcpy(msg, "God hates ponies\n"); // NOLINT
    msg = new(&buffer, APPEND) char[100];
    memset(msg, '\0', 100);
    strcpy(msg, "I hates ponies also\n"); // NOLINT
    Buffer::Iterator iter(buffer);

    d.sendPacket(reinterpret_cast<const sockaddr *>(&dstaddr),
                 sizeof(dstaddr), NULL, 0, &iter);

    {
        UDPDriver::Received recvd;
        while (!d.tryRecvPacket(&recvd));
        printf("Recvd: %s\n", recvd.payload);
    }

    return 0;
} catch (RAMCloud::Exception e) {
    fprintf(stderr, "FastEcho: %s\n", e.message.c_str());
}
