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

#include "Common.h"
#include "Buffer.h"
#include "TransportManager.h"

/**
 * \file
 * A telnet client.
 */

int
main()
{
    using namespace RAMCloud; // NOLINT

    Transport::SessionRef session(
        transportManager.getSession(SVRADDR, SVRPORT));

    char buf[1024];
    while (fgets(buf, sizeof(buf), stdin) != NULL) {
        Buffer request;
        Buffer response;
        Buffer::Chunk::appendToBuffer(&request, buf,
                                      static_cast<uint32_t>(strlen(buf)));
        session->clientSend(&request, &response)->getReply();

        uint32_t respLen = response.getTotalLength();
        if (respLen >= sizeof(buf))
            return 1;
        buf[respLen] = '\0';
        response.copy(0, respLen, buf);
        fputs(static_cast<char*>(buf), stdout);
    }
    return 0;
};
