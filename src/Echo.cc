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

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <getopt.h>

#include "Common.h"
#include "Buffer.h"
#include "OptionParser.h"
#include "ServiceManager.h"
#include "TransportManager.h"

/**
 * \file
 * A simple echo server over FastTransport used for sanity checking.
 */

/**
 * Entry point for the program.  Sets up a server which listens for packets
 * and returns them to the sender.
 *
 * \param argc
 *      The number of command line args.
 * \param argv
 *      An array of length argc containing the command line args.
 */
int
main(int argc, char *argv[])
try
{
    using namespace RAMCloud;

    logger.setLogLevel(TRANSPORT_MODULE, DEBUG);

    OptionParser optionParser(argc, argv);
    transportManager.initialize(optionParser.options.getLocalLocator().c_str());
    ServiceManager manager(NULL);

    while (true) {
        Transport::ServerRpc* rpc = manager.waitForRpc(1);
        if (rpc == NULL)
            continue;
        Buffer::Iterator iter(rpc->recvPayload);
        while (!iter.isDone()) {
            Buffer::Chunk::appendToBuffer(&rpc->replyPayload,
                                          iter.getData(),
                                          iter.getLength());
            // TODO(ongaro): This is unsafe if the Transport discards the
            // received buffer before it is done with the response buffer.
            // I can't think of any real RPCs where this will come up.
            iter.next();
        }
        rpc->sendReply();
    }
    return 0;
} catch (RAMCloud::Exception& e) {
    using namespace RAMCloud;
    LOG(ERROR, "Echo: %s\n", e.str().c_str());
}
