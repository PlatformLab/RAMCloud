/* Copyright (c) 2009 Stanford University
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

#include <config.h>

#include <backup/backup.h>
#include <shared/net.h>

int
main()
{
    using ::RAMCloud::Net;
    using ::RAMCloud::UDPNet;
    using ::RAMCloud::BackupServer;
    Net *net = new UDPNet(BACKSVRADDR, BACKSVRPORT,
                          BACKCLNTADDR, BACKCLNTPORT);
    BackupServer *server = new BackupServer(net,
                                            BACKUP_LOG_PATH);

    server->Run();

    delete server;
    delete net;

    return 0;
}
