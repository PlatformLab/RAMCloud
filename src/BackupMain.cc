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

/**
 * \file
 * Provides a way to launch a standalone backup server.
 */

#include <arpa/inet.h>

#include <config.h>

#include <BackupServer.h>
#include <TCPTransport.h>

/**
 * Instantiates a backup server using the configuration information in
 * config.h.  The backup server runs forever awaiting requests from
 * master servers.
 */
int
main()
{
    using ::RAMCloud::BackupServer;
    using ::RAMCloud::Service;
    using ::RAMCloud::TCPTransport;

    Service backupService;
    backupService.setIp(inet_addr(BACKSVRADDR));
    backupService.setPort(BACKSVRPORT);

    TCPTransport trans(BACKCLNTADDR, BACKCLNTPORT);
    BackupServer server(&backupService, &trans, BACKUP_LOG_PATH);

    server.run();

    return 0;
}
