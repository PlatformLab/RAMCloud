/* Copyright (c) 2009-2010 Stanford University
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
 * Header file for Client.cc.
 */

#ifndef RAMCLOUD_CLIENT_H
#define RAMCLOUD_CLIENT_H


#include <stdbool.h>
#include <config.h>

#include <Common.h>

#include <Buffer.h>
#include <Service.h>
#include <Transport.h>
#include <TCPTransport.h>

#include <PerfCounterType.h>
#include <Mark.h>

#if RC_CLIENT_SHARED
struct rc_client_shared; // declared in Client.c
#endif

struct rc_client {
    RAMCloud::Service* serv;
    RAMCloud::Transport* trans;
    rcrpc_perf_counter perf_counter_select;
    uint32_t perf_counter;
#if RC_CLIENT_SHARED
    struct rc_client_shared *shared;
#endif
};

#ifdef __cplusplus
extern "C" {
#endif

int rc_connect(struct rc_client *client,
               const char* serverAddr, int serverPort);
void rc_disconnect(struct rc_client *client);
int rc_ping(struct rc_client *client);
int rc_write(struct rc_client *client, uint64_t table, uint64_t key,
             const struct rcrpc_reject_rules *reject_rules,
             uint64_t *got_version, const char *buf, uint64_t len);
int rc_insert(struct rc_client *client, uint64_t table, const char *buf,
              uint64_t len, uint64_t *key);
int rc_delete(struct rc_client *client, uint64_t table, uint64_t key,
             const struct rcrpc_reject_rules *reject_rules,
             uint64_t *got_version);
int rc_read(struct rc_client *client, uint64_t table, uint64_t key,
            const struct rcrpc_reject_rules *reject_rules,
            uint64_t *got_version, char *buf, uint64_t *len);
int rc_create_table(struct rc_client *client, const char *name);
int rc_open_table(struct rc_client *client, const char *name,
                  uint64_t *table_id);
int rc_drop_table(struct rc_client *client, const char *name);

void rc_select_perf_counter(struct rc_client *client,
                            enum RAMCloud::PerfCounterType counterType,
                            enum RAMCloud::Mark beginMark,
                            enum RAMCloud::Mark endMark);
uint64_t rc_read_perf_counter(struct rc_client *client);

/* These aren't strictly necessary, but they make life easier for
 * foreign languages because they don't have to know how to allocate a
 * structure of the correct size */
struct rc_client *rc_new(void);
void rc_free(struct rc_client *client);
const char* rc_last_error(void);

#ifdef __cplusplus
}
#endif

#endif
