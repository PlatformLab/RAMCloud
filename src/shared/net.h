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

#ifndef RAMCLOUD_SHARED_NET_H
#define RAMCLOUD_SHARED_NET_H

#include <string.h>

#include <shared/rcrpc.h>

#ifdef USERSPACE_NET
#include <shared/net_user.h>
#else
#include <shared/net_udp.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif
void rc_net_init(struct rc_net *ret,
                 const char *srcaddr, uint16_t srcport,
                 const char *dstaddr, uint16_t dstport);
int rc_net_connect(struct rc_net *net);
int rc_net_close(struct rc_net *net);
int rc_net_send(struct rc_net *net, void *, size_t);
int rc_net_recv(struct rc_net *net, void **, size_t *);
int rc_net_send_rpc(struct rc_net *net, struct rcrpc *);
int rc_net_recv_rpc(struct rc_net *net, struct rcrpc **);
#ifdef __cplusplus
}
#endif

#endif
