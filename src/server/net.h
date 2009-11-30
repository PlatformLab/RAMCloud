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

#ifndef RAMCLOUD_SERVER_NET_H
#define RAMCLOUD_SERVER_NET_H

#include <shared/common.h>
#include <shared/net.h>

namespace RAMCloud {

class NetException {};

class Net {
  public:
    Net(const char* srcaddr, uint16_t srcport,
        const char* dstaddr, uint16_t dstport)
            : net(rc_net()) {
        rc_net_init(&net,
                    const_cast<char*>(srcaddr), srcport,
                    const_cast<char*>(dstaddr), dstport);
        Connect();
    }
    void Connect() { rc_net_connect(&net); }
    int Close() { return rc_net_close(&net); }
    void Send(const void *buf, size_t len) {
        // const ok - C code doesn't modify but can't accept const
        int r = rc_net_send(&net, const_cast<void *>(buf), len);
        if (r < 0)
            throw NetException();
    }
    size_t Recv(void **buf) {
        size_t len;
        int r = rc_net_recv(&net, buf, &len);
        if (r < 0)
            throw NetException();
        return len;
    }
    int SendRPC(struct rcrpc *msg) {
        return rc_net_send_rpc(&net, msg);
    }
    int RecvRPC(struct rcrpc **msg) {
        return rc_net_recv_rpc(&net, msg);
    }
    ~Net() {}
  private:
    DISALLOW_COPY_AND_ASSIGN(Net);
    rc_net net;
};

} // namespace RAMCloud

#endif
