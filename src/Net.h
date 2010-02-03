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

#ifndef RAMCLOUD_NET_H
#define RAMCLOUD_NET_H

#include <config.h>

#include <rcrpc.h>

#if defined(USERSPACE_NET) + defined(UDP_NET) + defined(TCP_NET) != 1
#error "You need exactly one network implementation."
#endif

#if defined(USERSPACE_NET)
#include <net_user.h>
#elif defined(UDP_NET)
#include <net_udp.h>
#elif defined(TCP_NET)
#include <net_tcp.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif
void rc_net_init(struct rc_net *ret,
                 const char *srcaddr, uint16_t srcport,
                 const char *dstaddr, uint16_t dstport);
int rc_net_connect(struct rc_net *net);
int rc_net_listen(struct rc_net *net);
int rc_net_close(struct rc_net *net);
int rc_net_send(struct rc_net *net, void *, size_t);
int rc_net_recv(struct rc_net *net, void **, size_t *);
int rc_net_send_rpc(struct rc_net *net, struct rcrpc_any *);
int rc_net_recv_rpc(struct rc_net *net, struct rcrpc_any **);
#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
namespace RAMCloud {

class NetException {};

class Net {
  public:
    virtual void Connect() = 0;
    virtual void Listen() = 0;
    virtual int Close() = 0;
    virtual void Send(const void *buf, size_t len) = 0;
    virtual size_t Recv(void **buf) = 0;
    virtual int SendRPC(struct rcrpc_any *msg) = 0;
    virtual int RecvRPC(struct rcrpc_any **msg) = 0;
    virtual ~Net() {}
};

class CNet : public Net {
  public:
    CNet(const char* srcaddr, uint16_t srcport,
         const char* dstaddr, uint16_t dstport) :
      net() {
        rc_net_init(&net,
                    const_cast<char*>(srcaddr), srcport,
                    const_cast<char*>(dstaddr), dstport);
    }
    void Connect() {
        int r = rc_net_connect(&net);
        if (r < 0)
            throw NetException();
    }
    void Listen() {
        int r = rc_net_listen(&net);
        if (r < 0)
            throw NetException();
    }
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
    int SendRPC(struct rcrpc_any *msg) {
        return rc_net_send_rpc(&net, msg);
    }
    int RecvRPC(struct rcrpc_any **msg) {
        return rc_net_recv_rpc(&net, msg);
    }
    ~CNet() {}
  private:
    DISALLOW_COPY_AND_ASSIGN(CNet);
    rc_net net;
};

} // namespace RAMCloud
#endif // __cplusplus

#endif
