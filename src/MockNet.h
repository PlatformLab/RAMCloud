/* Copyright (c) 2010 Stanford University
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

#include <Net.h>

#ifndef RAMCLOUD_MOCKNET_H
#define RAMCLOUD_MOCKNET_H

namespace RAMCloud {

class MockNet : public Net {
  public:
    // TODO(stutsman) only partially thoughtout.  Would like to make the mock
    // dev scriptable
    MockNet(void (*send_cb)(const char *, size_t)) : cb(send_cb) {}
    virtual void Connect() {}
    virtual void Listen() {}
    virtual int Close() { return 0; }
    virtual void Send(const void *buf, size_t len) {
        cb(static_cast<const char*>(buf), len);
    }
    virtual size_t Recv(void **buf) {
        return 0;
    }
    virtual int SendRPC(struct rcrpc_any *msg) {
        Send(msg, msg->header.len);
        return 0;
    }
    virtual int RecvRPC(struct rcrpc_any **msg) {
        return 0;
    }
    virtual ~MockNet() {}
  private:
    void (*cb)(const char *buf, size_t len);
};

} // namespace RAMCloud

#endif
