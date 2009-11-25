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

#ifndef RAMCLOUD_BACKUP_BACKUP_H
#define RAMCLOUD_BACKUP_BACKUP_H

#include <config.h>

#include <server/net.h>
#include <shared/common.h>
#include <shared/backuprpc.h>

#include <cstring>

#include <string>

namespace RAMCloud {

struct BackupException {
    /// Automatically captures errno and places string in message
    explicit BackupException() : message(""), errNo(0) {}
    explicit BackupException(std::string msg)
            : message(msg), errNo(0) {}
    BackupException(const BackupException &e)
            : message(e.message), errNo(e.errNo) {}
    BackupException &operator=(const BackupException &e) {
        if (&e == this)
            return *this;
        message = e.message;
        errNo = e.errNo;
        return *this;
    }
    static void FromErrno(BackupException &e, int errn) {
        e.message = strerror(errn);
        e.errNo = errn;
    }
    virtual ~BackupException();
    std::string message;
    int errNo;
};

struct BackupLogIOException : public BackupException {
    explicit BackupLogIOException(int errn) {
        BackupException::FromErrno(*this, errn);
    }
    explicit BackupLogIOException(std::string msg) : BackupException(msg) {}
};
struct BackupInvalidRPCOpException : public BackupException {};
struct BackupSegmentOverflowException : public BackupException {};

// size in bits
template <int64_t size>
class FreeBitmap {
  public:
    static uint32_t Words() {
        return (size / 64) + 1;
    }
    explicit FreeBitmap(bool set) {
        memset(&bitmap[0], set ? 0xff : 0x00, size / 8);
    }
    void SetAll() {
        memset(&bitmap[0], 0xff, size / 8);
    }
    void ClearAll() {
        memset(&bitmap[0], 0x00, size / 8);
    }
    void Set(int64_t num) {
        bitmap[num / 64] |= (1lu << (num % 64));
    }
    void Clear(int64_t num) {
        bitmap[num / 64] &= ~(1lu << (num % 64));
    }
    bool Get(int64_t num) {
        return (bitmap[num / 64] & (1lu << (num % 64))) != 0;
    }
    int64_t NextFree(int64_t start) {
        // TODO(stutsman) start ignored for now
        int r;
        for (int i = 0; i < size; i += 64) {
            r = ffsl(bitmap[i / 64]);
            if (r) {
                r = (r - 1) + i;
                if (r >= size)
                    return -1;
                return r;
            }
        }
        return -1;
    }
    void DebugDump() {
        debug_dump64(&bitmap[0], size / 8);
    }
  private:
    // TODO(stutsman) ensure size is a power of 2
    uint64_t bitmap[size / 64 + 1];
    DISALLOW_COPY_AND_ASSIGN(FreeBitmap);
};

enum { SEGMENT_FRAMES = SEGMENT_COUNT * 2 };
enum { LOG_SPACE = SEGMENT_FRAMES * SEGMENT_SIZE };

class BackupServer {
  public:
    explicit BackupServer();
    explicit BackupServer(Net *net_impl, const char *logPath);
    ~BackupServer();
    void Run();
  private:
    void Heartbeat(const backup_rpc *req, backup_rpc *resp);
    void Write(const backup_rpc *req, backup_rpc *resp);
    void Commit(const backup_rpc *req, backup_rpc *resp);

    void HandleRPC();
    void SendRPC(struct backup_rpc *rpc);
    void RecvRPC(struct backup_rpc **rpc);

    void DoWrite(const char *data, uint64_t off, uint64_t len);
    void DoCommit();
    void Flush();

    void ReserveSpace();

    Net *net;
    int log_fd;
    char *seg;

    FreeBitmap<SEGMENT_COUNT * 2> free_map;
    int64_t last_seg_written;

    friend class BackupTest;
    DISALLOW_COPY_AND_ASSIGN(BackupServer);
};

} // namespace RAMCloud

#endif
