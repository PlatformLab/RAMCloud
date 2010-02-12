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

#ifndef RAMCLOUD_BACKUPSERVER_H
#define RAMCLOUD_BACKUPSERVER_H

#include <Common.h>

#include <Net.h>
#include <backuprpc.h>
#include <BackupClient.h>

#include <Bitmap.h>

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
    static void FromErrno(BackupException *e, int errn) {
        e->message = strerror(errn);
        e->errNo = errn;
    }
    virtual ~BackupException();
    std::string message;
    int errNo;
};

struct BackupLogIOException : public BackupException {
    explicit BackupLogIOException(int errn) {
        BackupException::FromErrno(this, errn);
    }
    explicit BackupLogIOException(std::string msg) : BackupException(msg) {}
};
struct BackupInvalidRPCOpException : public BackupException {};
struct BackupSegmentOverflowException : public BackupException {};

const uint64_t SEGMENT_FRAMES = SEGMENT_COUNT * 2;
const uint64_t LOG_SPACE = SEGMENT_FRAMES * SEGMENT_SIZE;

const uint64_t INVALID_SEGMENT_NUM = ~(0ull);

class BackupServer : BackupClient {
  public:
    explicit BackupServer();
    explicit BackupServer(Net *netImpl, const char *logPath);
    virtual ~BackupServer();
    void run();
  private:
    void handleHeartbeat(const backup_rpc *req, backup_rpc *resp);
    void handleWrite(const backup_rpc *req, backup_rpc *resp);
    void handleBegin(const backup_rpc *req, backup_rpc *resp);
    void handleCommit(const backup_rpc *req, backup_rpc *resp);
    void handleFree(const backup_rpc *req, backup_rpc *resp);
    void handleGetSegmentList(const backup_rpc *req, backup_rpc *resp);
    void handleRetrieve(const backup_rpc *req, backup_rpc *resp);

    void handleRPC();
    void sendRPC(struct backup_rpc *rpc);
    void recvRPC(struct backup_rpc **rpc);

    virtual void heartbeat() {}
    virtual void writeSegment(uint64_t segNum, uint32_t offset,
                              const void *data, uint32_t len);
    virtual void commitSegment(uint64_t segNum);
    virtual void freeSegment(uint64_t segNum);
    virtual void getSegmentList(uint64_t *list, uint64_t *count);
    virtual size_t getSegmentMetadata(uint64_t segNum,
                                      RecoveryObjectMetadata *list,
                                      size_t maxSize);
    virtual void retrieveSegment(uint64_t segNum, void *buf);

    void flushSegment();
    void extractMetadata(const void *p,
                         RecoveryObjectMetadata *meta);

    void reserveSpace();
    uint64_t frameForSegNum(uint64_t segnum);

    Net *net;
    int logFD;
    char *seg;
    char *unalignedSeg;
    // segment number of the active segment
    uint64_t openSegNum;

    // TODO(stutsman) How much should we really allocate?
    static const uint64_t SEGMENT_FRAMES = SEGMENT_COUNT;
    /**
     * This array, given a segment frame, produces the current segment
     * number that is stored there.
     * SegmentFrame -> SegmentId
     */
    uint64_t segmentAtFrame[SEGMENT_FRAMES];

    Bitmap<SEGMENT_FRAMES> freeMap;

    friend class BackupServerTest;
    DISALLOW_COPY_AND_ASSIGN(BackupServer);
};

} // namespace RAMCloud

#endif
