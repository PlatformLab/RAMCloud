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
 * Declarations for the backup server, currently all backup RPC
 * requests are handled by this module including all the heavy lifting
 * to complete the work requested by the RPCs.
 */

#ifndef RAMCLOUD_BACKUPSERVER_H
#define RAMCLOUD_BACKUPSERVER_H

#include "Common.h"
#include "BackupClient.h"
#include "Bitmap.h"
#include "Rpc.h"

namespace RAMCloud {

struct BackupException : public Exception {
    BackupException() : Exception() {}
    explicit BackupException(string msg) : Exception(msg) {}
    explicit BackupException(int errNo) : Exception(errNo) {}
};

struct BackupLogIOException : public BackupException {
    BackupLogIOException() : BackupException() {}
    explicit BackupLogIOException(string msg): BackupException(msg) {}
    explicit BackupLogIOException(int errNo) : BackupException(errNo) {}
};
struct BackupInvalidRPCOpException : public BackupException {
    BackupInvalidRPCOpException() : BackupException() {}
    explicit BackupInvalidRPCOpException(string msg)
        : BackupException(msg) {}
    explicit BackupInvalidRPCOpException(int errNo)
        : BackupException(errNo) {}
};
struct BackupSegmentOverflowException : public BackupException {
    BackupSegmentOverflowException() : BackupException() {}
    explicit BackupSegmentOverflowException(string msg)
        : BackupException(msg) {}
    explicit BackupSegmentOverflowException(int errNo)
        : BackupException(errNo) {}
};

const uint64_t SEGMENT_FRAMES = SEGMENT_COUNT * 2;
const uint64_t LOG_SPACE = SEGMENT_FRAMES * SEGMENT_SIZE;

const uint64_t INVALID_SEGMENT_NUM = ~(0ull);

class BackupServer {
  public:
    explicit BackupServer(const char *logPath, int logOpenFlags = 0);
    ~BackupServer();
    void run();
  private:
    void commitSegment(const BackupCommitRpc::Request* reqHdr,
                       BackupCommitRpc::Response* respHdr,
                       Transport::ServerRpc* rpc);
    void flushSegment();
    void freeSegment(const BackupFreeRpc::Request* reqHdr,
                     BackupFreeRpc::Response* respHdr,
                     Transport::ServerRpc* rpc);
    uint64_t frameForSegmentId(uint64_t segmentId);
    void handleRpc();
    void getRecoveryData(const BackupGetRecoveryDataRpc::Request* reqHdr,
                         BackupGetRecoveryDataRpc::Response* respHdr,
                         Transport::ServerRpc* rpc);
    void openSegment(const BackupOpenRpc::Request* reqHdr,
                     BackupOpenRpc::Response* respHdr,
                     Transport::ServerRpc* rpc);
    void reserveSpace();
    void startReadingData(const BackupStartReadingDataRpc::Request* reqHdr,
                          BackupStartReadingDataRpc::Response* respHdr,
                          Transport::ServerRpc* rpc);
    void writeSegment(const BackupWriteRpc::Request* req,
                      BackupWriteRpc::Response* resp,
                      Transport::ServerRpc* rpc);

    /**
     * Tracks which segment frames are free on disk (i.e. frames that
     * contain live segments
     */
    Bitmap<SEGMENT_FRAMES> freeMap;

    /** A file descriptor for the log file */
    int logFd;

    /** Segment Id of the active segment */
    uint64_t openSegmentId;

    /**
     * The start of the active segment, it is pagesize aligned to
     * support O_DIRECT writes
     */
    char *seg;

    /**
     * This array, given a segment frame, produces the current segment
     * number that is stored there.
     * SegmentFrame -> SegmentId
     */
    uint64_t segmentAtFrame[SEGMENT_FRAMES];

    friend class BackupServerTest;
    DISALLOW_COPY_AND_ASSIGN(BackupServer);
};

} // namespace RAMCloud

#endif
