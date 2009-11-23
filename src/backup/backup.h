#ifndef RAMCLOUD_BACKUP_BACKUP_H
#define RAMCLOUD_BACKUP_BACKUP_H

#include <server/net.h>
#include <shared/common.h>
#include <shared/backuprpc.h>

#include <cerrno>
#include <cstring>

#include <string>

namespace RAMCloud {

struct BackupException {
    /// Automatically captures errno and places string in message
    explicit BackupException()
            : message(""), errNo(0) {
        errNo = errno;
        message = strerror(errNo);
    }
    BackupException(std::string msg)
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
    virtual ~BackupException();
    std::string message;
    int errNo;
};

struct BackupLogIOException : public BackupException {};
struct BackupInvalidRPCOpException : public BackupException {};
struct BackupSegmentOverflowException : public BackupException {
    // no automatic pull-in of errno
    explicit BackupSegmentOverflowException() {
        message = "";
        errNo = 0;
    }
};

class BackupServer {
  public:
    explicit BackupServer();
    explicit BackupServer(Net *net_impl);
    ~BackupServer();
    void Run();
  private:
    DISALLOW_COPY_AND_ASSIGN(BackupServer);
    void Heartbeat(const backup_rpc *req, backup_rpc *resp);
    void Write(const backup_rpc *req, backup_rpc *resp);
    void Commit(const backup_rpc *req, backup_rpc *resp);

    void HandleRPC();
    void SendRPC(struct backup_rpc *rpc);
    void RecvRPC(struct backup_rpc **rpc);

    void DoWrite(const char *data, size_t data_len);
    void DoCommit();
    void Flush();

    Net *net;
    int log_fd;
    char *seg;
    /// Length of data in seg and next free pos in seg
    size_t seg_off;
};

} // namespace RAMCloud

#endif
