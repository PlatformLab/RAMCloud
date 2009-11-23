#ifndef RAMCLOUD_BACKUP_BACKUP_H
#define RAMCLOUD_BACKUP_BACKUP_H

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
    BackupLogIOException(int errn) {
        BackupException::FromErrno(*this, errn);
    }
};
struct BackupInvalidRPCOpException : public BackupException {};
struct BackupSegmentOverflowException : public BackupException {};

class BackupServer {
  public:
    explicit BackupServer();
    explicit BackupServer(Net *net_impl, const char *logPath);
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
    friend class BackupTest;
};

} // namespace RAMCloud

#endif
