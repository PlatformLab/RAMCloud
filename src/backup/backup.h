#ifndef RAMCLOUD_BACKUP_BACKUP_H
#define RAMCLOUD_BACKUP_BACKUP_H

#include <server/net.h>
#include <shared/common.h>
#include <shared/backuprpc.h>

namespace RAMCloud {

/* What do I need to know?  I don't care much about what's in
 * segments, but is the server going to repack these oddly?  Maybe I
 * don't care - I'm not even trying to mimck its layout exactly ---
 * do I really even need to keep fix segment sizes?  Could be helpful
 * on flash at least */

class BackupServer {
  public:
    explicit BackupServer();
    explicit BackupServer(Net *net_impl);
    void Run();
  private:
    DISALLOW_COPY_AND_ASSIGN(BackupServer);
    void Heartbeat(const backup_rpc *req, backup_rpc *resp);
    void Write(const backup_rpc *req, backup_rpc *resp);
    void Commit(const backup_rpc *req, backup_rpc *resp);
    void HandleRPC();
    void SendRPC(struct backup_rpc *rpc);
    void RecvRPC(struct backup_rpc **rpc);
    Net *net;
};

} // namespace RAMCloud

#endif
