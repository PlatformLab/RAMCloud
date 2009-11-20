#ifndef RAMCLOUD_SERVER_BACKUP_CLIENT_H
#define RAMCLOUD_SERVER_BACKUP_CLIENT_H

#include <shared/common.h>
#include <server/net.h>

// requires 0x for cstdint
#include <stdint.h>
#include <vector>

namespace RAMCloud {

class BackupClient {
  public:
    explicit BackupClient(Net *net_impl);
    void Heartbeat();
    void Write(char *buf, size_t len);
    void Commit(std::vector<uintptr_t> freed);
  private:
    DISALLOW_COPY_AND_ASSIGN(BackupClient);
    void SendRPC(struct backup_rpc *rpc);
    void RecvRPC(struct backup_rpc **rpc);
    Net *net;
};

} // namespace RAMCloud

#endif
