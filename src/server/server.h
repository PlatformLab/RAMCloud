#ifndef RAMCLOUD_SERVER_SERVER_H
#define RAMCLOUD_SERVER_SERVER_H

#include <shared/rcrpc.h>

#include <server/backup_client.h>
#include <server/net.h>

#define RC_NUM_TABLES 256

namespace RAMCloud {

struct object {
    chunk_hdr hdr;
    char blob[1000];
};

struct table {
    char name[64];
    int next_key;
    struct object objects[256];
};

class Server {
  public:
    void Ping(const struct rcrpc *req, struct rcrpc *resp);
    void Read(const struct rcrpc *req, struct rcrpc *resp);
    void Write(const struct rcrpc *req, struct rcrpc *resp);
    void InsertKey(const struct rcrpc *req, struct rcrpc *resp);
    void DeleteKey(const struct rcrpc *req, struct rcrpc *resp);
    void CreateTable(const struct rcrpc *req, struct rcrpc *resp);
    void OpenTable(const struct rcrpc *req, struct rcrpc *resp);
    void DropTable(const struct rcrpc *req, struct rcrpc *resp);

    explicit Server(Net *net_impl);
    Server(const Server& server);
    Server& operator=(const Server& server);
    ~Server();
    void Run();
  private:
    void HandleRPC();
    explicit Server();
    Net *net;
    BackupClient *backup;
    struct table tables[RC_NUM_TABLES];
};

} // namespace RAMCloud

#endif
