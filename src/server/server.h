#ifndef RAMCLOUD_SERVER_SERVER_H
#define RAMCLOUD_SERVER_SERVER_H

#include <shared/rcrpc.h>

#include <server/net.h>

#define RC_NUM_TABLES 256

namespace RAMCloud {

struct object {
    char blob[1000];
};

struct table {
    char name[64];
    int next_key;
    struct object objects[256];
};

class Server {
  public:
    void ping(const struct rcrpc *req, struct rcrpc *resp);
    void read100(const struct rcrpc *req, struct rcrpc *resp);
    void read1000(const struct rcrpc *req, struct rcrpc *resp);
    void write100(const struct rcrpc *req, struct rcrpc *resp);
    void write1000(const struct rcrpc *req, struct rcrpc *resp);
    void insertKey(const struct rcrpc *req, struct rcrpc *resp);
    void deleteKey(const struct rcrpc *req, struct rcrpc *resp);
    void createTable(const struct rcrpc *req, struct rcrpc *resp);
    void openTable(const struct rcrpc *req, struct rcrpc *resp);
    void dropTable(const struct rcrpc *req, struct rcrpc *resp);

    explicit Server(Net *net_impl);
    Server(const Server& server);
    Server& operator=(const Server& server);
    ~Server();
    void handleRPC();
  private:
    explicit Server();
    Net *net;
    struct table tables[RC_NUM_TABLES];
};

} // namespace RAMCloud

#endif
