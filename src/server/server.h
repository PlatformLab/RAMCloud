#ifndef RAMCLOUD_SERVER_SERVER_H
#define RAMCLOUD_SERVER_SERVER_H

#include <shared/net.h>
#include <shared/rcrpc.h>

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
 private:
    Net *net;
    struct table tables[RC_NUM_TABLES];
 public:
    explicit Server();
    Server(const Server& server);
    Server& operator=(const Server& server);
    ~Server();
    void handleRPC();
};

} // namespace RAMCloud

#endif
