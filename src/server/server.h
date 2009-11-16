#ifndef RAMCLOUD_SERVER_SERVER_H
#define RAMCLOUD_SERVER_SERVER_H

#include <shared/net.h>
#include <shared/rcrpc.h>

namespace RAMCloud {

struct object {
    char blob[1000];
};


class Server {
 private:
    Net *net;
    struct object objects[256];
 public:
    explicit Server();
    Server(const Server& server);
    Server& operator=(const Server& server);
    ~Server();
    void handleRPC();
};

} // namespace RAMCloud

#endif
