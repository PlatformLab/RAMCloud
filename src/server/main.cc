#include <server/server.h>
#include <shared/net.h>

#define SVRADDR "127.0.0.1"
#define SVRPORT  11111

#define CLNTADDR "127.0.0.1"
#define CLNTPORT 22222

int
main()
{
    RAMCloud::Net *net = new RAMCloud::Net(SVRADDR, SVRPORT,
                                           CLNTADDR, CLNTPORT);
    RAMCloud::Server *server = new RAMCloud::Server(net);

    while (true)
        server->handleRPC();

    delete net;

    return 0;
}
