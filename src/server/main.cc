#include <server/server.h>
#include <shared/net.h>

int
main()
{
    RAMCloud::Net *net = new RAMCloud::Net(true);
    RAMCloud::Server *server = new RAMCloud::Server(net);

    while (true)
        server->handleRPC();

    delete net;

    return 0;
}
