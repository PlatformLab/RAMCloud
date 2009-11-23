#include <config.h>

#include <server/server.h>
#include <shared/net.h>

int
main()
{
    RAMCloud::Net *net = new RAMCloud::Net(SVRADDR, SVRPORT,
                                           CLNTADDR, CLNTPORT);
    RAMCloud::Server *server = new RAMCloud::Server(net);

    server->Run();

    delete net;

    return 0;
}
