#include <server/server.h>
#include <shared/net.h>

int
main()
{
    RAMCloud::Server *server = new RAMCloud::Server();

    while (1)
        server->handleRPC();

    return (0);
}
