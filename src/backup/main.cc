#include <backup/backup.h>
#include <shared/net.h>

#define BACKSVRADDR "127.0.0.1"
#define BACKSVRPORT  33333

#define BACKCLNTADDR "127.0.0.1"
#define BACKCLNTPORT  44444

int
main()
{
    RAMCloud::Net *net = new RAMCloud::Net(BACKSVRADDR, BACKSVRPORT,
                                           BACKCLNTADDR, BACKCLNTPORT);
    RAMCloud::BackupServer *server = new RAMCloud::BackupServer(net);

    server->Run();

    delete net;

    return 0;
}
