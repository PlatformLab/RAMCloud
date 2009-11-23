#include <config.h>

#include <backup/backup.h>
#include <shared/net.h>

int
main()
{
    RAMCloud::Net *net = new RAMCloud::Net(BACKSVRADDR, BACKSVRPORT,
                                           BACKCLNTADDR, BACKCLNTPORT);
    RAMCloud::BackupServer *server = new RAMCloud::BackupServer(net,
                                                                "/dev/null");

    server->Run();

    delete net;

    return 0;
}
