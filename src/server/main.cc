#include <server/server.h>
#include <shared/net.h>

int
main()
{
	netinit(1);

	init();

	while (1)
		handlerpc();

	return (0);
}
