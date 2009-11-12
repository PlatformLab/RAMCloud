#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <shared/rcrpc.h>
#include <shared/net.h>

//#define SVRADDR	"171.66.3.211"
#define SVRADDR	"192.168.25.1"
#define SVRPORT	 11111

//#define CLNTADDR "171.66.3.208"
#define CLNTADDR "192.168.25.2"
#define CLNTPORT 22222

static int is_server;
static int fd;

void
netinit(int is_srv)
{
	struct sockaddr_in sin;

	is_server = is_srv;

	sin.sin_family = AF_INET;
	sin.sin_port = htons(is_server ? SVRPORT : CLNTPORT);
	inet_aton(is_server ? SVRADDR : CLNTADDR, &sin.sin_addr);

	fd = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (fd == -1) {
		perror("socket");
		exit(1);
	}

	if (bind(fd, (struct sockaddr *)&sin, sizeof(sin)) == -1) {
		perror("bind");
		exit(1);
	}
	
}

int
sendrpc(struct rcrpc *rpc)
{
	struct sockaddr_in sin;

	sin.sin_family = AF_INET;
	sin.sin_port = htons(is_server ? CLNTPORT : SVRPORT); 
	inet_aton(is_server ? CLNTADDR : SVRADDR, &sin.sin_addr);

	if (sendto(fd, rpc, rpc->len, 0, (struct sockaddr *)&sin, sizeof(sin)) == -1) {
		perror("sendto");
		exit(1);
	}

	return (0);
}

int
recvrpc(struct rcrpc **rpc)
{
	static char recvbuf[1500];
	struct sockaddr_in sin;
	socklen_t sinlen = sizeof(sin);

	int len = recvfrom(fd, recvbuf, sizeof(recvbuf), 0, (struct sockaddr *)&sin, &sinlen);
	if (len == -1) {
		perror("recvfrom");
		exit(1);
	}
	if (len < 8) {
		fprintf(stderr, "%s: impossibly small rpc received: %d bytes\n", __func__, len);
		exit(1);
	}

	*rpc = (struct rcrpc *)recvbuf;

	return (0);
}
