#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>

#include <shared/rcrpc.h>
#include <shared/net.h>

static uint64_t
rdtsc()
{
        uint32_t lo, hi;

#ifdef __GNUC__
        __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
#else
        asm("rdtsc" : "=a" (lo), "=d" (hi));
#endif

        return (((uint64_t)hi << 32) | lo);
}

void
ping()
{
	struct rcrpc query, *resp;

/*** DO A PING ***/

	query.type = RCRPC_PING; 
	query.len  = RCRPC_PINGLEN;

	//printf("sending ping rpc...\n");
	sendrpc(&query);

	recvrpc(&resp);
	//printf("received reply, type = 0x%08x\n", resp->type);
}

void
write100(int key, char *buf, int len)
{
	struct rcrpc query, *resp;

	//printf("writing 100\n");
	memset(query.write100.buf, 0, sizeof(query.write100.buf));
	memcpy(query.write100.buf, buf, len);
	query.type = RCRPC_WRITE100;
	query.len  = RCRPC_WRITE100LEN;
	query.write100.key = key;
	sendrpc(&query);
	recvrpc(&resp);
	//printf("write100 got reply: 0x%08x\n", resp->type);
}

void
read100(int key, char *buf, int len)
{
	struct rcrpc query, *resp;

	//printf("read100\n");
	query.type = RCRPC_READ100;
	query.len  = RCRPC_READ100LEN;
	query.read100.key = key;
	sendrpc(&query);
	recvrpc(&resp);
	//printf("read back [%s]\n", resp->read100.buf); 
	memcpy(buf, resp->read100.buf, len);	
}

int
main()
{
	uint64_t b;

	netinit(0);

	b = rdtsc();
	ping();
	//printf("ping took %" PRIu64 " ticks\n", rdtsc() - b);

	b = rdtsc();
	write100(42, "Hello, World!", 14);
	//printf("write100 took %" PRIu64 " ticks\n", rdtsc() - b);

	char buf[100];
	b = rdtsc();
	read100(42, buf, 100); 
	//printf("read100 took %" PRIu64 " ticks\n", rdtsc() - b);
	//printf("Got back [%s]\n", buf);

	return (0);
}
