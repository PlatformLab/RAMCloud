#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>

#include <shared/rcrpc.h>
#include <shared/net.h>

#include <server/server.h>

struct rcrpc blobs[256];

void
handlerpc()
{
	struct rcrpc *rcrpc;
	struct rcrpc *resp;
	struct rcrpc buf;

	if (recvrpc(&rcrpc) == 0) {
		//printf("got rpc type: 0x%08x, len 0x%08x\n", rcrpc->type, rcrpc->len);	

		switch (rcrpc->type) {
		case RCRPC_PING:
			resp = &buf;
			resp->type = RCRPC_PING;
			resp->len  = RCRPC_PINGLEN;
			break;

		case RCRPC_READ100:
			//printf("read100 from key %d\n", rcrpc->read100.key);
			resp = &blobs[rcrpc->read100.key];
			resp->type = RCRPC_READ1000;
			resp->len  = RCRPC_READ1000LEN;
			//printf("resp key: %d\n", resp->read100.key);
			break;
				
		case RCRPC_READ1000:
			resp = &blobs[rcrpc->read1000.key];
			resp->type = RCRPC_READ1000;
			resp->len  = RCRPC_READ1000LEN;
			break;

		case RCRPC_WRITE100:
			//printf("write100 to key %d, val = %s\n", rcrpc->write100.key, rcrpc->write100.buf);
			memcpy(&blobs[rcrpc->write100.key], rcrpc, RCRPC_WRITE100LEN);
			//printf("post copy key: %d\n", blobs[rcrpc->write100.key].write100.key);
			resp = &buf;
			resp->type = RCRPC_OK;
			resp->len  = RCRPC_OKLEN;
			break;

		case RCRPC_WRITE1000:
			memcpy(&blobs[rcrpc->write1000.key], rcrpc, RCRPC_WRITE1000LEN);
			resp = &buf;
			resp->type = RCRPC_OK;
			resp->len  = RCRPC_OKLEN;
			break;

		default:
			//printf(stderr, "received unknown RPC type 0x%08x\n", rcrpc->type);
			exit(1);
		}
	}

	sendrpc(resp);
}

void
init()
{
	memset(blobs, 0, sizeof(blobs));
}

