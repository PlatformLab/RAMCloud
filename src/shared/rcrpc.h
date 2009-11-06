#define RCRPC_OK	0
#define RCRPC_PING	1
#define RCRPC_READ100	2
#define RCRPC_READ1000	3
#define RCRPC_WRITE100	4
#define RCRPC_WRITE1000	5

#define RCRPC_OKLEN		8
#define RCRPC_PINGLEN		8
#define RCRPC_READ100LEN	108
#define RCRPC_READ1000LEN	1008
#define RCRPC_WRITE100LEN	108
#define RCRPC_WRITE1000LEN	1008

struct ok {
};

struct ping {
};

struct read100 {
	int key;
	char buf[100];
};

struct read1000 {
	int key;
	char buf[1000];
};

struct write100 {
	int key;
	char buf[100];
};

struct write1000 {
	int key;
	char buf[1000];
};

struct rcrpc {
	uint32_t type;
	uint32_t len;
	union {
		struct ok ok;
		struct ping ping;
		struct read100 read100;
		struct read1000 read1000;
		struct write100 write100;
		struct write1000 write1000;
	};
};
