#ifndef RAMCLOUD_SHARED_RCRPC_H
#define RAMCLOUD_SHARED_RCRPC_H

// #include <cinttypes> // this requires c++0x support because it's c99
// so we'll go ahead and use the C header
#include <inttypes.h>

#define RCRPC_HEADER_LEN                ((size_t) &(((struct rcrpc *) 0)->ping_request))
#define RCRPC_PING_REQUEST_LEN          (RCRPC_HEADER_LEN + sizeof(struct rcrpc_ping_request))
#define RCRPC_PING_RESPONSE_LEN         (RCRPC_HEADER_LEN + sizeof(struct rcrpc_ping_response))
#define RCRPC_READ100_REQUEST_LEN       (RCRPC_HEADER_LEN + sizeof(struct rcrpc_read100_request))
#define RCRPC_READ100_RESPONSE_LEN      (RCRPC_HEADER_LEN + sizeof(struct rcrpc_read100_response))
#define RCRPC_READ1000_REQUEST_LEN      (RCRPC_HEADER_LEN + sizeof(struct rcrpc_read1000_request))
#define RCRPC_READ1000_RESPONSE_LEN     (RCRPC_HEADER_LEN + sizeof(struct rcrpc_read1000_response))
#define RCRPC_WRITE100_REQUEST_LEN      (RCRPC_HEADER_LEN + sizeof(struct rcrpc_write100_request))
#define RCRPC_WRITE100_RESPONSE_LEN     (RCRPC_HEADER_LEN + sizeof(struct rcrpc_write100_response))
#define RCRPC_WRITE1000_REQUEST_LEN     (RCRPC_HEADER_LEN + sizeof(struct rcrpc_write1000_request))
#define RCRPC_WRITE1000_RESPONSE_LEN    (RCRPC_HEADER_LEN + sizeof(struct rcrpc_write1000_response))

namespace RAMCloud {

enum RCRPC_TYPE {
    RCRPC_PING_REQUEST,
    RCRPC_PING_RESPONSE,
    RCRPC_READ100_REQUEST,
    RCRPC_READ100_RESPONSE,
    RCRPC_READ1000_REQUEST,
    RCRPC_READ1000_RESPONSE,
    RCRPC_WRITE100_REQUEST,
    RCRPC_WRITE100_RESPONSE,
    RCRPC_WRITE1000_REQUEST,
    RCRPC_WRITE1000_RESPONSE,
};


struct rcrpc_ping_request {
};

struct rcrpc_ping_response {
};


struct rcrpc_read100_request {
    int key;
};

struct rcrpc_read100_response {
    int key;
    char buf[100];
};


struct rcrpc_read1000_request {
    int key;
};

struct rcrpc_read1000_response {
    int key;
    char buf[1000];
};


struct rcrpc_write100_request {
    int key;
    char buf[100];
};

struct rcrpc_write100_response {
};


struct rcrpc_write1000_request {
    int key;
    char buf[1000];
};

struct rcrpc_write1000_response {
};


struct rcrpc {
    uint32_t type;
    uint32_t len;
    union {
        struct rcrpc_ping_request ping_request;
        struct rcrpc_ping_response ping_response;

        struct rcrpc_read100_request read100_request;
        struct rcrpc_read100_response read100_response;

        struct rcrpc_read1000_request read1000_request;
        struct rcrpc_read1000_response read1000_response;

        struct rcrpc_write100_request write100_request;
        struct rcrpc_write100_response write100_response;

        struct rcrpc_write1000_request write1000_request;
        struct rcrpc_write1000_response write1000_response;
    };
};

} // namespace RAMCloud

#endif

