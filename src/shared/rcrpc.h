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
#define RCRPC_INSERT_REQUEST_LEN        (RCRPC_HEADER_LEN + sizeof(struct rcrpc_insert_request))
#define RCRPC_INSERT_RESPONSE_LEN       (RCRPC_HEADER_LEN + sizeof(struct rcrpc_insert_response))
#define RCRPC_DELETE_REQUEST_LEN        (RCRPC_HEADER_LEN + sizeof(struct rcrpc_delete_request))
#define RCRPC_DELETE_RESPONSE_LEN       (RCRPC_HEADER_LEN + sizeof(struct rcrpc_delete_response))
#define RCRPC_CREATE_TABLE_REQUEST_LEN  (RCRPC_HEADER_LEN + sizeof(struct rcrpc_create_table_request))
#define RCRPC_CREATE_TABLE_RESPONSE_LEN (RCRPC_HEADER_LEN + sizeof(struct rcrpc_create_table_response))
#define RCRPC_OPEN_TABLE_REQUEST_LEN    (RCRPC_HEADER_LEN + sizeof(struct rcrpc_open_table_request))
#define RCRPC_OPEN_TABLE_RESPONSE_LEN   (RCRPC_HEADER_LEN + sizeof(struct rcrpc_open_table_response))
#define RCRPC_DROP_TABLE_REQUEST_LEN    (RCRPC_HEADER_LEN + sizeof(struct rcrpc_drop_table_request))
#define RCRPC_DROP_TABLE_RESPONSE_LEN   (RCRPC_HEADER_LEN + sizeof(struct rcrpc_drop_table_response))

//namespace RAMCloud {

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
    RCRPC_INSERT_REQUEST,
    RCRPC_INSERT_RESPONSE,
    RCRPC_DELETE_REQUEST,
    RCRPC_DELETE_RESPONSE,
    RCRPC_CREATE_TABLE_REQUEST,
    RCRPC_CREATE_TABLE_RESPONSE,
    RCRPC_OPEN_TABLE_REQUEST,
    RCRPC_OPEN_TABLE_RESPONSE,
    RCRPC_DROP_TABLE_REQUEST,
    RCRPC_DROP_TABLE_RESPONSE,
};


struct rcrpc_ping_request {
};

struct rcrpc_ping_response {
};


struct rcrpc_read100_request {
    uint64_t table;
    int key;
};

struct rcrpc_read100_response {
    char buf[100];
};


struct rcrpc_read1000_request {
    uint64_t table;
    int key;
};

struct rcrpc_read1000_response {
    char buf[1000];
};


struct rcrpc_write100_request {
    uint64_t table;
    int key;
    char buf[100];
};

struct rcrpc_write100_response {
};


struct rcrpc_write1000_request {
    uint64_t table;
    int key;
    char buf[1000];
};

struct rcrpc_write1000_response {
};

struct rcrpc_insert_request {
    uint64_t table;
    char buf[100];
};

struct rcrpc_insert_response {
    int key;
};

struct rcrpc_delete_request {
    uint64_t table;
    int key;
};

struct rcrpc_delete_response {
};


struct rcrpc_create_table_request {
    char name[64];
};

struct rcrpc_create_table_response {
};


struct rcrpc_open_table_request {
    char name[64];
};

struct rcrpc_open_table_response {
    uint64_t handle;
};


struct rcrpc_drop_table_request {
    char name[64];
};

struct rcrpc_drop_table_response {
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

        struct rcrpc_insert_request insert_request;
        struct rcrpc_insert_response insert_response;

        struct rcrpc_delete_request delete_request;
        struct rcrpc_delete_response delete_response;

        struct rcrpc_create_table_request create_table_request;
        struct rcrpc_create_table_response create_table_response;

        struct rcrpc_open_table_request open_table_request;
        struct rcrpc_open_table_response open_table_response;

        struct rcrpc_drop_table_request drop_table_request;
        struct rcrpc_drop_table_response drop_table_response;
    };
};

//} // namespace RAMCloud

#endif

