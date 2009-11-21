#ifndef RAMCLOUD_CLIENT_H
#define RAMCLOUD_CLIENT_H

#include <inttypes.h>
#include <shared/net.h>

namespace RAMCloud {

class Client {
 public:
    virtual ~Client() {}
    virtual void Ping() = 0;
    virtual void Write(uint64_t table, uint64_t key,
                       const char *buf, uint64_t len) = 0;
    virtual void Read(uint64_t table, uint64_t key,
                      char *buf, uint64_t *buf_len) = 0;
    virtual void Insert(uint64_t table,
                        const char *buf,
                        uint64_t len,
                        uint64_t *key) = 0;
    virtual void CreateTable(const char *name) = 0;
    virtual uint64_t OpenTable(const char *name) = 0;
    virtual void DropTable(const char *name) = 0;
};

class DefaultClient : public Client {
 private:
    rc_net *net;
 public:
    explicit DefaultClient();
    DefaultClient(const DefaultClient& client);
    DefaultClient& operator=(const DefaultClient& client);
    ~DefaultClient();
    virtual void Ping();
    virtual void Write(uint64_t table, uint64_t key,
                       const char *buf, uint64_t len);
    virtual void Read(uint64_t table, uint64_t key,
                      char *buf, uint64_t *buf_len);
    virtual void Insert(uint64_t table,
                        const char *buf,
                        uint64_t len,
                        uint64_t *key);
    virtual void CreateTable(const char *name);
    virtual uint64_t OpenTable(const char *name);
    virtual void DropTable(const char *name);
};

}

#endif
