#ifndef RAMCLOUD_CLIENT_H
#define RAMCLOUD_CLIENT_H

#include <inttypes.h>
#include <shared/net.h>

namespace RAMCloud {

class Client {
 public:
    virtual ~Client() {}
    virtual void ping() = 0;
    virtual void write100(uint64_t table, int key, const char *buf, int len) = 0;
    virtual void read100(uint64_t table, int key, char *buf, int len) = 0;
    virtual void create_table(const char *name) = 0;
    virtual uint64_t open_table(const char *name) = 0;
    virtual void drop_table(const char *name) = 0;
};

class DefaultClient : public Client {
 private:
    Net *net;
 public:
    explicit DefaultClient();
    DefaultClient(const DefaultClient& client);
    DefaultClient& operator=(const DefaultClient& client);
    ~DefaultClient();
    virtual void ping();
    virtual void write100(uint64_t table, int key, const char *buf, int len);
    virtual void read100(uint64_t table, int key, char *buf, int len);
    virtual void create_table(const char *name);
    virtual uint64_t open_table(const char *name);
    virtual void drop_table(const char *name);
};

class MockClient : public Client {
 public:
    explicit MockClient() {}
    virtual void ping();
    virtual void write100(uint64_t table, int key, const char *buf, int len);
    virtual void read100(uint64_t table, int key, char *buf, int len);
    virtual void create_table(const char *name);
    virtual uint64_t open_table(const char *name);
    virtual void drop_table(const char *name);
};

}

#endif
