#ifndef RAMCLOUD_CLIENT_H
#define RAMCLOUD_CLIENT_H

#include <shared/net.h>

namespace RAMCloud {

class Client {
 public:
    virtual ~Client() {}
    virtual void ping() = 0;
    virtual void write100(int key, const char *buf, int len) = 0;
    virtual void read100(int key, char *buf, int len) = 0;
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
    virtual void write100(int key, const char *buf, int len);
    virtual void read100(int key, char *buf, int len);
};

class MockClient : public Client {
 public:
    explicit MockClient() {}
    virtual void ping();
    virtual void write100(int key, const char *buf, int len);
    virtual void read100(int key, char *buf, int len);
};

}

#endif
