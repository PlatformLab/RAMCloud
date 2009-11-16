#ifndef RAMCLOUD_SHARED_NET_H
#define RAMCLOUD_SHARED_NET_H

namespace RAMCloud {

class Net {
 public:
    virtual ~Net() {}
    virtual bool isServer() = 0;
    virtual int sendRPC(struct rcrpc *) = 0;
    virtual int recvRPC(struct rcrpc **) = 0;
};

class DefaultNet : public Net {
 private:
    bool isServer_;
    bool connected;
    int fd;
    void connect();
 public:
    DefaultNet(bool isServer) : isServer_(isServer), connected(false), fd(-1) {};
    virtual ~DefaultNet() {}
    int sendRPC(struct rcrpc *);
    int recvRPC(struct rcrpc **);
    bool isServer() { return isServer_; }
};

} // namespace RAMCloud

#endif
