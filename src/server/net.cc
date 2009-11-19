#include <server/net.h>

UDPNet::UDPNet(bool is_server) : net(rc_udp_net())
{
    rc_udp_net_init(&net, is_server);
}

UDPNet::~UDPNet()
{
    Close();
}

void
UDPNet::Connect()
{
    net.net.connect((rc_net*)&net);
}

int
UDPNet::Close()
{
    return net.net.close((rc_net*)&net);
}

int
UDPNet::IsServer()
{
    return net.net.is_server((rc_net*)&net);
}

int
UDPNet::IsConnected()
{
    return net.net.is_connected((rc_net*)&net);
}

int
UDPNet::SendRPC(struct rcrpc *msg)
{
    return net.net.send_rpc((rc_net*)&net, msg);
}

int
UDPNet::RecvRPC(struct rcrpc **msg)
{
    return net.net.recv_rpc((rc_net*)&net, msg);
}


