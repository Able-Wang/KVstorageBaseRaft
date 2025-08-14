#include <iostream>
#include <string>
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "util.h"
#include "rpcheader.pb.h"
#include "mprpcchannel.h"

MprpcChannel::MprpcChannel(std::string ip, short port, bool connectNow) : m_ip(ip), m_port(port)
{
    // 可以允许延迟连接
    if (!connectNow)
    {
        return;
    }
    std::string errMsg;
    bool res = newConnect(ip.c_str(), port, &errMsg);

    // 三次重连
    int tryCount = 3;
    while (!res && tryCount--)
    {
        std::cout << errMsg << std::endl;
        res = newConnect(ip.c_str(), port, &errMsg);
    }
}

// 连接ip和端口,并设置m_clientFd
bool MprpcChannel::newConnect(const char *ip, uint16_t port, std::string *errMsg)
{
    //  TCP通信
    int clientfd = socket(AF_INET, SOCK_STREAM, 0);
    if (-1 == clientfd)
    {
        char errtxt[512] = {0};
        sprintf(errtxt, "create socket error!errno:%d", errno);
        m_clientFd = -1;
        *errMsg = errtxt;
        return false;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(ip);

    if (-1 == connect(clientfd, (struct sockaddr *)&server_addr, sizeof(server_addr)))
    {
        close(clientfd);
        char errtxt[512] = {0};
        sprintf(errtxt, "connect error!!errno:%d", errno);
        m_clientFd = -1;
        *errMsg = errtxt;
        return false;
    }
    m_clientFd = clientfd;
    return true;
}

// 字符流 header_size(4字节)+header_str+args_str
// 所有stub代理对象调用rpc方法，统一做rpc方法调用的数据序列化和网络发送
void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor *method,
                              google::protobuf::RpcController *controller, const google::protobuf::Message *request,
                              google::protobuf::Message *response, google::protobuf::Closure *done)
{
    if (m_clientFd == -1)
    {
        std::string errMsg;
        bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
        if (!rt)
        {
            DPrintf("[func-MprpcChannel::CallMethod]重连接ip：{%s} port{%d}失败", m_ip.c_str(), m_port);
            controller->SetFailed(errMsg);
            return;
        }
        else
        {
            DPrintf("[func-MprpcChannel::CallMethod]连接ip：{%s} port{%d}成功", m_ip.c_str(), m_port);
        }
    }

    const google::protobuf::ServiceDescriptor *sd = method->service();
    std::string service_name = sd->name();
    std::string method_name = method->name();

    uint32_t args_size{};
    std::string args_str;
    if (request->SerializeToString(&args_str))
    {
        args_size = args_str.size();
    }
    else
    {
        controller->SetFailed("serialize request error!");
        return;
    }

    // 发送头
    RPC::RpcHeader rpcHeadr;
    rpcHeadr.set_service_name(service_name);
    rpcHeadr.set_method_name(method_name);
    rpcHeadr.set_args_size(args_size);

    std::string rpc_header_str;
    if (!rpcHeadr.SerializeToString(&rpc_header_str))
    {
        controller->SetFailed("serialize rpc_header error!");
        return;
    }
    // 使用protobuf的CodedOutputStream来构建发送的数据流
    std::string send_rpc_str; // 用来存储最终发送的数据
    {
        // 创建一个StringOutputStream用于写入send_rpc_str
        google::protobuf::io::StringOutputStream string_output(&send_rpc_str);
        google::protobuf::io::CodedOutputStream coded_output(&string_output);

        // 先写入header的长度（变长编码）
        coded_output.WriteVarint32(static_cast<uint32_t>(rpc_header_str.size()));

        // 不需要手动写入header_size，因为上面的WriteVarint32已经包含了header的长度信息
        // 然后写入rpc_header本身
        coded_output.WriteString(rpc_header_str);
    }

    // 最后，将请求参数附加到send_rpc_str后面
    send_rpc_str += args_str;

    // 发送rpc请求
    while (-1 == send(m_clientFd, send_rpc_str.c_str(), send_rpc_str.size(), 0))
    {
        char errtxt[512] = {0};
        sprintf(errtxt, "send error!!errno:%d", errno);
        std::cout << "尝试重新连接，对方ip：" << m_ip << " 对方端口" << m_port << std::endl;
        close(m_clientFd);
        m_clientFd = -1;
        std::string errMsg;
        bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
        if (!rt)
        {
            controller->SetFailed(errMsg);
            return;
        }
    }

    // 接收rpc响应
    char recv_buf[1024] = {0};
    int recv_size = 0;
    if (-1 == (recv_size = recv(m_clientFd, recv_buf, 1024, 0)))
    {
        close(m_clientFd);
        char errtxt[512] = {0};
        sprintf(errtxt, "recv error!!errno:%d", errno);
        controller->SetFailed(errtxt);
        return;
    }

    // 反序列化
    // std::string response_str(recv_buf, 0, recv_size);//response_str遇到recv_buf中"\n‘就结束了
    // if (!response->ParseFromString(response_str))
    if (!response->ParseFromArray(recv_buf, recv_size))
    {
        char errtxt[1050] = {0};
        sprintf(errtxt, "send error!!errno:%s", recv_buf);
        controller->SetFailed(errtxt);
        return;
    }
}