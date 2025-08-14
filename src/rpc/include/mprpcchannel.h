#ifndef MPRPCCHANNEL_H
#define MPRPCCHANNEL_H

#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <algorithm>
#include <algorithm>
#include <functional>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

// 真正负责发送和接受的前后处理工作 如消息的组织方式，向哪个节点发送等等
class MprpcChannel : public google::protobuf::RpcChannel
{
public:
    // 所有stub代理对象调用rpc方法，统一做rpc方法调用的数据序列化和网络发送
    void CallMethod(const google::protobuf::MethodDescriptor *method,
                    google::protobuf::RpcController *controller, const google::protobuf::Message *request,
                    google::protobuf::Message *response, google::protobuf::Closure *done);

    MprpcChannel(std::string ip, short port, bool connectNow);

private:
    int m_clientFd;
    const std::string m_ip; // 保存ip和端口，如果断了可以尝试重连
    const uint16_t m_port;

    // 连接ip和端口,并设置m_clientFd
    bool newConnect(const char *ip, uint16_t port, std::string *errMsg);
};

#endif