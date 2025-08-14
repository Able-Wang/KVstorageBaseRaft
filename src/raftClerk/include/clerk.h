#ifndef RAFTCLERK_H
#define RAFTCLERK_H

#include <arpa/inet.h>
#include <netinet/in.h>
#include <raftServerRpcUtil.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <string>
#include <vector>
#include "kvServerRPC.pb.h"
#include "mprpcconfig.h"

class Clerk
{
private:
    std::vector<std::shared_ptr<raftServerRpcUtil>> m_servers; // 保存所有raft节点的fd
    std::string m_clientId;
    int m_requestId;
    int m_recentLeaderId; // 只是有可能是领导

    std::string Uuid()
    {
        // 用于返回随机的clientId
        return std::to_string(rand()) + std::to_string(rand()) + std::to_string(rand()) + std::to_string(rand());
    }

    //    MakeClerk
    void PutAppend(std::string key, std::string value, std::string op);

public:
    // 对外暴露的三个功能和初始化
    void Init(std::string configFileName);
    std::string Get(std::string key);
    void Put(std::string key, std::string value);
    void Append(std::string key, std::string value);

public:
    Clerk();
};

#endif