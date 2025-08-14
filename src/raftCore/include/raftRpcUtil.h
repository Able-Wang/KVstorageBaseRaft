#ifndef RAFTRPCUTIL_H
#define RAFTRPCUTIL_H

#include "raftRPC.pb.h"
#include <string>

// 维护当前节点对其他某一个结点的所有rpc发送通信的功能
//  对于一个raft节点来说，对于任意其他的节点都要维护一个rpc连接，即MprpcChannel
class RaftRpcUtil
{
public:
    // 主动调用其他节点的三个方法,可以按照mit6824来调用，但是别的节点调用自己的好像就不行了，要继承protoc提供的service类才行
    // 追加日志=心跳
    bool AppendEntries(raftRpcProtoc::AppendEntriesArgs *args, raftRpcProtoc::AppendEntriesReply *response);
    // 快照
    bool InstallSnapshot(raftRpcProtoc::InstallSnapshotRequest *args, raftRpcProtoc::InstallSnapshotResponse *response);
    // 投票
    bool RequestVote(raftRpcProtoc::RequestVoteArgs *args, raftRpcProtoc::RequestVoteReply *response);
    RaftRpcUtil(std::string ip, short port);
    ~RaftRpcUtil();

private:
    raftRpcProtoc::raftRpc_Stub *stub_;
};

#endif