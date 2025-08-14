#include "raftRpcUtil.h"

#include "mprpcchannel.h"
#include "mprpccontroller.h"

RaftRpcUtil::RaftRpcUtil(std::string ip, short port)
{
    stub_ = new raftRpcProtoc::raftRpc_Stub(new MprpcChannel(ip, port, true));
}

RaftRpcUtil::~RaftRpcUtil()
{
    if (stub_ != NULL)
        delete stub_;
}

// 追加日志=心跳
bool RaftRpcUtil::AppendEntries(raftRpcProtoc::AppendEntriesArgs *args, raftRpcProtoc::AppendEntriesReply *response)
{
    MprpcController controller;
    stub_->AppendEntries(&controller, args, response, nullptr);
    return !controller.Failed();
}

// 快照
bool RaftRpcUtil::InstallSnapshot(raftRpcProtoc::InstallSnapshotRequest *args, raftRpcProtoc::InstallSnapshotResponse *response)
{
    MprpcController controller;
    stub_->InstallSnapshot(&controller, args, response, nullptr);
    return !controller.Failed();
}

// 投票
bool RaftRpcUtil::RequestVote(raftRpcProtoc::RequestVoteArgs *args, raftRpcProtoc::RequestVoteReply *response)
{
    MprpcController controller;
    stub_->RequestVote(&controller, args, response, nullptr);
    return !controller.Failed();
}