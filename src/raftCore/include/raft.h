#ifndef RAFT_H
#define RAFT_H

#include <mutex>
#include <vector>
#include <memory>
#include <chrono>
#include <string>
#include <thread>
#include <cmath>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include "boost/serialization/serialization.hpp"
#include <google/protobuf/service.h>
#include "raftRPC.pb.h"
#include "util.h"
#include "monsoon.h"
#include "ApplyMsg.h"
#include "raftRpcUtil.h"
#include "Persister.h"

// 方便网络分区时候debug,网络异常未Disconnected，网络正常AppNormal，防止matchIndex[]数组异常减小
constexpr int Disconnected = 0;
constexpr int AppNormal = 1;

// 投票状态
constexpr int Killed = 0;
constexpr int Voted = 1;  // 本轮已投票
constexpr int Expire = 2; // 投票(消息、竞选者)已经过期
constexpr int Normal = 3;

class Raft : public raftRpcProtoc::raftRpc
{
private:
    std::mutex m_mtx;
    std::vector<std::shared_ptr<RaftRpcUtil>> m_peers; // 与其他节点通信的rpc入口
    std::shared_ptr<Persister> m_persister;            // 负责将raft数据持久化
    int m_me;                                          // raft以集群启动，用于标识自己编号
    int m_currentTerm;                                 // 记录当前任期
    int m_VoteFor;                                     // 记录当前任期给谁投过票
    std::vector<raftRpcProtoc::LogEntry> m_logs;       // 日志条目数据，包含状态机要执行的指令集，以及收到leader时任期号

    // 所有节点都在维护，易丢失
    int m_commitIndex; // 提交的日志Index
    int m_lastApplied; // 已经汇报给状态机log的index

    // 由leader维护
    std::vector<int> m_nextIndex;  // 下一个要发给从者的节点索引
    std::vector<int> m_matchIndex; // 从者返回给leader已经收到了多少日志条目索引

    enum Status
    {
        Follower,
        Candidate,
        Leader
    };
    Status m_status;                                // 当前身份
    std::shared_ptr<LockQueue<ApplyMsg>> applyChan; // client从这里获取日志，client与raft通信接口

    std::chrono::_V2::system_clock::time_point m_lastResetElectionTime;  // 选举超时时间
    std::chrono::_V2::system_clock::time_point m_lastResetHeartBeatTime; // 心跳超时时间

    // 用于传入快照点
    int m_lastSnapshotIncludeIndex; // 存储快照最后一个日志index
    int m_lastSnapshotIncludeTerm;  // 存储快照最后一个日志term

    // 协程
    std::unique_ptr<monsoon::IOManager> m_ioManager = nullptr;

public:
    // 日志同步+心跳
    void AppendEntries1(const raftRpcProtoc::AppendEntriesArgs *args, raftRpcProtoc::AppendEntriesReply *reply);
    // 定期向状态机写入日志
    void applierTicker();
    // 记录某个时刻状态
    bool CondInstallSnapshot(int lastIncludeTerm, int lastIncludeIndex, std::string snapshot);

    // 发起选举
    void doElection();
    // leader发起心跳
    void doHeartBeat();
    // 每隔一段时间检查睡眠时间内有没有重置定时器，没有则超时了
    // 如果有则设置合适睡眠时间：睡眠到重置时间+超时时间
    // 监控是否发起选择
    void electionTimeOutTicker();

    // 获取应用日志
    std::vector<ApplyMsg> getApplyLogs();
    // 获取新命令的索引
    int getnewCommandIndex();
    // 获取当前日志信息
    void getPreLogInfo(int server, int *preindex, int *preterm);
    // 查看当前节点是否是领导节点
    void GetState(int *term, bool *isLeader);
    // 安装快照
    void InstallSnapshot1(const raftRpcProtoc::InstallSnapshotRequest *request, raftRpcProtoc::InstallSnapshotResponse *response);
    // 负责查看是否该发起心跳了，如果该发起就执行doHeartBeat
    void leaderHearBeatTicker();
    // leader节点发送快照
    void leaderSendSnapShot(int server);
    // leader节点更新CommitIndex
    void leaderUpdateCommitIndex();
    // 对象index日志是否匹配，用来判断leader节点日志和从者是否匹配
    bool matchLog(int logIndex, int logTerm);
    // 持久化当前状态
    void persist();

    // 候选者请求其他节点投票
    void RequestVote1(const raftRpcProtoc::RequestVoteArgs *args, raftRpcProtoc::RequestVoteReply *reply);
    // 判断当前节点是否有最新日志
    bool UpToData(int index, int term);
    // 获取最后一个日志条目索引
    int getLastLogIndex();
    // 获取最后一个日志任期
    int getLastLogTerm();
    // 获取最后一个日志条目索引和任期
    void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm);
    // 获取指定日志索引的任期
    int getLogTermFromLogIndex(int logindex);
    // 获取Raft状态的大小
    int GetRaftStateSize();
    // 将日志索引转换为日志条目在m_logs数组中的位置
    int getSlicesIndexFromLogIndex(int logindex);
    // 请求其他节点给自己投票
    bool sendRequestVote(int server, std::shared_ptr<raftRpcProtoc::RequestVoteArgs> args, std::shared_ptr<raftRpcProtoc::RequestVoteReply> reply, std::shared_ptr<int> voteNum);
    // 发起追加日志条目
    bool sendAppendEntries(int server, std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args, std::shared_ptr<raftRpcProtoc::AppendEntriesReply> reply, std::shared_ptr<int> appendNum);

    // 给上层Kvserver层发消息
    void pushMsgToKVServer(ApplyMsg msg);
    // 读取持久数据
    void readPersist(std::string data);
    // 持久化数据
    std::string persistData();

    // 启动
    void Start(Op command, int *newLogIndex, int *newLogTerm, bool *isleader);

    // index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
    // 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
    // 即服务层主动发起请求raft保存snapshot里面的数据，index是用来表示snapshot快照执行到了哪条命令
    void Snapshot(int index, std::string snapshot);

public:
    // 重写基类方法,因为rpc远程调用真正调用的是这个方法
    // 序列化，反序列化等操作rpc框架都已经做完了，因此这里只需要获取值然后真正调用本地方法即可。
    void AppendEntries(google::protobuf::RpcController *controller, const ::raftRpcProtoc::AppendEntriesArgs *request,
                       ::raftRpcProtoc::AppendEntriesReply *response, ::google::protobuf::Closure *done) override;
    void InstallSnapshot(google::protobuf::RpcController *controller, const ::raftRpcProtoc::InstallSnapshotRequest *request,
                         ::raftRpcProtoc::InstallSnapshotResponse *response, ::google::protobuf::Closure *done) override;
    void RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProtoc::RequestVoteArgs *request,
                     ::raftRpcProtoc::RequestVoteReply *response, ::google::protobuf::Closure *done) override;

public:
    // 初始化
    void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
              std::shared_ptr<LockQueue<ApplyMsg>> applyCh);

private:
    // for persist 持久化
    class BoostPersistRaftNode
    {
    public:
        friend class boost::serialization::access;
        // When the class Archive corresponds to an output archive, the
        // & operator is defined similar to <<.  Likewise, when the class Archive
        // is a type of input archive the & operator is defined similar to >>.
        template <class Archive>
        void serialize(Archive &ar, const unsigned int version)
        {
            ar & m_currentTerm;
            ar & m_votedFor;
            ar & m_lastSnapshotIncludeIndex;
            ar & m_lastSnapshotIncludeTerm;
            ar & m_logs;
        }
        int m_currentTerm;
        int m_votedFor;
        int m_lastSnapshotIncludeIndex;
        int m_lastSnapshotIncludeTerm;
        std::vector<std::string> m_logs;
        std::unordered_map<std::string, int> umap;
    };
};

#endif