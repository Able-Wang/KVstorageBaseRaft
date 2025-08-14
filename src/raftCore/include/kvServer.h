#ifndef KVSERVER_H
#define KVSERVER_H

#include <boost/any.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/foreach.hpp>
#include <boost/serialization/export.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>
#include <iostream>
#include <mutex>
#include <unordered_map>
#include <kvServerRPC.pb.h>
#include <raft.h>
#include <skipList.h>

class KvServer : raftKVRpcProtoc::kvServerRpc
{
private:
    std::mutex m_mtx;
    int m_me;
    std::shared_ptr<Raft> m_raftNode;
    std::shared_ptr<LockQueue<ApplyMsg>> applyChan; // kvServer和raft节点的通信管道
    int m_maxRaftState;                             // snapshot if log grows this big

    std::string m_serializedKVData; // 序列化后的kv数据
    SkipList<std::string, std::string> m_skipList;
    std::unordered_map<std::string, std::string> m_kvDB;
    std::unordered_map<int, LockQueue<Op> *> waitApplyCh; // waitApplyCh是一个map，键是int，值是Op类型的管道
    std::unordered_map<std::string, int> m_lastRequestId; // clientid -> requestID  //一个kV服务器可能连接多个client

    int m_lastSnapShotRaftLogIndex; // last SnapShot point , raftIndex

public:
    KvServer() = delete;

    KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port);

    void StartKVServer();

    void DprintfKVDB();

    void ExecuteAppendOpOnKVDB(Op op);

    void ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist);

    void ExecutePutOpOnKVDB(Op op);

    // 将 GetArgs 改为rpc调用的，因为是远程客户端，即服务器宕机对客户端来说是无感的
    void Get(const raftKVRpcProtoc::GetArgs *args, raftKVRpcProtoc::GetReply *reply);

    // 从Raft节点获取消息
    void GetCommandFromRaft(ApplyMsg message);

    bool ifRequestDuplicate(std::string ClientId, int RequestId);

    // clerk 使用RPC远程调用
    void PutAppend(const raftKVRpcProtoc::PutAppendArgs *args, raftKVRpcProtoc::PutAppendReply *reply);

    // 一直等待raft传来的applych
    void ReadRaftApplyCommandLoop();

    void ReadSnapShotToInstall(std::string snapshot);

    bool SendMessageToWaitChan(const Op &op, int raftIndex);

    // 检查是否需要制作快照，需要的话就向raft之下制作快照
    void IfNeedToSendSnapShotCommand(int raftIndex, int proportion);

    // Handler the SnapShot from kv.rf.applyCh
    void GetSnapShotFromRaft(ApplyMsg message);

    std::string MakeSnapShot();

public: // for rpc
    void PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProtoc::PutAppendArgs *request,
                   ::raftKVRpcProtoc::PutAppendReply *response, ::google::protobuf::Closure *done) override;

    void Get(google::protobuf::RpcController *controller, const ::raftKVRpcProtoc::GetArgs *request,
             ::raftKVRpcProtoc::GetReply *response, ::google::protobuf::Closure *done) override;

private:
    /////////////////serialiazation start ///////////////////////////////
    friend class boost::serialization::access;

    template <class Archive>
    void serialize(Archive &ar, const unsigned int version) // 这里面写需要序列话和反序列化的字段
    {
        ar & m_serializedKVData;
        // ar & m_kvDB;
        ar & m_lastRequestId;
    }

    std::string getSnapshotData()
    {
        m_serializedKVData = m_skipList.dump_file();
        std::stringstream ss;
        boost::archive::text_oarchive oa(ss);
        oa << *this;
        m_serializedKVData.clear();
        return ss.str();
    }

    void parseFromString(const std::string &str)
    {
        std::stringstream ss(str);
        boost::archive::text_iarchive ia(ss);
        ia >> *this;
        m_skipList.load_file(m_serializedKVData);
        m_serializedKVData.clear();
    }

    /////////////////serialiazation end ///////////////////////////////
};

#endif