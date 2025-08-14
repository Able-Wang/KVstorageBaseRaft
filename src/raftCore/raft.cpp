#include "raft.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <memory>
#include <atomic>
#include "config.h"
#include "util.h"

// 日志同步+心跳
void Raft::AppendEntries1(const raftRpcProtoc::AppendEntriesArgs *args, raftRpcProtoc::AppendEntriesReply *reply)
{
    std::lock_guard<std::mutex> locker(m_mtx);
    // 能接收到网络是正常的
    reply->set_appstate(AppNormal);

    // 检查任期
    // 注意从过期的领导人收到消息不要
    if (args->term() < m_currentTerm)
    {
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(-100); // 让领导人可以及时更新自己
        DPrintf("[func-AppendEntries-rf{%d}] 拒绝了 因为Leader{%d}的term{%v}< rf{%d}.term{%d}\n",
                m_me, args->leaderid(), args->term(), m_me, m_currentTerm);
        return;
    }

    // 确保了持久化persist（）会在作用域结束时自动调用，即使在这个作用域中发生了异常或者提前返回了
    DEFER { persist(); };

    if (args->term() > m_currentTerm)
    {
        m_status = Follower;
        m_currentTerm = args->term();
        m_VoteFor = -1; // 这里应该设置成-1有意义，如果突然宕机然后上线依旧可以继续投票1
    }

    myAssert(args->term() == m_currentTerm, format("assert {args.Term == rf.currentTerm} fail"));

    // 如果发生网络分区，那么candidate可能会收到同一个term的leader的消息，此时要转变成follower
    m_status = Follower;             // 这里是有必要的，因为如果candidate收到同一个term的leader的AE，需要变成follower
    m_lastResetElectionTime = now(); // 这里重置超时定时器的目的是为了告诉当前分区已经有了learder避免重复选举

    // 不能无脑从prevlogIndex开始截断日志，因为rpc可能会延迟，导致发送过来的log可能是很久之前的，所以需比较日志
    if (args->prevlogindex() > getLastLogIndex())
    {
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(getLastLogIndex() + 1); // updatenextindex的意思就是期望下次传送数据的index范围这个主要由接受节点自己去判断
        return;
    }
    else if (args->prevlogindex() < m_lastSnapshotIncludeIndex)
    {
        // 如果prevLogIndex小于本地快照的最后一个日志索引，表示leader发送的日志过于陈旧。
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(m_lastSnapshotIncludeIndex + 1);
        // return; // 需要？
    }

    if (matchLog(args->prevlogindex(), args->prevlogterm()))
    {
        /* 这里判断的情况就是Leader发送的index大于本地raft快照1astindex并且小于本地raft日志数组范围内的，不能进行无脑的截断，直接截断可能重复拿去已经存在到
         追随者当前日志中条目，需要一个一个进行匹配，也就是向前纠错，查看到具体那个任期中那个日志出现问题*/
        for (int i = 0; i < args->entries_size(); ++i)
        {
            auto log = args->entries(i);
            if (log.logindex() > getLastLogIndex())
            {
                // 超过就直接添加日志
                m_logs.push_back(log);
            }
            else
            {
                // 没超过意味着leader发送的日志条目的index小于当前raft日志中最后记录的index
                // 没超过就比较是否匹配，不匹配再更新而不是直接截断。
                // 这里上面有一个细节首先判断了leader发送过来认为raft已经接收到是prelogindex和任期的值和raft当前节点是否一致，是就继续
                // else就在下面进行优化rpc请求次数的处理。
                // 这里判断的就是leader新发送的数据的index小于等于日志记录最后的index，不是直接截断而是看leader发送的任期和命令是否和当前raft记录的任期和命令一致，不一致就更新
                if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() == log.logterm() &&
                    m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() != log.command())
                {
                    myAssert(false, format("[func-AppendEntries-rf{%d}] 两节点logIndex{%d}和term{%d}相同，但是其command{%d:%d}   "
                                           " {%d:%d}却不同！！\n",
                                           m_me, log.logindex(), log.logterm(), m_me,
                                           m_logs[getSlicesIndexFromLogIndex(log.logindex())].command(), args->leaderid(), log.command()));
                }
                if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() != log.logterm())
                {
                    // 不匹配就更新
                    m_logs[getSlicesIndexFromLogIndex(log.logindex())] = log;
                }
            }
        }

        // 因为可能会收到过期的log！！！ 因此这里是大于等于
        myAssert(getLastLogIndex() >= args->prevlogindex() + args->entries_size(),
                 format("[func-AppendEntries1-rf{%d}]rf.getLastLogIndex(){%d} != args.PrevLogIndex{%d}+len(args.Entries){%d}",
                        m_me, getLastLogIndex(), args->prevlogindex(), args->entries_size()));

        if (args->leadercommit() > m_commitIndex)
        {
            // 这个地方不能无脑跟上getLastLogIndex()，因为可能存在args->leadercommit()落后于 getLastLogIndex()的情况
            m_commitIndex = std::min(args->leadercommit(), getLastLogIndex());
        }

        // 领导会一次发送完所有的日志
        myAssert(getLastLogIndex() >= m_commitIndex, format("[func-AppendEntries1-rf{%d}]  rf.getLastLogIndex{%d} < rf.commitIndex{%d}",
                                                            m_me, getLastLogIndex(), m_commitIndex));
        reply->set_success(true);
        reply->set_term(m_currentTerm);
        return;
    }
    else
    {
        // 此时的情况就是领导认为我们应该同步的index和任期在我们的追随者日志中不一致
        // 所以就需要发送期望领导发送日志的index值，这个由接收者去设置期望的index
        // 并且优化请求rpc的次数
        // 注意：如果没出现任期不匹配的情况可能是其他矛盾就不会利用到for循环的优化而是一步步向前匹配
        reply->set_updatenextindex(args->prevlogindex());
        for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index)
        {
            if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex()))
            {
                reply->set_updatenextindex(index + 1);
                break;
            }
        }

        reply->set_success(false);
        reply->set_term(m_currentTerm);
        return;
    }
}

// 定期向状态机写入日志
void Raft::applierTicker()
{
    while (true)
    {
        m_mtx.lock();
        if (m_status == Leader)
        {
            DPrintf("[Raft::applierTicker() - raft{%d}]  m_lastApplied{%d}   m_commitIndex{%d}", m_me, m_lastApplied, m_commitIndex);
        }

        auto applyMsgs = getApplyLogs();
        m_mtx.unlock();

        if (!applyMsgs.empty())
        {
            DPrintf("[func- Raft::applierTicker()-raft{%d}] 向kvserver报告的applyMsgs长度为{%d}", m_me, applyMsgs.size());
        }

        for (auto &message : applyMsgs)
        {
            applyChan->Push(message);
        }
        // usleep(1000 * ApplyInterval);
        sleepNMilliseconds(ApplyInterval);
    }
}

// 记录某个时刻状态
bool Raft::CondInstallSnapshot(int lastIncludeTerm, int lastIncludeIndex, std::string snapshot)
{
    return true;
}

// 发起选举
void Raft::doElection()
{
    std::lock_guard<std::mutex> lg(m_mtx);

    if (m_status == Leader)
    {
    }

    if (m_status != Leader)
    {
        DPrintf("[       ticker-func-rf(%d)              ]  选举定时器到期且不是leader，开始选举 \n", m_me);
        // 当选举的时候定时器超时就必须重新选举，不然没有选票就会一直卡主
        // 重竞选超时，term也会增加的
        m_status = Candidate;
        // 开始新一轮选举
        m_currentTerm += 1;
        m_VoteFor = m_me; // 即是自己给自己投，也避免candidate给同辈的candidate投
        persist();
        // std::shared_ptr<std::atomic<int>> votedNum = std::make_shared<std::atomic<int>>(1);
        std::shared_ptr<int> votedNum = std::make_shared<int>(1); // 使用 make_shared 函数初始化
        //  重置定时器
        m_lastResetElectionTime = now();
        // 发布RequsetVote Rpc
        for (int i = 0; i < m_peers.size(); ++i)
        {
            if (i == m_me)
            {
                continue;
            }

            int lastLogIndex = -1, lastLogTerm = -1;
            getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);
            std::shared_ptr<raftRpcProtoc::RequestVoteArgs> rquestVoteArgs = std::make_shared<raftRpcProtoc::RequestVoteArgs>();
            rquestVoteArgs->set_term(m_currentTerm);
            rquestVoteArgs->set_candidateid(m_me);
            rquestVoteArgs->set_lastlogindex(lastLogIndex);
            rquestVoteArgs->set_lastlogterm(lastLogTerm);
            std::shared_ptr<raftRpcProtoc::RequestVoteReply> rquestVoteReply = std::make_shared<raftRpcProtoc::RequestVoteReply>();

            // 创建新线程并执行b函数，并传递参数
            std::thread t(&Raft::sendRequestVote, this, i, rquestVoteArgs, rquestVoteReply, votedNum);
            t.detach();
        }
    }
}

// leader发起心跳
void Raft::doHeartBeat()
{
    std::lock_guard<std::mutex> lg(m_mtx);
    if (m_status == Leader)
    {
        DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了且拿到mutex，开始发送AE\n", m_me);
        // 正确返回的节点的数量
        auto appendNums = std::make_shared<int>(1);

        // 对Follower（除了自己外的所有节点发送AE）
        for (int i = 0; i < m_peers.size(); ++i)
        {
            if (i == m_me)
            {
                continue;
            }

            DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了 index:{%d}\n", m_me, i);
            myAssert(m_nextIndex[i] >= 1, format("rf.nextIndex[%d] = {%d}", i, m_nextIndex[i]));
            // 日志压缩加入后要判断是发送快照还是发送AE
            if (m_nextIndex[i] < m_lastSnapshotIncludeIndex)
            {
                std::thread t(&Raft::leaderSendSnapShot, this, i);
                t.detach();
                continue;
            }

            // 构造发送参数
            int preLogIndex = -1;
            int preLogTerm = -1;
            // 获取第i个追随者拥有最后匹配的索引index和任期term
            getPreLogInfo(i, &preLogIndex, &preLogTerm);
            std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> appendEntriesArgs = std::make_shared<raftRpcProtoc::AppendEntriesArgs>();
            appendEntriesArgs->set_term(m_currentTerm);
            appendEntriesArgs->set_leaderid(m_me);
            appendEntriesArgs->set_prevlogindex(preLogIndex);
            appendEntriesArgs->set_prevlogterm(preLogTerm);
            appendEntriesArgs->clear_entries(); // ；//这一步是为了防止有残留的日志条目信息和新的一起发出去。
            appendEntriesArgs->set_leadercommit(m_commitIndex);
            // 说明追随者的日志不是快照中的最后一条日志，因此直接从日志数组中提取日志条目。
            if (preLogIndex != m_lastSnapshotIncludeIndex)
            {
                // getSlicesIndexFromLogIndex：将日志索引（逻辑索引）转换为日志数组的下标
                for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < m_logs.size(); ++j)
                {
                    // 添加了新的日志条目，并且添加指向日志条目的指针
                    raftRpcProtoc::LogEntry *sendEntryPtr = appendEntriesArgs->add_entries();
                    // 将日志数据赋值给新添加的日志条目。
                    *sendEntryPtr = m_logs[j];
                }
            }
            else
            {
                // 这里是追随者同步的preindex和领导记录的最后快照index一致，所以从快照往后发
                for (const auto &item : m_logs)
                {
                    raftRpcProtoc::LogEntry *sendEntryPtr = appendEntriesArgs->add_entries();
                    *sendEntryPtr = item; //=是可以点进去的，可以点进去看下protobuf如何重写这个的
                }
            }

            int lastLogIndex = getLastLogIndex();
            // leader对每个节点发送的日志长短不一，但是都保证从prevIndex发送直到最后
            myAssert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex,
                     format("appendEntriesArgs.PrevLogIndex{%d}+len(appendEntriesArgs.Entries){%d} != lastLogIndex{%d}",
                            appendEntriesArgs->prevlogindex(), appendEntriesArgs->entries_size(), lastLogIndex));

            // 构造返回值
            std::shared_ptr<raftRpcProtoc::AppendEntriesReply> appendEntriesReply =
                std::make_shared<raftRpcProtoc::AppendEntriesReply>();
            appendEntriesReply->set_appstate(Disconnected);

            std::thread t(&Raft::sendAppendEntries, this, i, appendEntriesArgs,
                          appendEntriesReply, appendNums);
            t.detach();
        }
        // leader发送心跳，重置心跳时间
        m_lastResetHeartBeatTime = now();
    }
}

// 每隔一段时间检查睡眠时间内有没有重置定时器，没有则超时了
// 如果有则设置合适睡眠时间：睡眠到重置时间+超时时间
// 监控是否发起选择
void Raft::electionTimeOutTicker()
{
    while (true)
    {
        /**
         * 对于leader,如果不睡眠，这个函数会一直空转，浪费cpu。且加入协程之后，空转会导致其他协程无法运行，对于时间敏感的AE，会导致心跳无法正常发送导致异常
         */
        while (m_status == Leader)
        {
            usleep(HeartBeatTimeout); // 定时时间没有严谨设置，因为HeartBeatTimeout比选举超时一般小一个数量级，因此就设置为HeartBeatTimeout了
        }

        std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
        std::chrono::system_clock::time_point weaktime{};
        {
            m_mtx.lock();
            weaktime = now();
            suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - weaktime;
            m_mtx.unlock();
        }

        if (std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() > 1)
        {
            // 获取当前时间点
            auto start = std::chrono::steady_clock::now();

            usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());

            // 获取函数运行结束之后的时间点
            auto end = std::chrono::steady_clock::now();

            // 计算时间差并输出结果（单位为毫秒）
            std::chrono::duration<double, std::milli> duration = end - start;

            // 使用ANSI控制序列将输出颜色修改为紫色
            std::cout << "\033[1;35m electionTimeOutTicker();函数设置睡眠时间为: "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                      << std::endl;
            std::cout << "\033[1;35m electionTimeOutTicker();函数实际睡眠时间为: " << duration.count() << " 毫秒\033[0m"
                      << std::endl;
        }

        // 说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠
        if (std::chrono::duration<double, std::milli>(m_lastResetElectionTime - weaktime).count() > 0)
        {
            continue;
        }

        doElection();
    }
}

// 获取应用日志
std::vector<ApplyMsg> Raft::getApplyLogs()
{
    std::vector<ApplyMsg> applyMsgs;
    myAssert(m_commitIndex <= getLastLogIndex(), format("[func-getApplyLogs-rf{%d}] commitIndex{%d} >getLastLogIndex{%d}",
                                                        m_me, m_commitIndex, getLastLogIndex()));

    while (m_lastApplied < m_commitIndex)
    {
        m_lastApplied++;
        myAssert(m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() == m_lastApplied,
                 format("rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogIndex{%d} != rf.lastApplied{%d} ",
                        m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(), m_lastApplied));
        ApplyMsg applyMsg;
        applyMsg.CommandValid = true;
        applyMsg.SnapshotValid = false;
        applyMsg.Command = m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].command();
        applyMsg.CommandIndex = m_lastApplied;
        applyMsgs.emplace_back(applyMsg);
    }
    return applyMsgs;
}

// 获取新命令的索引
int Raft::getnewCommandIndex()
{
    //	如果len(logs)==0,就为快照的index+1，否则为log最后一个日志+1
    auto lastLogIndex = getLastLogIndex();
    return lastLogIndex + 1;
}

// 获取当前日志信息
void Raft::getPreLogInfo(int server, int *preindex, int *preterm)
{ // logs长度为0返回0,0，不是0就根据nextIndex数组的数值返回
    if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1)
    {
        // 要发送的日志是第一个日志，因此直接返回m_lastSnapshotIncludeIndex和m_lastSnapshotIncludeTerm
        *preindex = m_lastSnapshotIncludeIndex;
        *preterm = m_lastSnapshotIncludeTerm;
        return;
    }

    auto nextIndex = m_nextIndex[server];
    *preindex = nextIndex - 1;
    *preterm = m_logs[getSlicesIndexFromLogIndex(*preindex)].logterm();
}

// 查看当前节点是否是领导节点
void Raft::GetState(int *term, bool *isLeader)
{
    m_mtx.lock();
    DEFER
    {
        m_mtx.unlock();
    };

    *term = m_currentTerm;
    *isLeader = (m_status == Leader);
}

// 快照
void Raft::InstallSnapshot1(const raftRpcProtoc::InstallSnapshotRequest *request, raftRpcProtoc::InstallSnapshotResponse *response)
{
    m_mtx.lock();
    DEFER { m_mtx.unlock(); };
    if (request->term() < m_currentTerm)
    {
        response->set_term(m_currentTerm);
        return;
    }

    // 后面两种情况都要接收日志
    if (request->term() > m_currentTerm)
    {
        m_currentTerm = request->term();
        m_VoteFor = -1;
        m_status = Leader;
        persist();
    }

    m_status = Follower;
    m_lastResetElectionTime = now(); // 重置定时器
    if (request->lastsnapshotincludeindex() <= m_lastSnapshotIncludeIndex)
    {
        return;
    }

    // 截断日志，修改commitIndex和lastApplied
    // 截断日志包括：日志长了，截断一部分，日志短了，全部清空，其实两个是一种情况
    // 但是由于现在getSlicesIndexFromLogIndex的实现，不能传入不存在logIndex，否则会panic
    auto lastLogIndex = getLastLogIndex();

    if (lastLogIndex > request->lastsnapshotincludeindex())
    {
        m_logs.erase(m_logs.begin(), m_logs.begin() + getSlicesIndexFromLogIndex(request->lastsnapshotincludeindex()) + 1);
    }
    else
    {
        m_logs.clear();
    }

    m_commitIndex = std::max(m_commitIndex, request->lastsnapshotincludeindex());
    m_lastApplied = std::max(m_lastApplied, request->lastsnapshotincludeindex());
    m_lastSnapshotIncludeIndex = request->lastsnapshotincludeindex();
    m_lastSnapshotIncludeTerm = request->lastsnapshotincludeterm();

    response->set_term(m_currentTerm);
    ApplyMsg msg;
    msg.SnapshotValid = true;
    msg.Snapshot = request->data();
    msg.SnapshotTerm = request->lastsnapshotincludeterm();
    msg.SnapshotIndex = request->lastsnapshotincludeindex();

    std::thread t(&Raft::pushMsgToKVServer, this, msg); // 创建新线程并执行b函数，并传递参数
    t.detach();
    // 持久化
    m_persister->Save(persistData(), request->data());
}

// 负责查看是否该发起心跳了，如果该发起就执行doHeartBeat
void Raft::leaderHearBeatTicker()
{
    while (true)
    {
        // 从者节点睡眠
        while (m_status != Leader)
        {
            usleep(1000 * HeartBeatTimeout);
        }

        static std::atomic<int32_t> atomicCount = 0;
        // 表示当前线程需要睡眠的时间，计算方式基于心跳超时时间（HeartBeatTimeout）和上一次心跳重置
        // 目的：用于动态调整睡眠时间，避免线程频繁检查状态导致cpu空转
        std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
        std::chrono::system_clock::time_point weaktime{};
        {
            m_mtx.lock();
            weaktime = now();
            suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout) + m_lastResetHeartBeatTime - weaktime;
            m_mtx.unlock();
        }

        if (std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() > 1)
        {
            std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数设置睡眠时间为: "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                      << std::endl;
            // 获取当前时间点
            auto start = std::chrono::steady_clock::now();

            usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());

            // 获取函数运行结束之后的时间点
            auto end = std::chrono::steady_clock::now();

            // 计算时间差并输出结果（单位为毫秒）
            std::chrono::duration<double, std::milli> duration = end - start;

            // 使用ANSI控制序列将输出颜色修改为紫色
            std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数实际睡眠时间为: " << duration.count() << " 毫秒\033[0m"
                      << std::endl;
        }

        // 说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠
        if (std::chrono::duration<double, std::milli>(m_lastResetHeartBeatTime - weaktime).count() > 0)
        {
            continue;
        }

        doHeartBeat(); // 执行心跳
    }
}

// leader节点发送快照
void Raft::leaderSendSnapShot(int server)
{
    m_mtx.lock();
    raftRpcProtoc::InstallSnapshotRequest args;
    args.set_leaderid(m_me);
    args.set_term(m_currentTerm);
    args.set_lastsnapshotincludeindex(m_lastSnapshotIncludeIndex);
    args.set_lastsnapshotincludeterm(m_lastSnapshotIncludeTerm);
    args.set_data(m_persister->ReadSnapshot());

    raftRpcProtoc::InstallSnapshotResponse reply;
    m_mtx.unlock();

    bool ok = m_peers[server]->InstallSnapshot(&args, &reply);

    m_mtx.lock();
    DEFER { m_mtx.unlock(); };

    if (!ok)
    {
        return;
    }

    // 中间释放过锁，可能状态已经改变了
    if (m_status != Leader || m_currentTerm != args.term())
    {
        return;
    }

    //	无论什么时候都要判断term
    if (reply.term() > m_currentTerm)
    {
        m_currentTerm = reply.term();
        m_status = Follower;
        m_VoteFor = -1;
        persist();
        m_lastResetElectionTime = now();
        return;
    }

    m_matchIndex[server] = args.lastsnapshotincludeindex();
    m_nextIndex[server] = m_matchIndex[server] + 1;
}

// leader节点更新CommitIndex
void Raft::leaderUpdateCommitIndex()
{
    m_commitIndex = m_lastSnapshotIncludeIndex;
    for (int index = getLastLogIndex(); index >= m_lastSnapshotIncludeIndex; --index)
    {
        int sum = 0;
        for (int i = 0; i < m_peers.size(); ++i)
        {
            if (i == m_me)
            {
                sum += 1;
                continue;
            }
            if (m_matchIndex[i] >= index)
            {
                sum += 1;
            }
        }

        //        !!!只有当前term有新提交的，才会更新commitIndex！！！！
        if (sum >= m_peers.size() / 2 + 1 && getLogTermFromLogIndex(index) == m_currentTerm)
        {
            m_commitIndex = index;
            break;
        }
    }
}

// 对象index日志是否匹配，用来判断leader节点日志和从者是否匹配
// 进来前要保证logIndex是存在的，即≥rf.lastSnapshotIncludeIndex	，而且小于等于rf.getLastLogIndex()
bool Raft::matchLog(int logIndex, int logTerm)
{
    myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(),
             format("不满足：logIndex{%d}>=rf.lastSnapshotIncludeIndex{%d}&&logIndex{%d}<=rf.getLastLogIndex{%d}",
                    logIndex, m_lastSnapshotIncludeIndex, logIndex, getLastLogIndex()));
    return logTerm == getLogTermFromLogIndex(logIndex);
}

// 候选者请求其他节点投票
void Raft::RequestVote1(const raftRpcProtoc::RequestVoteArgs *args, raftRpcProtoc::RequestVoteReply *reply)
{
    std::lock_guard<std::mutex> lg(m_mtx);
    DEFER
    {
        persist(); // 要在锁释放前更新状态避免在锁释放后才更新，会导致状态被别人更新
    };

    // 检查任期
    // 竞选者任期小于选民任期，reason: 出现网络分区，该竞选者已经OutOfDate(过时）
    if (args->term() < m_currentTerm)
    {
        reply->set_term(m_currentTerm);
        reply->set_votestate(Expire);
        reply->set_votestate(false);
        return;
    }
    else if (args->term() > m_currentTerm) // 何时候rpc请求或者响应的term大于自己的term，更新term，并变成follower
    {
        m_status = Follower;
        m_currentTerm = args->term();
        m_VoteFor = -1; // 这里的目的是期望它投票继续进行日志复制。
    }
    myAssert(args->term() == m_currentTerm,
             format("[func--rf{%d}] 前面校验过args.Term==rf.currentTerm，这里却不等", m_me));

    // 现在节点任期都是相同的（任期小的也已经更新到新的args的term了
    // 还需要检查log的term和index是不是匹配的
    int lastLogTerm = getLastLogTerm();
    if (!UpToData(args->lastlogindex(), args->lastlogterm()))
    {
        // 日志任期
        if (args->lastlogterm() < lastLogTerm)
        {
            // to do
            // if (Debug)
            // {
            //     DPrintf("[	    func-RequestVote-rf(%d)		] : refuse voted rf[%d] ,because candidate_lastlog_term{ % d} < lastlog_term { %d} ]",
            //             args->candidateid(), args->candidateid(), args->lastlogterm(), lastLogTerm);
            // }
        }
        else
        {
            // to do
            // if (Debug)
            // {
            //     DPrintf("[	    func-RequestVote-rf(%d)		] : refuse voted rf[%d] ,because candidate_log_index{ % d} < log_index { % d }\n ",
            //             args->candidateid(), args->candidateid(), args->lastlogindex(), getLastLogIndex());
            // }
        }
        // 日志的最后一个任期或者index和请求参数的不匹配无法投票；
        reply->set_term(m_currentTerm);
        reply->set_votestate(Voted);
        reply->set_votegranted(false);

        return;
    }

    if (m_VoteFor != -1 && m_VoteFor != args->candidateid()) // 这里的情况是该节点已经投过票并且不是投给当前候选者
    {
        reply->set_term(m_currentTerm);
        reply->set_votestate(Voted);
        reply->set_votegranted(false);
        return;
    }
    else // 投票给该请求节点
    {
        m_VoteFor = args->candidateid();
        m_lastResetElectionTime = now(); // 必须在投出票的时候才重置定时器。
        reply->set_term(m_currentTerm);
        reply->set_votestate(Normal);
        reply->set_votegranted(true);
        return;
    }
}

// 判断当前节点是否有最新日志
bool Raft::UpToData(int index, int term)
{
    int lastLogTerm = -1;
    int lastLogIndex = -1;
    getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);

    return term > lastLogTerm || (lastLogTerm == term && index >= lastLogIndex);
}

// 获取最后一个日志条目索引
int Raft::getLastLogIndex()
{
    int lastLogIndex = -1;
    int _ = -1;
    getLastLogIndexAndTerm(&lastLogIndex, &_);
    return lastLogIndex;
}

// 获取最后一个日志任期
int Raft::getLastLogTerm()
{
    int _ = -1;
    int lastLogTerm = -1;
    getLastLogIndexAndTerm(&_, &lastLogTerm);
    return lastLogTerm;
}

// 获取最后一个日志条目索引和任期
void Raft::getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm)
{
    if (m_logs.empty())
    {
        *lastLogIndex = m_lastSnapshotIncludeIndex;
        *lastLogTerm = m_lastSnapshotIncludeTerm;
        return;
    }
    else
    {
        *lastLogIndex = m_logs[m_logs.size() - 1].logindex();
        *lastLogTerm = m_logs[m_logs.size() - 1].logterm();
        return;
    }
}

// 获取指定日志索引的任期
int Raft::getLogTermFromLogIndex(int logindex)
{
    myAssert(logindex >= m_lastSnapshotIncludeIndex,
             format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} < rf.lastSnapshotIncludeIndex{%d}",
                    m_me, logindex, m_lastSnapshotIncludeIndex));

    int lastindex = getLastLogIndex();

    myAssert(logindex <= lastindex,
             format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                    m_me, logindex, lastindex));

    if (logindex == m_lastSnapshotIncludeIndex)
    {
        return m_lastSnapshotIncludeTerm;
    }
    else
    {
        return m_logs[getSlicesIndexFromLogIndex(logindex)].logterm();
    }
}

// 获取Raft状态的大小
int Raft::GetRaftStateSize()
{
    return m_persister->RaftStateSize();
}

// 将日志索引转换为日志条目在m_logs数组中的位置
int Raft::getSlicesIndexFromLogIndex(int logindex)
{
    myAssert(logindex > m_lastSnapshotIncludeIndex,
             format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} <= rf.lastSnapshotIncludeIndex{%d}", m_me,
                    logindex, m_lastSnapshotIncludeIndex));
    int lastlogIndex = getLastLogIndex();
    myAssert(logindex <= lastlogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                              m_me, logindex, lastlogIndex));
    int SliceIndex = logindex - m_lastSnapshotIncludeIndex - 1;
    return SliceIndex;
}

// 请求其他节点给自己投票
bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcProtoc::RequestVoteArgs> args, std::shared_ptr<raftRpcProtoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum)
{
    auto start = now();
    DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} lastLogIndex:{%d}, 发送 RequestVote 开始", m_me, m_currentTerm, getLastLogIndex());
    // 这个ok是网络是否正常通信的ok，而不是requestVote rpc是否投票的rpc
    bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
    DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} lastLogIndex:{%d}，发送RequestVote 完毕，耗时:{%d} ms", m_me, m_currentTerm, getLastLogIndex(), now() - start);

    if (!ok)
    {
        return ok;
    }

    // 对回应进行处理，要记得无论什么时候收到回复就要检查term
    std::lock_guard<std::mutex> lg(m_mtx);
    if (reply->term() > m_currentTerm) // 别的节点任期大于本节点任期
    {
        m_status = Follower;
        m_currentTerm = reply->term();
        m_VoteFor = -1;
        persist();
        return true;
    }
    else if (reply->term() < m_currentTerm) // 别的节点任期小于本节点任期
    {
        return true;
    }
    myAssert(reply->term() == m_currentTerm, format("assert {reply.Term==rf.currentTerm} fail"));

    // 是否投票
    if (!reply->votegranted())
    {
        return true;
    }

    // 选票加1
    *votedNum = *votedNum + 1;
    if (*votedNum >= m_peers.size() / 2 + 1)
    {
        // 变成leader
        *votedNum = 0;
        if (m_status == Leader)
        {
            // 如果已经是leader了，那么是就是了，不会进行下一步处理了
            myAssert(false, format("[func-sendRequestVote-rf{%d}]  term:{%d} 同一个term当两次领导，error", m_me, m_currentTerm));
        }

        // 第一次变成leader，初始化状态和nextIndex、matchIndex
        m_status = Leader;
        DPrintf("[func-sendRequestVote rf{%d}] elect success  ,current term:{%d} ,lastLogIndex:{%d}\n", m_me, m_currentTerm, getLastLogIndex());

        int lastLogIndex = getLastLogIndex();
        for (int i = 0; i < m_nextIndex.size(); ++i)
        {
            m_nextIndex[i] = lastLogIndex + 1; // 有效下标从1开始，因此要+1
            m_matchIndex[i] = 0;               // 表示已经接收到领导日志的index下标，并且换一次领导就要设置为0.
        }

        // 马上向其他节点宣告自己就是leader,进行心跳和日志复制
        std::thread t(&Raft::doHeartBeat, this);
        t.detach();

        persist();
    }
    return true;
}

// 发起追加日志条目
bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args, std::shared_ptr<raftRpcProtoc::AppendEntriesReply> reply, std::shared_ptr<int> appendNum)
{
    // 这个ok是网络是否正常通信的ok，而不是requestVote rpc是否投票的rpc
    //  如果网络不通的话肯定是没有返回的，不用一直重试
    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc开始 ， args->entries_size():{%d}]",
            m_me, server, args->entries_size());
    // //调用 RPC 发送 AppendEntries 请求并等待响应，ok 表示 RPC 调用是否成功，而不是请求的逻辑
    bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());
    if (!ok)
    {
        // //RPC 调用失败（例如网络问题
        DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc失败]", m_me, server);
        return ok;
    }

    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc成功", m_me, server);
    if (reply->appstate() == Disconnected)
    {
        // RPC调用成功，但从者因网络分区或其他原因未能处理请求
        return ok;
    }

    std::lock_guard<std::mutex> lg(m_mtx);
    // 检查返回的 term 以维持日志的一致性
    if (reply->term() > m_currentTerm)
    {
        // 发现自己的 term过时，降级为Follower，并更新 term和votedFor
        m_status = Follower;
        m_currentTerm = reply->term();
        m_VoteFor = -1;
        persist(); // 持久化最新的 term和votedFor
        return ok;
    }
    else if (reply->term() < m_currentTerm)
    {
        // 忽略term过时的从者的响应
        DPrintf("[func -sendAppendEntries  rf{%d}]  节点：{%d}的term{%d}<rf{%d}的term{%d}\n", m_me, server, reply->term(), m_me, m_currentTerm);
        return ok;
    }

    if (m_status != Leader)
    {
        // 如果不是leader，那么就不要对返回的情况进行处理了
        return ok;
    }

    // term相等
    myAssert(reply->term() == m_currentTerm,
             format("reply.Term{%d} != rf.currentTerm{%d}   ", reply->term(), m_currentTerm));

    if (!reply->success())
    {
        // 日志不匹配，调整nextIndex并重试
        if (reply->updatenextindex() != -100) //-100是特殊标记，用于优化leader的回退逻辑
        {
            DPrintf("[func -sendAppendEntries  rf{%d}]  返回的日志term相等，但是不匹配，回缩nextIndex[%d]：{%d}\n", m_me, server, reply->updatenextindex());
            m_nextIndex[server] = reply->updatenextindex(); // 使用追随者提供的 nextIndex，减少不必要的重试,失败是不更新mathIndex的
        }
    }
    else
    {
        // 日志匹配，更新appendNums和相关索引
        *appendNum = *appendNum + 1;
        DPrintf("---------------------------tmp------------------------- 节点{%d}返回true,当前*appendNums{%d}", server, *appendNum);
        // 更新 matchIndex 和 nextIndex
        m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());
        m_nextIndex[server] = m_matchIndex[server] + 1;

        int lastLogIndex = getLastLogIndex();

        myAssert(m_nextIndex[server] <= lastLogIndex + 1,
                 format("error msg:rf.nextIndex[%d] > lastLogIndex+1, len(rf.logs) = %d   lastLogIndex{%d} = %d",
                        server, m_logs.size(), server, lastLogIndex));

        // 检查是否有日志可以提交
        if (*appendNum >= 1 + m_peers.size() / 2)
        {
            *appendNum = 0; // 避免重复提交

            // leader只有在当前term有日志提交的时候才更新commitIndex，因为raft无法保证之前term的Index是否提交
            // 只有当前term有日志提交，之前term的log才可以被提交，只有这样才能保证“领导人完备性{当选领导人的节点拥有之前被提交的所有log，当然也可能有一些没有被提交的}”
            if (args->entries_size() > 0)
            {
                DPrintf("args->entries(args->entries_size()-1).logterm(){%d}   m_currentTerm{%d}", args->entries(args->entries_size() - 1).logterm(), m_currentTerm);
            }

            // 只有当前 term 的日志被大多数追随者同步后，才能提交
            if (args->entries_size() > 0 && args->entries(args->entries_size() - 1).logterm() == m_currentTerm)
            {
                DPrintf("---------------------------tmp------------------------- 当前term有log成功提交，更新leader的m_commitIndex "
                        "from{%d} to{%d}",
                        m_commitIndex, args->prevlogindex() + args->entries_size());
                m_commitIndex = std::max(m_commitIndex, args->prevlogindex() + args->entries_size());
            }

            myAssert(m_commitIndex <= lastLogIndex, format("[func-sendAppendEntries,rf{%d}] lastLogIndex:%d  rf.commitIndex:%d\n",
                                                           m_me, lastLogIndex, m_commitIndex));
        }
    }
    return ok;
}

// 给上层Kvserver层发消息
void Raft::pushMsgToKVServer(ApplyMsg msg)
{
    applyChan->Push(msg);
}

// 持久化当前状态
void Raft::persist()
{
    auto data = persistData();
    m_persister->SaveRaftState(data);
}

// 持久化数据
std::string Raft::persistData()
{
    BoostPersistRaftNode boostPersistRaftNode;
    boostPersistRaftNode.m_currentTerm = m_currentTerm;
    boostPersistRaftNode.m_votedFor = m_VoteFor;
    boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
    boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;
    for (auto &item : m_logs)
    {
        boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());
    }

    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << boostPersistRaftNode;
    return ss.str();
}

// 读取持久数据
void Raft::readPersist(std::string data)
{
    if (data.empty())
    {
        return;
    }

    std::stringstream iss(data);
    boost::archive::text_iarchive ia(iss);
    // read class state from archive
    BoostPersistRaftNode boostPersistRaftNode;
    ia >> boostPersistRaftNode;
    m_currentTerm = boostPersistRaftNode.m_currentTerm;
    m_VoteFor = boostPersistRaftNode.m_votedFor;
    m_lastSnapshotIncludeIndex = boostPersistRaftNode.m_lastSnapshotIncludeIndex;
    m_lastSnapshotIncludeTerm = boostPersistRaftNode.m_lastSnapshotIncludeTerm;
    m_logs.clear();
    for (auto &item : boostPersistRaftNode.m_logs)
    {
        raftRpcProtoc::LogEntry logEntry;
        logEntry.ParseFromString(item);
        m_logs.emplace_back(logEntry);
    }
}

// 启动
void Raft::Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader)
{
    std::lock_guard<std::mutex> lg(m_mtx);

    if (m_status != Leader)
    {
        DPrintf("[func-Start-rf{%d}]  is not leader");
        *newLogIndex = -1;
        *newLogTerm = -1;
        *isLeader = false;
        return;
    }

    raftRpcProtoc::LogEntry newLogEntry;
    newLogEntry.set_command(command.asString());
    newLogEntry.set_logterm(m_currentTerm);
    newLogEntry.set_logindex(getnewCommandIndex());
    m_logs.emplace_back(newLogEntry);

    int lastLogIndex = getLastLogIndex();

    // leader应该不停的向各个Follower发送AE来维护心跳和保持日志同步，目前的做法是新的命令来了不会直接执行，而是等待leader的心跳触发
    DPrintf("[func-Start-rf{%d}]  lastLogIndex:%d,command:%s\n", m_me, lastLogIndex, command.Operation.c_str());
    persist();
    *newLogIndex = newLogEntry.logindex();
    *newLogTerm = newLogEntry.logterm();
    *isLeader = true;
}

// index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
// 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
// 即服务层主动发起请求raft保存snapshot里面的数据，index是用来表示snapshot快照执行到了哪条命令
void Raft::Snapshot(int index, std::string snapshot)
{
    std::lock_guard<std::mutex> lg(m_mtx);

    if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex)
    {
        DPrintf("[func-Snapshot-rf{%d}] rejects replacing log with snapshotIndex %d as current snapshotIndex %d is larger or smaller ",
                m_me, index, m_lastSnapshotIncludeIndex);
        return;
    }

    // 为了检查snapshot前后日志是否一样，防止多截取或者少截取日志
    auto lastLogIndex = getLastLogIndex();

    // 制造完此快照后剩余的所有日志
    int newlastSnapshotIncludeIndex = index;
    int newlastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
    std::vector<raftRpcProtoc::LogEntry> trunckedLogs;

    for (int i = index + 1; i <= getLastLogIndex(); i++)
    {
        // 注意有=，因为要拿到最后一个日志
        trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
    }

    m_lastSnapshotIncludeIndex = newlastSnapshotIncludeIndex;
    m_lastSnapshotIncludeTerm = newlastSnapshotIncludeTerm;
    m_logs = trunckedLogs;
    m_commitIndex = std::max(m_commitIndex, index);
    m_lastApplied = std::max(m_lastApplied, index);

    m_persister->Save(persistData(), snapshot);

    DPrintf("[SnapShot]Server %d snapshot snapshot index {%d}, term {%d}, loglen {%d}", m_me, index,
            m_lastSnapshotIncludeTerm, m_logs.size());
    myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
             format("len(rf.logs){%d} + rf.lastSnapshotIncludeIndex{%d} != lastLogIndex{%d}", m_logs.size(),
                    m_lastSnapshotIncludeIndex, lastLogIndex));
}

void Raft::AppendEntries(google::protobuf::RpcController *controller, const ::raftRpcProtoc::AppendEntriesArgs *request,
                         ::raftRpcProtoc::AppendEntriesReply *response, ::google::protobuf::Closure *done)
{
    AppendEntries1(request, response);
    done->Run();
}

void Raft::InstallSnapshot(google::protobuf::RpcController *controller, const ::raftRpcProtoc::InstallSnapshotRequest *request,
                           ::raftRpcProtoc::InstallSnapshotResponse *response, ::google::protobuf::Closure *done)
{
    InstallSnapshot1(request, response);
    done->Run();
}

void Raft::RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProtoc::RequestVoteArgs *request,
                       ::raftRpcProtoc::RequestVoteReply *response, ::google::protobuf::Closure *done)
{
    RequestVote1(request, response);
    done->Run();
}

// 初始化
void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh)
{
    m_peers = peers;         // 与其他节点沟通的rpc类
    m_me = me;               // 标记自己，不能自己给自己发送rpc
    m_persister = persister; // 持久化类

    m_mtx.lock();
    this->applyChan = applyCh; // 与KV-Server沟通
    m_currentTerm = 0;         // 初始化任期为0
    m_status = Follower;       // 初始化身份为从者
    m_commitIndex = 0;         // 初始化提交的日志索引
    m_lastApplied = 0;         // 初始化提交到状态机的日志
    m_logs.clear();
    for (int i = 0; i < m_peers.size(); ++i)
    {
        m_matchIndex.push_back(0); // 表示没有日志条目已提交或已应用
        m_nextIndex.push_back(0);
    }
    m_VoteFor = -1; // 表示没有投票对象

    m_lastSnapshotIncludeIndex = 0;
    m_lastSnapshotIncludeTerm = 0;
    m_lastResetElectionTime = now();
    m_lastResetHeartBeatTime = now();

    readPersist(m_persister->ReadRaftState()); // 从持久化存储中恢复Raft状态
    // 如果m_lastSnapshotIncludeIndex>0，则将m_lastAppiled设置为该值，为确保崩溃后能够从快照中恢复状态
    if (m_lastSnapshotIncludeIndex > 0)
    {
        m_lastApplied = m_lastSnapshotIncludeIndex;
    }
    DPrintf("[Init&ReInit] Server %d, term %d, lastSnapshotIncludeIndex {%d}, lastSnapshotIncludeTerm {%d}",
            m_me, m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);
    m_mtx.unlock(); // 完成初始化后解锁，方便其他线程或者协程可以访问共享数据

    m_ioManager = std::make_unique<monsoon::IOManager>(FIBER_THREAD_NUM, FIBER_USE_CALLER_THREAD);

    // 启动三个循环定时器
    m_ioManager->scheduler([this]() -> void
                           { this->leaderHearBeatTicker(); });
    m_ioManager->scheduler([this]() -> void
                           { this->electionTimeOutTicker(); });
    std::thread t3(&Raft::applierTicker, this);
    t3.detach();
}