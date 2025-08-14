#ifndef UTIL_H
#define UTIL_H

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/access.hpp>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <random>
#include <sstream>
#include <thread>
#include "config.h"

template <class F>
class Deferclass
{
public:
    Deferclass(F &&f) : m_func(std::forward<F>(f)) {}
    Deferclass(const F &f) : m_func(f) {};
    ~Deferclass() { m_func(); }

    Deferclass(const Deferclass &e) = delete;
    Deferclass &operator=(const Deferclass &e) = delete;

private:
    F m_func;
};

#define _CONCAT(a, b) a##b
#define _MAKE_DEFER_(line) Deferclass _CONCAT(defer_placeholder, line) = [&]()

#undef DEFER
#define DEFER _MAKE_DEFER_(__LINE__)

void DPrintf(const char *format, ...);

void myAssert(bool condition, std::string message = "Assertion failed!");

// 输入："Hello, {}! You have {} messages.", "Alice", 3
// 输出："Hello, Alice! You have 3 messages."
template <typename... Args>
std::string format(const char *format_str, Args... args)
{
    // 计算所需缓冲区大小
    int size_s = snprintf(nullptr, 0, format_str, args...) + 1; //'\0'
    if (size_s <= 0)
    {
        throw std::runtime_error("Error during formatting.");
    }

    auto size = static_cast<size_t>(size_s);
    std::vector<char> buf(size);
    // 将格式化后的字符串写入缓冲区（包含\0）
    std::snprintf(buf.data(), size, format_str, args...);

    return std::string(buf.data(), buf.data() + size - 1); // remove '\0'
}

std::chrono::_V2::system_clock::time_point now();

std::chrono::milliseconds getRandomizedElectionTimeout();
void sleepNMilliseconds(int N);

// //////////////////////////////// 异步日志缓存队列 ////////////////////////////////
template <typename T>
class LockQueue
{
public:
    // 多个工作线程写日志queue
    void Push(const T &data)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_queue.push(data);
        m_condvariable.notify_one();
    }

    // 一个线程读日志queue,写日志文件
    T Pop()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        while (m_queue.empty())
        {
            // 日志队列为空，线程进入wait状态
            m_condvariable.wait(lock);
        }

        T data = m_queue.front();
        m_queue.pop();
        return data;
    }

    // 超时弹出，默认为50ms
    bool timeOutPop(int timeout, T *ResData)
    {
        std::unique_lock<std::mutex> lock(m_mutex);

        // 获取当前时间点，并计算出超时时刻
        auto now = std::chrono::system_clock::now();
        auto timeout_time = now + std::chrono::milliseconds(timeout);

        while (m_queue.empty())
        {
            if (m_condvariable.wait_until(lock, timeout_time) == std::cv_status::timeout)
            {
                return false;
            }
            else
            {
                continue;
            }
        }

        T data = m_queue.front();
        m_queue.pop();
        *ResData = data;
        return true;
    }

private:
    std::queue<T> m_queue;
    std::mutex m_mutex;
    std::condition_variable m_condvariable;
};

// //////////////////////////////// kv传递给raft的命令序列化和反序列化 ////////////////////////////////
class Op
{
public:
    std::string Operation; // "Get" "Put" "Append"
    std::string Key;
    std::string Value;
    std::string ClientId; // 客户端号码
    int RequestId;        // 客户端号码请求的Request的序列号，为了保证线性一致性

public:
    // todo
    // 为了协调raftRPC中的command只设置成了string,这个的限制就是正常字符中不能包含|
    // 序列化OP
    std::string asString() const
    {
        std::stringstream ss;
        boost::archive::text_oarchive oa(ss);

        // write class instance to archive
        oa << *this;
        // close archive

        return ss.str();
    }

    // 反序列化
    bool parseFromString(std::string str)
    {
        std::stringstream iss(str);
        boost::archive::text_iarchive ia(iss);
        // read class state from archive
        ia >> *this;
        return true; // todo : 解析失敗如何處理，要看一下boost庫了
    }

public:
    friend std::ostream &operator<<(std::ostream &os, const Op &obj)
    {
        os << "[MyClass:Operation{" + obj.Operation + "},Key{" + obj.Key + "},Value{" + obj.Value + "},ClientId{" +
                  obj.ClientId + "},RequestId{" + std::to_string(obj.RequestId) + "}"; // 在这里实现自定义的输出格式
        return os;
    }

private:
    //  Boost.Serialization 支持
    friend class boost::serialization::access;
    template <class Archive>
    void serialize(Archive &ar, const unsigned int version)
    {
        ar & Operation;
        ar & Key;
        ar & Value;
        ar & ClientId;
        ar & RequestId;
    }
};

///////////////////////////////////////////////kvserver reply err to clerk

const std::string OK = "OK";
const std::string ErrNoKey = "ErrNoKey";
const std::string ErrWrongLeader = "ErrWrongLeader";

////////////////////////////////////获取可用端口

bool isReleasePort(unsigned short usPort);

bool getReleasePort(short &port);

#endif