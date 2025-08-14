#include "util.h"
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <iomanip>

void myAssert(bool condition, std::string message)
{
    if (!condition)
    {
        std::cerr << "Error: " << message << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

std::chrono::_V2::system_clock::time_point now()
{
    return std::chrono::high_resolution_clock::now();
}

std::chrono::milliseconds getRandomizedElectionTimeout()
{
    // std::random_device 是 C++ 标准库提供的非确定性随机数生成器，通常基于硬件熵源（如系统时间、设备状态等）,仅用于初始化其他随机数引擎。
    std::random_device rd;
    // std::mt19937 是一个基于 Mersenne Twister 算法的伪随机数生成器，生成 32 位随机数
    std::mt19937 rng(rd());
    /*std::uniform_int_distribution 是一个分布适配器，将随机数引擎的输出转换为均匀分布的整数。
    minRandomizedElectionTime 和 maxRandomizedElectionTime 定义了生成随机数的闭区间 [min, max]。*/
    std::uniform_int_distribution<int> dist(minRandomizedElectionTime, maxRandomizedElectionTime);

    // dist(rng) 调用分布对象，从随机数引擎 rng 中获取随机数，并将其映射到指定区间
    return std::chrono::milliseconds(dist(rng));
}

void sleepNMilliseconds(int N)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(N));
}

void DPrintf(const char *format, ...)
{
    if (Debug)
    {
        time_t now = time(nullptr);
        tm *nowtm = localtime(&now);

        std::printf("[%d-%d-%d-%d-%d-%d]",
                    nowtm->tm_year + 1900,
                    nowtm->tm_mon + 1,
                    nowtm->tm_mday,
                    nowtm->tm_hour,
                    nowtm->tm_min,
                    nowtm->tm_sec);
        // 定义可变参数列表
        va_list args;
        // 初始化参数列表，以 format 为起点
        va_start(args, format);
        // 将参数列表传递给 vprintf 进行格式化输出
        std::vprintf(format, args);
        std::printf("\n");
        // 清理参数列表
        va_end(args);
    }
}

// 检查指定端口是否可用（即未被占用）
bool isReleasePort(unsigned short usPort)
{
    int s = socket(AF_INET, SOCK_STREAM, IPPROTO_IP); // 创建TCP套接字

    // 配置地址结构，使用本地回环地址(127.0.0.1)
    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(usPort);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    // 尝试绑定端口
    int ret = ::bind(s, (sockaddr *)&addr, sizeof(addr));
    if (ret != 0)
    {
        close(s);
        return false; // 绑定失败，端口被占用
    }
    close(s);
    return true; // 绑定成功，端口可用
}

bool getReleasePort(short &port)
{
    short num = 0;
    // 最多尝试30个连续端口
    while (!isReleasePort(port) && num < 30)
    {
        ++port;
        ++num;
    }
    if (num >= 30)
    {
        port = -1; // 重置为无效值
        return false;
    }
    return true;
}