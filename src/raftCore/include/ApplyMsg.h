#ifndef APPLYMSG_H
#define APPLYMSG_H
#include <string>
class ApplyMsg
{
public:
    bool CommandValid;    // 命令是否有效（true表示这是一条命令消息）
    std::string Command;  // 客户端发送的命令内容
    int CommandIndex;     // 命令在日志中的索引位置
    bool SnapshotValid;   // 快照是否有效（true表示这是一条快照消息）
    std::string Snapshot; // 快照数据内容
    int SnapshotTerm;     // 快照对应的任期号
    int SnapshotIndex;    // 快照对应的最后日志索引

public:
    // 两个valid最开始要赋予false！！
    ApplyMsg() : CommandValid(false), Command(), CommandIndex(-1), SnapshotValid(false), SnapshotTerm(-1), SnapshotIndex(-1) {};
};

#endif