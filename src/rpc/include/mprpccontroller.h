#ifndef MPRPCCONTROLLER_H
#define MPRPCCONTROLLER_H

#include <google/protobuf/service.h>
#include <string>

class MprpcController : public google::protobuf::RpcController
{
public:
    MprpcController();
    void Reset();
    bool Failed() const;
    std::string ErrorText() const;
    void SetFailed(const std::string &reason);
    // to do
    void StartCancel();
    bool IsCanceled() const;
    void NotifyOnCancel(google::protobuf::Closure *callback);

private:
    bool m_failed;         // rpc方法执行过程中状态
    std::string m_errText; // rpc方法执行过程中错误信息
};

#endif