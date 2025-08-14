#ifndef MPRPCCONFIG_H
#define MPRPCCONFIG_H

#include <unordered_map>
#include <string>

// 框架读取配置文件类
// rpcserverip rpcserverport zookerip zookeeperport
class MprpcConfig
{
public:
    // 负责加载配置文件
    void loadConfigFile(const char *config_file);
    // 查询配置项信息
    std::string Load(const std::string &key);
    // 去除多余空格
    void Trim(std::string &src_buf);

private:
    std::unordered_map<std::string, std::string> m_configMap;
};

#endif