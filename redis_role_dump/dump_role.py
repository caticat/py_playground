# -*- coding:utf-8 -*-

'''将数据库的角色数据导出到一个文件中'''

import redis, os
from sshtunnel import SSHTunnelForwarder

class DBConfig:
    def __init__(self, sshHost, sshPort, sshUser, sshPass, redisHost, redisPort):
        self.sshHost=sshHost
        self.sshPort=sshPort
        self.sshUser=sshUser
        self.sshPass=sshPass
        self.redisHost=redisHost
        self.redisPort=redisPort

dumpFile = './role_data.dump'
fromDBCfg = DBConfig('192.168.111.111', 22, 'username', 'password', '127.0.0.1', 9898) # 外网个人的测试数据库
fromUserName = '角色名'

def getGUID(r, userName):
    res = r.hget("name", userName)
    if res == None:
        return ""
    else:
        return r.hget("name", userName).decode('utf8')

def getUserData(r, GUID):
    key = "u:%s" % GUID
    return r.hget(key, "base")

if __name__ == '__main__':
    # 逻辑
    with SSHTunnelForwarder(ssh_address_or_host=(fromDBCfg.sshHost, fromDBCfg.sshPort),
                            ssh_username=fromDBCfg.sshUser,
                            ssh_password=fromDBCfg.sshPass,
                            local_bind_address=(fromDBCfg.redisHost, fromDBCfg.redisPort),
                            remote_bind_address=(fromDBCfg.redisHost, fromDBCfg.redisPort)) as remote:
        # 连接数据库
        r = redis.Redis(host=fromDBCfg.redisHost, port=fromDBCfg.redisPort, db=0)

        # 获取guid
        guid = getGUID(r, fromUserName)
        if guid == "":
            print("没有找到角色[%s]" % fromUserName)
            exit()
        userData = getUserData(r, guid)

        # 写文件
        file = open(dumpFile, 'wb')
        file.write(userData)
        file.close()

        print("导出角色[%s]到文件[%s]完成" % (fromUserName, dumpFile))



