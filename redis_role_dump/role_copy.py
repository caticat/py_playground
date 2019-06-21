# -*- coding:utf-8 -*-

'''外网角色数据库之间复制'''

import redis, os
from sshtunnel import SSHTunnelForwarder

pathProto = 'D:/trunk/proto/in'
pathProtoBin = 'D:/trunk/tools/win-protoc/proto3'
cmd = "%s --python_out. --proto_path=%s %s/role.proto" % (pathProtoBin, pathProto, pathProto)
os.system(cmd)
import role

class DBConfig:
    def __init__(self, sshHost, sshPort, sshUser, sshPass, redisHost, redisPort):
        self.sshHost=sshHost
        self.sshPort=sshPort
        self.sshUser=sshUser
        self.sshPass=sshPass
        self.redisHost=redisHost
        self.redisPort=redisPort

fromDBCfg = DBConfig('192.168.111.111', 22, 'username', 'password', '127.0.0.1', 9898) # 外网个人的测试数据库1
toDBCfg = DBConfig('192.168.111.112', 22, 'username', 'password', '127.0.0.1', 9898) # 外网个人的测试数据库2
fromUserName = '角色名1'
toUserName = '角色名2'
toAccount = '账号名'

def getServerId(r):
    ret = r.keys("server:*")
    if len(ret) != 1:
        exit("不能指定服务器id:"+str(ret))
    key = ret[0].decode('utf8')
    idx = key.find(":")
    if idx < 0:
        exit("无法找到服务器id:"+key)
    id = key[idx+1:]
    return id

def getGUID(r, userName):
    res = r.hget("name", userName)
    if res == None:
        return ""
    else:
        return r.hget("name", userName).decode('utf8')

def getUserData(r, GUID):
    key = "u:%s" % GUID
    return r.hget(key, "base")

def setUserData(r, GUID, userData):
    key = "u:%s" % GUID
    return r.hset(key, "base", userData)

# 将源数据转化成可以使用的目标数据
def fixUserData(userData, GUID, userName, account, serverID):
    msg = role.UserDBData()
    msg.ParseFromString(userData)
    msg.base.id=int(GUID)
    msg.base.name=userName
    msg.base.uuid=account
    msg.base.server_id=int(serverID)
    return msg.SerializeToString()

if __name__ == '__main__':
    # 读取源数据
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

    # 写入目标数据
    with SSHTunnelForwarder(ssh_address_or_host=(toDBCfg.sshHost, toDBCfg.sshPort),
                            ssh_username=toDBCfg.sshUser,
                            ssh_password=toDBCfg.sshPass,
                            local_bind_address=(toDBCfg.redisHost, toDBCfg.redisPort),
                            remote_bind_address=(toDBCfg.redisHost, toDBCfg.redisPort)) as remote:
        # 连接数据库
        r = redis.Redis(host=toDBCfg.redisHost, port=toDBCfg.redisPort, db=0)

        # 覆盖目标角色
        guid = getGUID(r, toUserName)
        userData = fixUserData(userData, guid, toUserName, toAccount, getServerId(r))
        ret = setUserData(r, guid, userData)

    print("角色复制[%s:%s][%s]->[%s:%s][%s][%s]" % (fromDBCfg.sshHost, fromDBCfg.sshPort, fromUserName, toDBCfg.sshHost, toDBCfg.sshPort, toUserName, ret == 0 and '成功' or '失败'))


