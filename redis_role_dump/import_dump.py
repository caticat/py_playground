# -*- coding:utf-8 -*-

'''
    从角色dump文件导入一个角色到指定账号
    步骤:
        1. 从外网运行dump_role.py导出角色数据
        2. 将外网导出的role_data.dump复制到role_dump/data目录下
        3. 运行import_dump.py到付覆盖现有角色数据
'''

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

dumpFile = './data/role_data.dump'
toDBCfg = DBConfig('192.168.111.112', 22, 'username', 'password', '127.0.0.1', 9898) # 目标数据库配置
toUserName = '角色名'
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
    # 读取dump文件
    file = open(dumpFile, 'rb')
    userData = file.read()
    file.close()

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

    print("角色复制[%s]->[%s:%s][%s][%s]" % (dumpFile, toDBCfg.sshHost, toDBCfg.sshPort, toUserName, ret == 0 and '成功' or '失败'))


