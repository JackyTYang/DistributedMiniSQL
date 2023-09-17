# -*- coding: UTF-8 -*-
import hashlib
import logging
import math
import parser
import random
import re
import sys
import time
from threading import Condition
from kazoo.client import KazooClient

hosts = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
logging.basicConfig(level=logging.WARNING, stream=sys.stdout)
# 创建一个客户端，可以指定多台zookeeper
# zk = KazooClient(hosts=hosts, logger=logging)
server_list = ["minisql1", "minisql2", "minisql3"]
condition = Condition()
dataWatchFinished = 0
condition_for_file = Condition()
dataWatchFinished_for_file = 0



# 脚本执行的配套监听函数
def watch_result_node_for_file(data, stat):
    global dataWatchFinished_for_file
    if stat and data:
        condition_for_file.acquire()
        data_str = data.decode("utf-8")
        print(data_str)
        dataWatchFinished_for_file += 1
        condition_for_file.notify()
        condition_for_file.release()


# 根据随机数，指令，时间哈希生成节点名，返回路径列表
def get_path_list(target_server, sql):
    ans = []
    m = hashlib.sha256()
    for server in target_server:
        m.update((str(random.random()) + sql + str(time.time())).encode('utf-8'))
        node_name = m.hexdigest()
        ans.append('/servers/{}/instructions/{}'.format(server, node_name))
    return ans


def delete_finished_node(path_list):
    for path in path_list:
        if zookeeperClient.zk.exists(path):
            zookeeperClient.zk.delete(path, recursive=True)


# 写入指令， 同时监听result节点
def set_sql_and_watchers(path_list, sql, watch_function):
    for path in path_list:
        zookeeperClient.zk.create(path, sql)
        zookeeperClient.zk.ensure_path(path + '/result')
        zookeeperClient.zk.DataWatch(path + '/result', watch_function)


# 当节点kazoo的数据变化时这个函数会被调用
# 如果节点被删除这个函数也会被调用，但是data和stat都是None
def watch_result_node(data, stat):
    global dataWatchFinished
    if stat and data:
        condition.acquire()
        data_str = data.decode("utf-8")
        print(data_str)
        dataWatchFinished += 1
        condition.notify()
        condition.release()


class Client:
    def __init__(self, hosts, logging, server_list):
        self.hosts = hosts
        self.logger = logging
        self.server_list = server_list
        self.zk = KazooClient(hosts=hosts, logger=logging)

    def cmd_get_sql(self):  # 客户端功能
        sql = ''
        s = input('MiniSQL>  ')
        while True:
            if s.rstrip().endswith(';'):
                sql += ' ' + s
                return sql
            else:
                sql += ' ' + s
                s = input()

    # 获取目标服务器列表，客户端功能
    def get_target_server(self, sql):
        tmp = re.sub(r'[;()]', ' ', sql).strip(' ').split()
        ans = []
        if tmp[0] == 'create':  # backup
            if tmp[1] == 'table':
                # ans = get_create_table_server(2, tmp[2]) # 采用两台MinisqlServer作为副本存储一张表
                ans = LoadBalance.get_create_table_server(zookeeperClient.zk, 2, tmp[2], server_list,
                                                          zookeeperClient.take_weight)  # 采用两台MinisqlServer作为副本存储一张表
            elif tmp[1] == 'index':
                ans = LoadBalance.get_normal_server(tmp[4])
        elif tmp[0] == 'select':  # balancing
            ans = LoadBalance.get_select_server(tmp[3])
        elif tmp[0] == 'drop' and tmp[1] == 'index':
            ans = LoadBalance.get_drop_index_server(tmp[2])
        else:
            ans = LoadBalance.get_normal_server(tmp[2])
        return ans

    # 执行一条文件中的sql命令，客户端功能
    def execute_one_sql(self, sql):
        global dataWatchFinished_for_file
        condition_for_file.acquire()
        dataWatchFinished_for_file = 0
        target_server = Client.get_target_server(self, sql)
        print(target_server)
        path_list = get_path_list(target_server, sql)
        set_sql_and_watchers(path_list, bytes(sql, encoding="utf-8"), watch_result_node_for_file)
        while dataWatchFinished_for_file != len(target_server):
            condition_for_file.wait()
        delete_finished_node(path_list)
        condition_for_file.release()

    # 执行整个脚本文件，客户端功能
    def execute_file(self, filename):
        with open(filename, 'r') as file:
            lines = (line.strip() for line in file.readlines())
            sql = ''
            for line in lines:
                if line.rstrip().endswith(';'):
                    sql += ' ' + line
                    Client.execute_one_sql(self, sql)
                    sql = ''
                else:
                    sql += ' ' + line

    # 排序辅助函数，客户端功能
    def take_weight(self, elem):
        return elem['weight']

class LoadBalance:

    @staticmethod
    def get_normal_server(table_name):  # 找表对应的服务器
        if not zookeeperClient.zk.exists("/tables/" + table_name):
            print('Table not exists.')
            return []
        else:
            return zookeeperClient.zk.get_children("/tables/" + table_name)

    @staticmethod
    def get_create_table_server(zk, num, table_name, server_list, take_weight):  # 负载均衡，建表
        if zk.exists("/tables/" + table_name):
            return zookeeperClient.zk.get_children("/tables/" + table_name)
        candidate = []
        for server in server_list:  # 根据每一个Server中的表的数量以及记录数量的权重，选择最小的，作为创建新表的服务器
            data1, stat1 = zookeeperClient.zk.get("/servers/{}/info/recordNum".format(server))
            data_str1 = data1.decode('utf-8')
            data2, stat2 = zookeeperClient.zk.get("/servers/{}/info/tableNum".format(server))
            data_str2 = data2.decode('utf-8')
            tmp = int(data_str1) + int(data_str2)
            candidate.append({'server': server, 'weight': tmp})
        candidate.sort(key=take_weight)
        ans = []
        for i in range(num):
            ans.append(candidate[i]['server'])
        return ans

    @staticmethod
    def get_create_index_server(table_name):  # 副本管理，建索引
        i = table_name.find('(')
        if not i == -1:
            table_name = table_name[:table_name.find('(')]
        return LoadBalance.get_normal_server(table_name)

    @staticmethod
    def get_drop_index_server(index_name):  # 副本管理，删除索引
        if not zookeeperClient.zk.exists("/indexes/" + index_name):
            print('Index not exists.')
            return []
        else:
            ans = zookeeperClient.zk.get_children("/indexes/" + index_name)
            return ans

    @staticmethod
    def get_select_server(table_name):  # 负载均衡
        if not zookeeperClient.zk.exists("/tables/" + table_name):
            print('Table not exists.')
            return []
        else:
            ans = [zookeeperClient.zk.get_children("/tables/" + table_name)[
                       math.floor(random.random() * 2)]]  # 已有的两个服务器中选一个进行读取
            return ans



if __name__ == '__main__':
    zookeeperClient = Client(hosts, logging, server_list)
    zookeeperClient.zk.start()
    # zk.start()
    target_server = []
    path_list = []
    sql_input = ''

    while True:
        condition.acquire()
        dataWatchFinished = 0
        # 读取sql指令
        sql_input = zookeeperClient.cmd_get_sql()
        sql_tmp = sql_input.replace(';', '').strip().split()
        # 如果是文件处理，转处理函数
        if sql_tmp[0] == 'exec':
            zookeeperClient.execute_file(sql_tmp[1])
            target_server.clear()
            path_list.clear()
            dataWatchFinished = 0
        # 一般指令
        else:
            # 获取应该写入的服务器列表
            target_server = zookeeperClient.get_target_server(sql_input)
            # target_server = ["test"]
            print(target_server)
            # 生成具体zookeeper路径，指令节点名由哈希获得
            path_list = get_path_list(target_server, sql_input)
            # 写入指令， 同时监听result节点
            set_sql_and_watchers(path_list, bytes(sql_input, encoding="utf-8"), watch_result_node)
        # 等待所有服务器返回结果
        while dataWatchFinished != len(target_server):
            condition.wait()
        # 删除指令节点
        delete_finished_node(path_list)
        condition.release()
