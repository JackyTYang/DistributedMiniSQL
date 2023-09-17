import hashlib
import logging
import math
import random
import re
import sys
import os
import time
from time import sleep

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from kazoo.client import KazooClient
from configs.config import zookeeperConfig
from minisql_cluster.src.interpreter import parser, clear_result, get_result, zookeeper_result, get_result_flag

class Master:
    hosts = zookeeperConfig['hosts']
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    zk = KazooClient(hosts=hosts, logger=logging)
    server_name = sys.argv[1]
    server_path = "/servers/" + server_name
    server_num = 0
    server_list = []

    # def __init__(self):
    #     self.hosts = zookeeperConfig['hosts']
    #     logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    #     self.zk = KazooClient(hosts=self.hosts, logger=logging)
    #     self.server_name = sys.argv[1]
    #     self.server_path = "/servers/" + self.server_name
    #     self.server_num = 0
    #     self.server_list = []

    # 删除断线服务器相关节点
    @staticmethod
    def delete_server_node(offline_server_name):
        Master.zk.delete('/servers/{}'.format(offline_server_name), recursive=True)
        tables = Master.zk.get_children('{}/tables'.format(Master.server_path))
        for table in tables:
            if offline_server_name in Master.zk.get_children('/tables/{}'.format(table)):
                Master.zk.delete('/tables/{}/{}'.format(table, offline_server_name))

    # 确定复制到哪台服务器
    @staticmethod
    def copy_server(offline_server_name):
        table_list = Master.zk.get_children('/tables')
        remained_server_list = Master.server_list[:]
        remained_server_list.remove(Master.server_name)
        remained_server_list.remove(offline_server_name)
        for table in table_list:
            children = Master.zk.get_children('/tables/{}'.format(table))
            if Master.server_name in children and offline_server_name in children:
                random_index = math.floor(random.random() * len(remained_server_list))
                Master.copy_table(table, remained_server_list[random_index])

    # 生成复制一张表的所有指令，并写入目标服务器
    @staticmethod
    def copy_table(table_name, target_server_name):
        # 生成create相关指令
        instruction_name_list = Master.zk.get_children('{}/tables/{}'.format(Master.server_path, table_name))
        instruction_list = []
        for instruction_name in instruction_name_list:
            data, stat = Master.zk.get('{}/tables/{}/{}'.format(Master.server_path, table_name, instruction_name))

            instruction_list.append(
                {'name': instruction_name, 'content': bytes('copy ' + data.decode('utf-8'), encoding='utf-8')})
        # 生成insert相关指令
        parser.parse('select * from {};'.format(table_name))
        result = get_result()
        clear_result()
        tmp = result.decode(encoding='utf-8').replace('|', '').split('\n')
        template_str = 'insert into {} values ({});'
        m = hashlib.sha256()
        digital = re.compile(r'^[-+]?[0-9]+\.*[0-9]*$')
        for i in range(len(tmp)):
            if i > 2 and tmp[i]:
                s = tmp[i].split()
                for j in range(len(s)):
                    if not digital.match(s[j]):
                        s[j] = "'" + s[j] + "'"
                sql_str = template_str.format(table_name, ', '.join(s))
                m.update((str(random.random()) + sql_str + str(time.time())).encode('utf-8'))
                instruction_list.append({'name': m.hexdigest(), 'content': bytes('copy ' + sql_str, encoding='utf-8')})
        for instruction in instruction_list:
            sleep(0.1)
            Master.zk.create('/servers/{}/instructions/{}'.format(target_server_name, instruction['name']),
                             instruction['content'])

    # 监听是否有服务器断线
    @staticmethod
    def watch_server_party(children):
        # global server_num
        # global server_list
        if Master.server_num < len(children):
            Master.server_num = len(children)
            Master.server_list.clear()
            for ch in children:
                data, stat = Master.zk.get('/party/{}'.format(ch))
                Master.server_list.append(data.decode('utf-8'))
        elif Master.server_num > len(children):
            cur_server_list = []
            for ch in children:
                data, stat = Master.zk.get('/party/{}'.format(ch))
                cur_server_list.append(data.decode('utf-8'))
            for server in Master.server_list:
                if server not in cur_server_list:
                    Master.copy_server(server)
                    Master.delete_server_node(server)
            Master.server_list = cur_server_list[:]

    @staticmethod
    def watch_instruction_children(children):
        for ch in children:
            data, stat = Master.zk.get('{}/instructions/{}'.format(Master.server_path, ch))
            # copy_flag 用于判断是否是容错容灾导致的表复制
            copy_flag = False
            if data and stat:
                data_str = data.decode('utf-8')
                print(data_str)
                # 指令以copy起始代表跟容错容灾相关
                if data_str.find('copy') == 0:
                    copy_flag = True
                    data_str = data_str.replace('copy', '')
                parser.parse(data_str)
                # 如果执行成功，更新相关信息 /info
                if get_result_flag():
                    Master.update_info(data_str, ch)
                # 如果是容错容灾相关，则不需要写回结果，直接删除指令节点
                if copy_flag:
                    Master.zk.delete('{}/instructions/{}'.format(Master.server_path, ch), recursive=True)
                    print(get_result())
                    clear_result()
                # 正常情况下，需要写回指令
                else:
                    Master.zk.ensure_path('{}/instructions/{}/result'.format(Master.server_path, ch))
                    zookeeper_result(Master.zk, '{}/instructions/{}/result'.format(Master.server_path, ch),
                                     Master.server_name)

    # 处理各种指令导致的信息更新，delete由于minisql的问题还不完善
    @staticmethod
    def update_info(sql, node_name):
        tmp = re.sub(r'[;()]', ' ', sql).strip(' ').split()
        # server_path = Master.server_path
        if tmp[0] == 'create':
            if tmp[1] == 'table':
                data, stat = Master.zk.get('{}/info/tableNum'.format(Master.server_path))
                num = int(data.decode('utf-8')) + 1
                Master.zk.set('{}/info/tableNum'.format(Master.server_path), bytes(str(num), encoding='utf-8'))
                Master.zk.ensure_path('/tables/{}/{}'.format(tmp[2], Master.server_name))
                Master.zk.ensure_path('{}/tables/{}/{}'.format(Master.server_path, tmp[2], node_name))
                Master.zk.set('{}/tables/{}/{}'.format(Master.server_path, tmp[2], node_name),
                              bytes(sql, encoding='utf-8'))
            elif tmp[1] == 'index':
                Master.zk.ensure_path('/indexes/{}/{}'.format(tmp[2], Master.server_name))
                Master.zk.ensure_path('{}/tables/{}/{}'.format(Master.server_path, tmp[4], node_name))
                Master.zk.set('{}/tables/{}/{}'.format(Master.server_path, tmp[4], node_name),
                              bytes(sql, encoding='utf-8'))
        elif tmp[0] == 'insert':
            data, stat = Master.zk.get('{}/info/recordNum'.format(Master.server_path))
            num = int(data.decode('utf-8')) + 1
            Master.zk.set('{}/info/recordNum'.format(Master.server_path), bytes(str(num), encoding='utf-8'))
        elif tmp[0] == 'delete':
            data, stat = Master.zk.get('{}/info/recordNum'.format(Master.server_path))
            num = int(data.decode('utf-8')) - 1
            Master.zk.set('{}/info/recordNum'.format(Master.server_path), bytes(str(num), encoding='utf-8'))
        elif tmp[0] == 'drop':
            if tmp[1] == 'table':
                data, stat = Master.zk.get('{}/info/tableNum'.format(Master.server_path))
                num = int(data.decode('utf-8')) - 1
                Master.zk.set('{}/info/tableNum'.format(Master.server_path), bytes(str(num), encoding='utf-8'))
                if Master.zk.exists('/tables/{}'.format(tmp[2])):
                    Master.zk.delete('/tables/{}'.format(tmp[2]), recursive=True)
                if Master.zk.exists('{}/tables/{}'.format(Master.server_path, tmp[2])):
                    Master.zk.delete('{}/tables/{}'.format(Master.server_path, tmp[2]), recursive=True)
            elif tmp[1] == 'index':
                if Master.zk.exists('/indexes/{}'.format(tmp[2])):
                    Master.zk.delete('/indexes/{}'.format(tmp[2]), recursive=True)
                # zk.create('{}/tables/{}/{}'.format(server_path, tmp[2], node_name), bytes(sql, encoding='utf-8'))


if __name__ == '__main__':
    # 开始心跳
    Master.zk.start()
    # party部分，用于检测服务器断线情况
    Master.zk.ensure_path('/party')
    Master.zk.ChildrenWatch('/party', Master.watch_server_party)
    party = Master.zk.Party('/party', Master.server_name)
    party.join()

    # 构建zookeeper结构
    Master.zk.ensure_path('/tables')
    Master.zk.ensure_path('/indexes')
    Master.zk.ensure_path("{}/tables".format(Master.server_path))
    Master.zk.set("{}".format(Master.server_path), b'0')
    Master.zk.ensure_path("{}/info".format(Master.server_path))
    if not Master.zk.exists("{}/info/recordNum".format(Master.server_path)):
        Master.zk.create("{}/info/recordNum".format(Master.server_path), b'0')
    if not Master.zk.exists("{}/info/tableNum".format(Master.server_path)):
        Master.zk.create("{}/info/tableNum".format(Master.server_path), b'0')
    Master.zk.delete("{}/instructions".format(Master.server_path), recursive=True)
    Master.zk.ensure_path("{}/instructions".format(Master.server_path))
    # 监听指令节点
    Master.zk.ChildrenWatch(Master.server_path + "/instructions", Master.watch_instruction_children)

    while True:
        sleep(60)
        print("Watching...")
