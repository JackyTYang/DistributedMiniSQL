import hashlib
import logging
import math
import random
import re
import sys
import time
from time import sleep

from kazoo.client import KazooClient

# hosts = '172.16.238.2:2181,172.16.238.3:2182,172.16.238.4:2183'
from interpreter import parser, clear_result, get_result, zookeeper_result, get_result_flag

hosts = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
# test_hosts = '127.0.0.1:2181'
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
server_name = sys.argv[1]
# server_name = "test"
server_path = "/servers/" + server_name
# 创建一个客户端，可以指定多台zookeeper，
zk = KazooClient(hosts=hosts, logger=logging)
server_num = 0
server_list = []


# 删除断线服务器相关节点
def delete_server_node(offline_server_name):
    zk.delete('/servers/{}'.format(offline_server_name), recursive=True)
    tables = zk.get_children('{}/tables'.format(server_path))
    for table in tables:
        if offline_server_name in zk.get_children('/tables/{}'.format(table)):
            zk.delete('/tables/{}/{}'.format(table, offline_server_name))


# 确定复制到哪台服务器
def copy_server(offline_server_name):
    table_list = zk.get_children('/tables')
    remained_server_list = server_list[:]
    remained_server_list.remove(server_name)
    remained_server_list.remove(offline_server_name)
    for table in table_list:
        children = zk.get_children('/tables/{}'.format(table))
        if server_name in children and offline_server_name in children:
            random_index = math.floor(random.random() * len(remained_server_list))
            copy_table(table, remained_server_list[random_index])


# 生成复制一张表的所有指令，并写入目标服务器
def copy_table(table_name, target_server_name):
    # 生成create相关指令
    instruction_name_list = zk.get_children('{}/tables/{}'.format(server_path, table_name))
    instruction_list = []
    for instruction_name in instruction_name_list:
        data, stat = zk.get('{}/tables/{}/{}'.format(server_path, table_name, instruction_name))

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
        zk.create('/servers/{}/instructions/{}'.format(target_server_name, instruction['name']), instruction['content'])


# 监听是否有服务器断线
def watch_server_party(children):
    global server_num
    global server_list
    if server_num < len(children):
        server_num = len(children)
        server_list.clear()
        for ch in children:
            data, stat = zk.get('/party/{}'.format(ch))
            server_list.append(data.decode('utf-8'))
    elif server_num > len(children):
        cur_server_list = []
        for ch in children:
            data, stat = zk.get('/party/{}'.format(ch))
            cur_server_list.append(data.decode('utf-8'))
        for server in server_list:
            if server not in cur_server_list:
                copy_server(server)
                delete_server_node(server)
        server_list = cur_server_list[:]


def watch_instruction_children(children):
    for ch in children:
        data, stat = zk.get('{}/instructions/{}'.format(server_path, ch))
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
                update_info(data_str, ch)
            # 如果是容错容灾相关，则不需要写回结果，直接删除指令节点
            if copy_flag:
                zk.delete('{}/instructions/{}'.format(server_path, ch), recursive=True)
                print(get_result())
                clear_result()
            # 正常情况下，需要写回指令
            else:
                zk.ensure_path('{}/instructions/{}/result'.format(server_path, ch))
                zookeeper_result(zk, '{}/instructions/{}/result'.format(server_path, ch), server_name)


# 处理各种指令导致的信息更新，delete由于minisql的问题还不完善
def update_info(sql, node_name):
    tmp = re.sub(r'[;()]', ' ', sql).strip(' ').split()
    if tmp[0] == 'create':
        if tmp[1] == 'table':
            data, stat = zk.get('{}/info/tableNum'.format(server_path))
            num = int(data.decode('utf-8')) + 1
            zk.set('{}/info/tableNum'.format(server_path), bytes(str(num), encoding='utf-8'))
            zk.ensure_path('/tables/{}/{}'.format(tmp[2], server_name))
            zk.ensure_path('{}/tables/{}/{}'.format(server_path, tmp[2], node_name))
            zk.set('{}/tables/{}/{}'.format(server_path, tmp[2], node_name), bytes(sql, encoding='utf-8'))
        elif tmp[1] == 'index':
            zk.ensure_path('/indexes/{}/{}'.format(tmp[2], server_name))
            zk.ensure_path('{}/tables/{}/{}'.format(server_path, tmp[4], node_name))
            zk.set('{}/tables/{}/{}'.format(server_path, tmp[4], node_name), bytes(sql, encoding='utf-8'))
    elif tmp[0] == 'insert':
        data, stat = zk.get('{}/info/recordNum'.format(server_path))
        num = int(data.decode('utf-8')) + 1
        zk.set('{}/info/recordNum'.format(server_path), bytes(str(num), encoding='utf-8'))
    elif tmp[0] == 'delete':
        data, stat = zk.get('{}/info/recordNum'.format(server_path))
        num = int(data.decode('utf-8')) - 1
        zk.set('{}/info/recordNum'.format(server_path), bytes(str(num), encoding='utf-8'))
    elif tmp[0] == 'drop':
        if tmp[1] == 'table':
            data, stat = zk.get('{}/info/tableNum'.format(server_path))
            num = int(data.decode('utf-8')) - 1
            zk.set('{}/info/tableNum'.format(server_path), bytes(str(num), encoding='utf-8'))
            if zk.exists('/tables/{}'.format(tmp[2])):
                zk.delete('/tables/{}'.format(tmp[2]), recursive=True)
            if zk.exists('{}/tables/{}'.format(server_path, tmp[2])):
                zk.delete('{}/tables/{}'.format(server_path, tmp[2]), recursive=True)
        elif tmp[1] == 'index':
            if zk.exists('/indexes/{}'.format(tmp[2])):
                zk.delete('/indexes/{}'.format(tmp[2]), recursive=True)
            # zk.create('{}/tables/{}/{}'.format(server_path, tmp[2], node_name), bytes(sql, encoding='utf-8'))


if __name__ == '__main__':
    # 开始心跳
    zk.start()
    # party部分，用于检测服务器断线情况
    zk.ensure_path('/party')
    zk.ChildrenWatch('/party', watch_server_party)
    party = zk.Party('/party', server_name)
    party.join()

    # 构建zookeeper结构
    zk.ensure_path('/tables')
    zk.ensure_path('/indexes')
    zk.ensure_path("{}/tables".format(server_path))
    zk.set("{}".format(server_path), b'0')
    zk.ensure_path("{}/info".format(server_path))
    if not zk.exists("{}/info/recordNum".format(server_path)):
        zk.create("{}/info/recordNum".format(server_path), b'0')
    if not zk.exists("{}/info/tableNum".format(server_path)):
        zk.create("{}/info/tableNum".format(server_path), b'0')
    zk.delete("{}/instructions".format(server_path), recursive=True)
    zk.ensure_path("{}/instructions".format(server_path))
    # 监听指令节点
    zk.ChildrenWatch(server_path + "/instructions", watch_instruction_children)

    while True:
        sleep(60)
        print("Watching...")
