import sys
import os
import socket
import logging

from kazoo.client import KazooClient

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from configs.config import zookeeperConfig
from minisql_cluster.src.interpreter import parser, clear_result, get_result, zookeeper_result, get_result_flag


class region:
    server_name = sys.argv[1]
    # server_name = 'minisql2'
    c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    hosts = zookeeperConfig['hosts']
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    zk = KazooClient(hosts=hosts, logger=logging)
    zk.start()

    while 1:
        if c.connect(('127.0.0.1', 8888)) == socket.error:
            print('FAILED')
        data = server_name.encode()
        c.send(data)
        result = c.recv(1024).decode()
        print(result)
        party = zk.Party('/party', server_name)
        party.join()
        break

    # 等待数据库操作指令
    while 1:
        data = "Instruction Accepted".encode()
        c.send(data)
        instruction = c.recv(1024).decode()
        print(instruction)
        parser.parse(instruction)



    # @staticmethod
    # def watch_instruction_children(children):
    #     for ch in children:
    #         data, stat = Master.zk.get('{}/instructions/{}'.format(Master.server_path, ch))
    #         # copy_flag 用于判断是否是容错容灾导致的表复制
    #         copy_flag = False
    #         if data and stat:
    #             data_str = data.decode('utf-8')
    #             print(data_str)
    #             # 指令以copy起始代表跟容错容灾相关
    #             if data_str.find('copy') == 0:
    #                 copy_flag = True
    #                 data_str = data_str.replace('copy', '')
    #             parser.parse(data_str)
    #             # 如果执行成功，更新相关信息 /info
    #             if get_result_flag():
    #                 Master.update_info(data_str, ch)
    #             # 如果是容错容灾相关，则不需要写回结果，直接删除指令节点
    #             if copy_flag:
    #                 Master.zk.delete('{}/instructions/{}'.format(Master.server_path, ch), recursive=True)
    #                 print(get_result())
    #                 clear_result()
    #             # 正常情况下，需要写回指令
    #             else:
    #                 Master.zk.ensure_path('{}/instructions/{}/result'.format(Master.server_path, ch))
    #                 zookeeper_result(Master.zk, '{}/instructions/{}/result'.format(Master.server_path, ch),
    #                                  Master.server_name)
