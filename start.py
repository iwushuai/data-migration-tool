#!/usr/bin/python3
# encoding: utf-8
'''
Descripttion: 
Author: wushuai
version: 1.0.0
Date: 2022-07-25 17:04:00
LastEditors: wushuai
LastEditTime: 2022-08-11 11:52:33
'''
from common import *
from desencrypt import *
from work import Work
from init import Init
from mysql_client import MysqlClient
from es_client import ESClient

def geneMysqlClient(config):
    '''
    初始化MySQL连接
    '''
    config['password'] = des_descrypt(config['password'])
    return MysqlClient(**config)

def geneESClient(config):
    '''
    初始化ElasticSearch连接
    '''
    for host in config['hosts']:
        host['http_auth'] = des_descrypt(host['http_auth'])
    return ESClient(**esConfig)

if __name__ == "__main__":
    # 最大线程数
    threadMaxWorkers = int(conf_reader["business"]["threadmaxworkers"])
    threadMaxBound = int(conf_reader['business']['threadmaxbound'])
    # 线程名前缀
    threadNamePrefix = conf_reader["business"]["threadnameprefix"]
    # 最大数据行
    readMaxLine = int(conf_reader["business"]["readmaxline"])
    # 迁移表集合
    tables = conf_reader["business"]["tables"]
    # 生成MySQL连接
    mysqlConfig = conf_reader["mysql"]
    mysqlClient = geneMysqlClient(mysqlConfig)
    # 生成ElasticSearch连接
    esConfig = conf_reader['es']
    esClient = geneESClient(esConfig)
    # 生成数据模型
    Init(esClient, tables).start()
    # 正式运行作业
    Work(threadMaxWorkers, threadMaxBound, threadNamePrefix, readMaxLine, tables, mysqlClient, esClient).start()
    