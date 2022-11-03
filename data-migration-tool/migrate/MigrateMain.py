#!/usr/bin/python3
# encoding: utf-8
'''
Descripttion: 从GreatDB到OpenSearch全量同步数据
Author: wushuai
version: 1.0.0
Date: 2022-07-25 17:04:00
LastEditors: wushuai
LastEditTime: 2022-11-03 18:55:37
'''
from common.Common import logger, conf_reader
from common.DesEncrypt import *
from common.MysqlClient import MysqlClient
from common.ElasticSearchClient import ElasticSearchClient
from MigrateWork import MigrateWork
from IniESIndex import IniESIndex
from datetime import datetime
import os

class MigrateMain(object):
    def geneMysqlClient(self, config):
        '''
        初始化MySQL连接
        '''
        config['password'] = des_descrypt(config['password'])
        return MysqlClient(**config)

    def geneESClient(self, config):
        '''
        初始化ElasticSearch连接
        '''
        for host in config['hosts']:
            host['http_auth'] = des_descrypt(host['http_auth'])
        return ElasticSearchClient(**config)

    def start(self):
        # 最大线程数
        threadMaxWorkers = int(conf_reader["business"]["threadmaxworkers"])
        threadMaxBound = int(conf_reader['business']['threadmaxbound'])
        # 线程名前缀
        threadNamePrefix = conf_reader["business"]["threadnameprefix"]
        # 最大数据行
        readMaxLine = int(conf_reader["business"]["readmaxline"])
        # 迁移表集合
        tables = conf_reader["business"]["tables"]
        # 失败数据保存文件
        errorFile = conf_reader['business']['errorfile']
        errorFile = errorFile.format(os.path.dirname(__file__), datetime.datetime.now().strftime('%Y-%m-%d'))
        # 生成MySQL连接
        mysqlConfig = conf_reader["mysql"]
        mysqlClient = self.geneMysqlClient(mysqlConfig)
        # 生成ElasticSearch连接
        esConfig = conf_reader['es']
        esClient = self.geneESClient(esConfig)
        
        # 正式运行作业
        init = IniESIndex(esClient, tables)
        init.start()
        MigrateWork(threadMaxWorkers, threadMaxBound, threadNamePrefix, readMaxLine, tables, mysqlClient, esClient, errorFile).start()
        init.recover()
        
