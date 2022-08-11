#!/usr/bin/python3
# encoding: utf-8
'''
Descripttion: 
Author: wushuai
version: 1.0.0
Date: 2022-07-27 09:53:48
LastEditors: wushuai
LastEditTime: 2022-08-11 11:40:31
'''
from concurrent.futures import ThreadPoolExecutor, thread
import datetime
import threading
from time import sleep, time
from common import *
from mysql_client import MysqlClient
from es_client import ESClient
import json
import hashlib
from desencrypt import des_encrypt,des_descrypt
from bounded_executor import BoundedExecutor

class Work(object):
    def __init__(self, threadMaxWorkers, threadMaxBound, threadNamePrefix, readMaxLine, tables, mysqlClient, esClient):
        '''
        初始化作业配置
        '''
        # 最大线程数
        self.threadMaxWorkers = threadMaxWorkers 
        self.threadMaxBound = threadMaxBound
        # 线程名前缀
        self.threadNamePrefix = threadNamePrefix
        # 最大数据行
        self.readMaxLine = readMaxLine
        # 迁移表集合（数组）
        self.tables = tables
        # 初始化MySQL连接池
        self.mysqlClient = mysqlClient
        # 初始化ElasticSearch连接池
        self.esClient = esClient

    def run(self, srcTable, desIndex, pageNum, pageSize):
        '''
        作业运行步骤: 查询MySQL原始库表,保存ES目标库表
        '''
        currThreadName = threading.current_thread().getName()
        
        # 从MySQL查询数据
        querSql = "select * from {} limit {},{}".format(srcTable, (pageNum-1)*pageSize, pageSize)
        datas = self.mysqlClient.getAll(querSql)

        # 向ElasticSearch保存数据，手动指定_id方式：PUT /[索引]/_doc/[id] ，自动指定_id方式：POST /[索引]/_doc
        body = ""
        for data in datas:
            # 日期类型 转换为 字符串类型;bytes转换字符串
            for key in data.keys():
                value = data[key]
                if isinstance(value, datetime.datetime):
                    data[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                if isinstance(value, bytes):
                    data[key] = value.decode()
            # 字典列表 转换为 json数组（字典类型单引号 转换为 json类型双引号，字典类型None 转换为 json类型null）。
            data = json.dumps(data)
            
            # 生成批量插入报文
            # ,"_id": hashlib.md5(data.encode(encoding='UTF-8')).hexdigest()
            body += json.dumps({"index": {"_index": desIndex}}) + "\n"
            body += data + "\n"
        url = "/_bulk"
        ret = self.esClient.bulk(url, body)
        logger.info("==>{}::{}::当前查询MySQL共计{}条。sql={}".format(currThreadName, srcTable, len(datas), querSql))
        logger.info("==>{}::{}::当前查询MySQL并且插入ES共计{}条，querSql={}, desIndex={}".format(currThreadName, srcTable, len(datas), querSql, desIndex))

    def start(self):
        '''
        通过查询数据总数,计算分页页数,然后交给作业线程run()进行并行处理。
        '''
        logger.info("=================Work::start()::批处理作业开始...=================")
        startTime = datetime.datetime.now()
        # 创建多线程有界执行器
        executor = BoundedExecutor(self.threadMaxWorkers, self.threadMaxBound, thread_name_prefix=self.threadNamePrefix)
        
        # 循环遍历迁移表
        pageSize = self.readMaxLine
        for table in self.tables:
            # 查询表数据总量total
            srcTable = table['srcTable']
            desIndex = table['desIndex']
            totalSql = "select count(0) as count from {}".format(srcTable)
            logger.debug("==>{}::查询数据量SQL：{}".format(srcTable, totalSql))
            
            dataTotal = self.mysqlClient.getOne(totalSql)['count']
            logger.debug("==>{}::查询数据量为{}。".format(srcTable, dataTotal))

            # 计算分页页数交给作业线程并行处理。 注意：python中//代表无小数除法，/代表有小数除法
            pageTotal = (dataTotal//pageSize) if (dataTotal%pageSize)==0 else (dataTotal//pageSize+1) 
            logger.debug("==>{}::计算页数为{}。".format(srcTable, pageTotal))
            for pageNum in range(1, pageTotal):
                executor.submit(self.run, srcTable, desIndex, pageNum, pageSize)
                logger.info("==>将作业推入队列: srcTable={}, desIndex={}, pageNum={}, pageSize={}".format(srcTable, desIndex, pageNum, pageSize))
        executor.shutdown()

        
        endTime = datetime.datetime.now()
        logger.info("==>work::start()::开始时间：{}".format(startTime))
        logger.info("==>work::start()::结束时间：{}".format(endTime))
        logger.info("==>work::start()::累计耗时：{}".format(endTime - startTime))
        logger.info("=================Work::start()::批处理作业结束！=================")
        
    

    

    