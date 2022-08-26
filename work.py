#!/usr/bin/python3
# encoding: utf-8
'''
Descripttion: 
Author: wushuai
version: 1.0.0
Date: 2022-07-27 09:53:48
LastEditors: wushuai
LastEditTime: 2022-08-18 16:07:32
'''
from concurrent.futures import ThreadPoolExecutor, thread
import datetime
from email.policy import default
import threading
import time
from common import *
from mysql_client import MysqlClient
from es_client import ESClient
import json
import hashlib
from desencrypt import des_encrypt,des_descrypt
from bounded_executor import BoundedExecutor
import sys
import traceback
import decimal

class Work(object):
    def __init__(self, threadMaxWorkers, threadMaxBound, threadNamePrefix, readMaxLine, tables, mysqlClient, esClient, errorFile):
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
        # 失败数据保存文件
        self.errorFile = errorFile
        errorPath = os.path.dirname(errorFile)
        if not os.path.exists(errorPath):
            os.mkdir(errorPath)

    def queryFromMySQL(self, srcTable, desIndex, querSql, pageTotal):
        '''
        从MySQL查询数据
        '''
        currThreadName = threading.current_thread().getName()
        startTime = datetime.datetime.now()
        while True:
            try:
                datas = self.mysqlClient.getAll(querSql)
                break
            except Exception:
                logger.error("==>{}::{}::查询MySQL异常, 2秒后即将重试! querSql={}, 异常信息如下：\n{}".format(currThreadName, querSql, traceback.format_exc()))
                time.sleep(2)
        endTime = datetime.datetime.now()
        logger.info("==>{}::{}::查询MySQL完成! 耗时={}, srcTable={}, desIndex={}, querSql={}, pageTotal={}".format(currThreadName, srcTable, (endTime-startTime), srcTable, desIndex, querSql, pageTotal))
        return datas

    def saveToES(self, srcTable, desIndex, querSql, datas):
        '''
        向ElasticSearch保存数据
        '''
        currThreadName = threading.current_thread().getName()
        startTime = datetime.datetime.now()
        
        while True:
            try:  
                body = ""
                for data in datas:
                    # 日期类型 转换为 字符串类型;bytes转换字符串
                    for key in data.keys():
                        value = data[key]
                        if isinstance(value, datetime.date):
                            data[key] = value.strftime('%Y-%m-%d')
                        elif isinstance(value, datetime.datetime):
                            data[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                        elif isinstance(value, decimal.Decimal):
                            data[key] = float(value)
                        if isinstance(value, bytes):
                            data[key] = value.decode()
                    # 字典列表 转换为 json数组（字典类型单引号 转换为 json类型双引号，字典类型None 转换为 json类型null）。
                    data = json.dumps(data)
                    
                    # 生成批量插入报文。  
                    # 不显示指定id以增加性能。"_id": hashlib.md5(data.encode(encoding='UTF-8')).hexdigest()
                    body += json.dumps({"index": {"_index": desIndex}}) + "\n"
                    body += data + "\n"
                url = "/_bulk"
                ret = self.esClient.bulk(url, body)
                break
            except Exception:
                logger.error("==>{}::{}::保存ES异常, 2秒后即将重试! srcTable={}, desIndex={}, querSql={},  数据大小={}MB, 异常信息如下：\n{}".format(currThreadName, srcTable, srcTable, desIndex, querSql, sys.getsizeof(datas)/1024/1024, traceback.format_exc()))
                time.sleep(2)
        
        endTime = datetime.datetime.now()

        # 解析返回报文
        errors = []
        for item in ret.get('items'):
            status = item.get('index').get('status')
            if status != 201:
                error = item.get('index').get('error', default=None)
                errors.append(error)
        if len(errors) == 0:
            logger.info("==>{}::{}::保存ES完成! 耗时={}, 数据大小={}MB, querSql={}, desIndex={}".format(currThreadName, srcTable, (endTime-startTime), sys.getsizeof(datas)/1024/1024 , querSql, desIndex))
        else:
            errorMsg = "==>{}::{}::保存ES失败! 耗时={}, 数据大小={}MB, querSql={}, desIndex={}, errors={}".format(currThreadName, srcTable,  (endTime-startTime), sys.getsizeof(datas)/1024/1024 , querSql, desIndex, errors)
            logger.error(errorMsg)
            with open(self.errorFile, 'a+', encoding='UTF-8') as file:
                file.write(errorMsg)
                file.newlines()
                file.flush()
                file.close()
                
    def run(self, srcTable, desIndex, pageNum, pageSize, pageTotal):
        '''
        作业运行步骤: 查询MySQL原始库表,保存ES目标库表
        '''
        try:
            currThreadName = threading.current_thread().getName()
            logger.info("==>{}::{}::作业运行开始! srcTable={}, desIndex={}, pageNum={}, pageSize={}, pageTotal={}".format(currThreadName, srcTable, srcTable, desIndex, pageNum, pageSize, pageTotal))
            querSql = "select * from {} limit {},{}".format(srcTable, pageNum*pageSize, pageSize)
            datas = self.queryFromMySQL(srcTable, desIndex, querSql, pageTotal)
            self.saveToES(srcTable, desIndex, querSql, datas)
        except Exception:
            logger.error(traceback.format_exc())
        
    @logger.catch
    def start(self):
        '''
        通过查询数据总数,计算分页页数,然后交给作业线程run()进行并行处理。
        '''
        logger.info("=================Work::start()::批处理作业开始...=================")
        startTime = datetime.datetime.now()
        
        # 开启多线程有界执行器
        executor = BoundedExecutor(self.threadMaxWorkers, self.threadMaxBound, thread_name_prefix=self.threadNamePrefix)
        
        # 循环遍历迁移表
        pageSize = self.readMaxLine
        for table in self.tables:
            # 查询表数据总量total
            srcTable = table['srcTable']
            desIndex = table['desIndex']
            totalSql = "select count(0) as count from {}".format(srcTable)
            dataTotal = self.mysqlClient.getOne(totalSql)['count']
            pageTotal = (dataTotal//pageSize) if (dataTotal%pageSize)==0 else (dataTotal//pageSize+1) 
            # 推送作业队列
            for pageNum in range(pageTotal):
                future = executor.submit(self.run, srcTable, desIndex, pageNum, pageSize, pageTotal)
                logger.info("==>将作业推入队列: srcTable={}, desIndex={}, pageNum={}, pageSize={}, dataTotal={}, pageTotal={}".format(srcTable, desIndex, pageNum, pageSize, dataTotal, pageTotal))
        
        # 优雅关闭多线程有界执行器
        executor.shutdown()

        endTime = datetime.datetime.now()
        logger.info("==>work::start()::开始时间：{}".format(startTime))
        logger.info("==>work::start()::结束时间：{}".format(endTime))
        logger.info("==>work::start()::累计耗时：{}".format(endTime - startTime))
        logger.info("=================Work::start()::批处理作业结束！=================")
        
    

    

    