#!/usr/bin/python3
# encoding: utf-8
'''
Descripttion: 
Author: wushuai
version: 1.0.0
Date: 2022-11-03 18:37:03
LastEditors: wushuai
LastEditTime: 2022-11-03 19:01:12
'''

from threading import Thread
from queue import Queue
from time import strptime,mktime,localtime,strftime
from pymysqlreplication import BinLogStreamReader   #pip3 install mysql-replication==0.21
from pymysqlreplication.row_event import DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent

#读取队列中解析后的日志，然后对日志进行应用
class BinlogSyncWork(Thread):
    def __init__(self,queue_logs):
        self._Queue = queue_logs
        super(ReadBinlog, self).__init__()
 
    def app(self,log):
        print(log)
 
    def run(self):
        while True:
            log = self._Queue.get()
            if log == "PutEnd.":break
            self.app(log)