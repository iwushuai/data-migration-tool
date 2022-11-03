#!/usr/bin/env python3
# _*_ coding:utf8 _*_
from threading import Thread
from queue import Queue
from time import strptime,mktime,localtime,strftime
from pymysqlreplication import BinLogStreamReader   #pip3 install mysql-replication==0.21
from pymysqlreplication.row_event import DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent
 
##从主库读取binlog，然后将event解析，将解析结果放队列中
class BinlogStream(Thread):
    def __init__(self,
                 queue_logs,                                             #队列
                 master_host,master_port,master_user,master_pswd,        #主库配置
                 start_time, stop_time,                                  #开始结束时间,格式:YYYY-mm-dd HH:MM:SS
                 log_file,log_pos=0,                                     #开始binlog文件位置
                 only_schemas = None, only_tables = None                 #只监听指定的库和表,格式:['db1','db2']
                 ):
        self._Queue = queue_logs
        self.connect = {'host': master_host, 'port': master_port, 'user': master_user,'passwd': master_pswd}
        self.log_file,self.log_pos = log_file,log_pos
        self.start_time = int(mktime(strptime(start_time, "%Y-%m-%d %H:%M:%S")))
        self.stop_time = int(mktime(strptime(stop_time, "%Y-%m-%d %H:%M:%S")))
        self.only_schemas,self.only_tables = only_schemas,only_tables
        self.only_events = [DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent]
        super(BinlogStream, self).__init__()
 
    def insert(self,event):
        for row in event.rows:
            self._Queue.put({
                "log_pos":event.packet.log_pos,
                "log_time": strftime("%Y-%m-%dT%H:%M:%S",localtime(event.timestamp)),
                "schema_table":"%s.%s" % (event.schema, event.table),
                "table_pk":event.primary_key,
                "op_type":"insert",
                "values":row.get("values")
            })
 
    def update(self,event):
        for row in event.rows:
            self._Queue.put({
                "log_pos":event.packet.log_pos,
                "log_time": strftime("%Y-%m-%dT%H:%M:%S",localtime(event.timestamp)),
                "schema_table":"%s.%s" % (event.schema, event.table),
                "table_pk":event.primary_key,
                "op_type":"update",
                "before_values":row.get("before_values"),
                "after_values":row.get("after_values")
                })
 
    def delete(self,event):
        for row in event.rows:
            self._Queue.put({
                "log_pos": event.packet.log_pos,
                "log_time": strftime("%Y-%m-%dT%H:%M:%S",localtime(event.timestamp)),
                "schema_table": "%s.%s" % (event.schema, event.table),
                "table_pk": event.primary_key,
                "op_type": "delete",
                "values": row.get("values")
            })
 
    def run(self):
        stream = BinLogStreamReader(connection_settings=self.connect, server_id=999, only_events=self.only_events,log_file=self.log_file, log_pos=self.log_pos,only_schemas=self.only_schemas,only_tables=self.only_tables)
        for event in stream:
            if event.timestamp < self.start_time:continue
            elif event.timestamp > self.stop_time:break
            if isinstance(event, UpdateRowsEvent):
                self.update(event)
            elif isinstance(event, WriteRowsEvent):
                self.insert(event)
            elif isinstance(event, DeleteRowsEvent):
                self.delete(event)
        self._Queue.put("PutEnd.")
 
#读取队列中解析后的日志，然后对日志进行应用
class ReadBinlog(Thread):
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
 
 
if __name__ == '__main__':
    master_host = "127.0.0.1"
    master_port = 3306
    master_user = "root"
    master_pswd = "123456"
    start_time = "2020-11-06 15:00:00"
    stop_time = "2020-11-06 17:00:00"
    log_file = "mysqlbinlog.000012"
    log_pos = 0
    only_schemas = None
    only_tables = None
 
    queue_logs = Queue(maxsize=10000)
    BS = BinlogStream(queue_logs,master_host,master_port,master_user,master_pswd,start_time,stop_time,log_file,log_pos,only_schemas,only_tables)
    RB = ReadBinlog(queue_logs)
    BS.start()
    RB.start()
    BS.join()
    RB.join()
 