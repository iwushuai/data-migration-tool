#!/usr/bin/python3
# encoding: utf-8
'''
Descripttion: 从GreatDB到OpenSearch增量同步数据
Author: wushuai
version: 1.0.0
Date: 2022-11-03 15:03:08
LastEditors: wushuai
LastEditTime: 2022-11-03 19:01:17
'''


'''
ctl_connection_settings：群集保持的连接设置
                            架构信息
resume_stream：从位置开始事件或最新事件
                二进制日志或来自较早的可用事件
阻塞：当主站完成读取/发送二进制日志时，它将
            发送 EOF 而不是阻止连接。
only_events：允许的事件数组
ignored_events：忽略的事件数组
log_file：设置复制启动日志文件
log_pos：设置复制开始日志 pos（resume_stream应为
            真）
end_log_pos：设置复制结束日志位置
auto_position：使用 master_auto_position gtid 设置位置
only_tables：包含要监视的表的数组（仅有效
                在binlog_format行）
ignored_tables：包含要跳过的表的数组
only_schemas：包含要监视的架构的数组
ignored_schemas：包含要跳过的架构的数组
freeze_schema：如果为 true，则不支持 ALTER TABLE。它更快。
skip_to_timestamp：忽略所有事件，直到达到指定值
                    时间戳。
report_slave：在显示从属主机中报告从属主机。
slave_uuid：在显示从属主机中报告slave_uuid。
fail_on_table_metadata_unavailable：如果我们
                                    无法获取表信息
                                    row_events
slave_heartbeat：（秒）应掌握主动发送检测信号
                    连接。这也减少了 GTID 中的流量
                    复制恢复时的复制（以防
                    二进制日志中要跳过的许多事件）。看
                    MASTER_HEARTBEAT_PERIOD mysql 文档中的内容
                    对于语义
is_mariadb：指示它是 MariaDB 服务器的标志，与auto_position一起使用
        指向Mariadb特定的GTID。

'''

from pymysqlreplication import BinLogStreamReader
from queue import Queue
from binlogsync.BinlogStream import BinlogStream
from binlogsync.BinlogSyncWork import BinlogSyncWork

class BinlogSyncMain(object):
    def start(selft):
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
        queue_maxsize = 10000
    
        queue_logs = Queue(maxsize=queue_maxsize)
        stream = BinlogStream(queue_logs,master_host,master_port,master_user,master_pswd,start_time,stop_time,log_file,log_pos,only_schemas,only_tables)
        work = BinlogSyncWork(queue_logs)
        stream.start()
        work.start()
        stream.join()
        work.join()