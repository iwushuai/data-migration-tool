#!/usr/bin/python3
# encoding: utf-8
'''
Descripttion: 
Author: wushuai
version: 1.0.0
Date: 2022-08-11 11:30:51
LastEditors: wushuai
LastEditTime: 2022-11-03 17:29:00
'''
from loguru import logger
import datetime
import os
import yaml 

"""
日志配置(支持多线程安全！！)
"""
# 日志文件
log_file_name = 'data-migration-tool-{}.log'.format(datetime.datetime.now().strftime('%Y%m%d'))
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", log_file_name)
#logger.remove(handler_id=None)  # 清除logger之前的句柄
logger.add(log_file_path,  # 可以带有路径 没路径的logger会自己创建
            rotation='1024 MB',  # 按文件大小切割日志
            enqueue=True,  # 这使得进程安全
            colorize=True,  # 彩色显示 只要系统支持
            level='DEBUG')  # 这个级别以上的才会被写入文件，包含这个级别的


"""
.yml配置文件用法
1. 引入from global_configy import config_reader
2. 通过config_reader[key][key]...读取数据
"""
file_path = os.path.join(os.path.dirname(__file__), "conf", "config.yml")
datas = open(file=file_path, encoding='utf-8').read()
conf_reader = yaml.load(datas, Loader=yaml.SafeLoader)


'''
.conf配置文件用法
1. 引入from global_configy import config_reader
2. 通过config_reader.get([section], [option])读取数据
'''
# file_path = os.path.join(os.path.dirname(__file__), "conf", "config.conf")
# config_reader = configparser.RawConfigParser() 
# config_reader.read(file_path, "utf-8")