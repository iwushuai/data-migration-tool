#!/usr/bin/python3
# encoding: utf-8
'''
Descripttion: 
Author: wushuai
version: 1.0.0
Date: 2022-08-12 10:29:50
LastEditors: wushuai
LastEditTime: 2022-08-15 17:55:26
可以输入预定的版权声明、个性签名、空行等
'''
from common import logger
import os 
import datetime

if __name__ == "__main__":
    logfile = '{}/logs/data-migration-tool-{}.log'.format(os.path.dirname(__file__), datetime.datetime.now().strftime('%Y%m%d'))

    logger.info('==>::作业!运行!开始 运行次数：')
    os.system('cat {}  |grep "::作业运行开始!" |wc -l'.format(logfile))

    logger.info('==>::查询!MySQL!完成! 运行次数：')
    os.system('cat {}  |grep "::查询MySQL完成!" |wc -l'.format(logfile))

    logger.info('==>::保存!ES!完成! 运行次数：')
    os.system('cat {}  |grep "::保存ES完成!" |wc -l'.format(logfile))

    logger.info('==>::保存!ES!失败! 运行次数：')
    os.system('cat {}  |grep "::保存ES失败!" |wc -l'.format(logfile))
