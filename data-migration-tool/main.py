#!/usr/bin/python3
# encoding: utf-8
'''
Descripttion: 主函数
Author: wushuai
version: 1.0.0
Date: 2022-11-03 17:16:41
LastEditors: wushuai
LastEditTime: 2022-11-03 18:25:22
'''

import sys
from fullsync.fullsync import FullSync
from incrsync.incrsync import IncrSync

if __name__ == "__main__":
    argvs = sys.argv
    for argv in argvs:
        if argv == 'fullsync':
            FullSync().start()
        elif argv == 'incrsync':
            IncrSync().start()
        
    
    
