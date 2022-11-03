#!/usr/bin/python3
# encoding: utf-8
'''
Descripttion: 多线程有界执行器
Author: wushuai
version: 1.0.0
Date: 2022-08-09 10:47:02
LastEditors: wushuai
LastEditTime: 2022-08-11 11:03:52
'''

from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore


class BoundedExecutor(object):

    """ 
    BoundedExecutor 表现为 ThreadPoolExecutor 将阻塞。一旦作为“绑定”工作项的限制被排队等待，就调用 submit() 执行。

    :param max_workers: Integer - 线程池的大小
    :param bound: Integer - 工作队列中的最大项目数
    """
    def __init__(self, max_workers, bound, thread_name_prefix=''):
        self.executor  = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=thread_name_prefix)
        self.semaphore = BoundedSemaphore(bound + max_workers)
    

    def submit(self, fn, *args, **kwargs):
        self.semaphore.acquire()
        try:
            future = self.executor.submit(fn, *args, **kwargs)
        except:
            self.semaphore.release()
            raise
        else:
            future.add_done_callback(lambda x: self.semaphore.release())
            return future

    def shutdown(self, wait=True):
        self.executor.shutdown(wait)