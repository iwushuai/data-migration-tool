#!/usr/bin/python3
# encoding: utf-8
'''
Descripttion: 
Author: wushuai
version: 1.0.0
Date: 2022-08-09 14:55:35
LastEditors: wushuai
LastEditTime: 2022-08-10 11:18:59
可以输入预定的版权声明、个性签名、空行等
'''

import time
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from global_log import logger
import hashlib
import json

def timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        res = func(*args, **kwargs)
        print('共耗时约 {:.2f} 秒'.format(time.time() - start))
        print('===>res{}'.format(res))
        return res
    return wrapper
    
class ESClient(object):
    es = None

    def __init__(self, hosts, account, password):
        self.es = Elasticsearch(hosts=hosts, basic_auth=(account, password))
        logger.info(self.es.info())

    @timer
    def create_data(self):
        """ 写入数据 """
        for line in range(100):
            self.es.index(index='s2', doc_type='doc', body={'title': line})

    @timer
    def bulk(self, index, datas):
        '''
        批量插入数据
        '''
        body = ""
        for data in datas:
            body.append({"index": {
                    "_index": index,
                    #"_type": "_doc",
                    "_id": hashlib.md5(json.dumps(data).encode(encoding='UTF-8')).hexdigest()
                }
            })
            body.append(data)
        ret = self.es.bulk(body=body, index=index)

        #此方案失效，原因未知
        #ret = helpers.bulk(self.es, actions) 
        return ret


if __name__ == '__main__':
    #client = ESClient([{ 'host':'10.248.50.224', 'port': 9001}, { 'host':'10.248.50.224', 'port': 9002},{ 'host':'10.248.50.224', 'port': 9003}], 'elastic', '1qaz!qaz')
    data = {"name": "张三", "age": 12}
    ret = json.dumps(data).encode(encoding='UTF-8')
    ret = hashlib.md5(ret).hexdigest()
    print("===>{}".format(ret))
    
'''
在索引中创建一个新文档。当有文档时返回 409 响应索引中已存在具有相同 ID 的。
def create(self, index, id, body, doc_type=None, params=None, headers=None):

创建或更新索引中的文档。
def index(self, index, body, doc_type=None, id=None, params=None, headers=None):

允许在单个请求中执行多个索引/更新/删除操作。
def bulk(self, body, index=None, doc_type=None, params=None, headers=None):

显式清除滚动的搜索上下文。
def clear_scroll(self, body=None, scroll_id=None, params=None, headers=None):

返回与查询匹配的文档数。
def count(self, body=None, index=None, doc_type=None, params=None, headers=None):

def delete(self, index, id, doc_type=None, params=None, headers=None):

删除与提供的查询匹配的文档。
def delete_by_query(self, index, body, doc_type=None, params=None, headers=None):

有关文档是否存在于索引中的信息。
def 存在（自我，索引，id，doc_type=None，params=None，headers=None）：

返回有关文档源是否存在于索引中的信息。
def exists_source(self, index, id, doc_type=None, params=None, headers=None):

返回一个文档。
def get(self, index, id, doc_type=None, params=None, headers=None):

允许在一个请求中获取多个文档。
def mget(self, body, index=None, doc_type=None, params=None, headers=None):

允许在一个请求中执行多个搜索操作。
def msearch(self, body, index=None, doc_type=None, params=None, headers=None):

允许从单个搜索请求中检索大量结果。
def scroll(self, body=None, scroll_id=None, params=None, headers=None):

返回匹配查询的结果。
def search(self, body=None, index=None, doc_type=None, params=None, headers=None):

使用脚本或部分文档更新文档。
def update(self, index, id, body, doc_type=None, params=None, headers=None):

创建或更新脚本。
def put_script(self, id, body, context=None, params=None, headers=None):
'''