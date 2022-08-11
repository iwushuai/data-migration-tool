#!/usr/bin/python3
# encoding: utf-8
'''
Descripttion: 
Author: wushuai
version: 1.0.0
Date: 2022-07-26 23:33:07
LastEditors: wushuai
LastEditTime: 2022-08-11 11:34:25
'''
import json
from elasticsearch import RequestsHttpConnection
from elasticsearch import Transport
import traceback
from common import *

class ESClient(object):
 
    _index = ""
    _type = ""
    _conn = ""
    
    def __init__(self, hosts):
        # 基于requests实例化es连接池
        self.hosts = hosts
        self.conn_pool = Transport(hosts=hosts, connection_class=RequestsHttpConnection).connection_pool
        self._conn = self.get_conn()
 
    def get_conn(self):
        """
        从连接池获取一个连接
        """
        conn = self.conn_pool.get_connection()
        return conn
 
    def request(self, method, url, headers=None, params=None, body=None):
        """
        想es服务器发送一个求情
        @method     请求方式
        @url        请求的绝对url  不包括域名
        @headers    请求头信息
        @params     请求的参数：dict
        @body       请求体：json对象(headers默认Content-Type为application/json)
        # return    返回体：python内置数据结构
        """
        try:
            status, headers, body = self._conn.perform_request(method, url, headers=headers, params=params, body=body)
        except Exception as e:
            logger.error(traceback.print_exc())
            raise e
			
        if method == "HEAD":
            return status
        return json.loads(body)

 
    def search(self, query=None, method="GET"):
        url = "/%s/%s/_search" % (self._index, self._type)
        if method == "GET":
            data = self.get(url, params=query)
        elif method == "POST":
            data = self.post(url, body=query)
        else:
            return None
        return data
 
    def get(self, url, params=None, method="GET"):
        """
        使用get请求访问es服务器
        """
        data = self.request(method, url, params=params)
        return data
 
    def put(self, url, body=None, method="PUT"):
        """
        使用put请求访问es
        """
        data = self.request(method, url, body=body)
        return data
 
    def post(self, url, body=None, method="POST"):
        """使用post请求访问服务器"""
        data = self.request(method, url, body=body)
        return data
 
    def head(self, url, *args, **kwargs):
        status = self.request("HEAD", url, *args, **kwargs)
        return status
 
    def delete(self, url, *args, **kwargs):
        ret = self.request("DELETE", url, *args, **kwargs)
        return ret

    def bulk(self, url, body=None, *args, **kwargs):
        ret = self.request("POST", url, body=body, *args, **kwargs)
        return ret
