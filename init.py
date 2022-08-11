'''
Descripttion: 
Author: wushuai
version: 1.0.0
Date: 2022-08-10 09:58:26
LastEditors: wushuai
LastEditTime: 2022-08-11 11:39:37
可以输入预定的版权声明、个性签名、空行等
'''
import json
from time import sleep
import traceback
from es_client import ESClient
from desencrypt import *
from common import *


class Init(object):
    esClient = None
    tables = None

    def __init__(self, esClient, tables):
        self.esClient = esClient
        self.tables = tables   

    def delIndex(self, index):
        try:
            url = '/{}'.format(index)
            ret = self.esClient.delete(url)
            logger.info("==>删除索引{}完成！运行结果：{}".format(index, ret))
        except Exception as e:
            info = traceback.print_exc()
            logger.info("==>删除索引{}异常！运行结果：{}".format(index, info))
            logger.error(info) 

    def createIndex(self, index, mapping):
        try:
            url = '/{}'.format(index)
            ret = self.esClient.put(url, json.dumps(mapping))
            logger.info("==>创建索引{}完成！运行结果：{}".format(index, ret))
        except Exception as e:
            info = traceback.print_exc()
            logger.info("==>创建索引{}异常！运行结果：{}".format(index, info))
            logger.error(info) 

    def start(self):
        logger.info("=================Init::start()::初始化开始！=================")
        indexs = None
        for table in self.tables:
            initMapping = table['initMapping']
            #  删除原来索引，创建新索引
            if initMapping == True:
                desIndex = table['desIndex']
                mapping = table['mapping']
                self.delIndex(desIndex)
                self.createIndex(desIndex, mapping)
            # 拼接索引
            if indexs == None:
                indexs = desIndex
            else:
                indexs = indexs + "," + desIndex
        
        # 集群状态
        clusterStats = self.esClient.get("/_cluster/stats")
        logger.info("==>当前集群状态：{}".format(clusterStats))
        # 索引状态
        indexStats = self.esClient.get('/{}/_stats'.format(indexs))
        logger.info("==>当前索引状态：{}".format(indexStats))
        logger.info("=================Init::start()::初始化完成！=================")
        
def geneESClient(config):
    '''
    初始化ElasticSearch连接
    '''
    for host in config['hosts']:
        host['http_auth'] = des_descrypt(host['http_auth'])
    return ESClient(**esConfig)
            
if __name__ == "__main__":
    # 迁移表集合
    tables = conf_reader["business"]["tables"]
    # 生成ElasticSearch连接
    esConfig = conf_reader['es']
    esClient = geneESClient(esConfig)
    # 运行初始化作业
    Init(esClient, tables).start()

    
