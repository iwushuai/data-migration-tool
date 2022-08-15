'''
Descripttion: 
Author: wushuai
version: 1.0.0
Date: 2022-08-10 09:58:26
LastEditors: wushuai
LastEditTime: 2022-08-11 16:20:36
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
        '''
        删除索引
        '''
        try:
            url = '/{}'.format(index)
            ret = self.esClient.delete(url)
            logger.info("==>删除索引{}完成！运行结果：{}".format(index, ret))
        except Exception as e:
            info = traceback.print_exc()
            logger.info("==>删除索引{}异常！运行结果：{}".format(index, info))
            logger.error(info) 
    
    def createIndex(self, index, mapping):
        '''
        创建索引
        '''
        try:
            url = '/{}'.format(index)
            ret = self.esClient.put(url, json.dumps(mapping))
            logger.info("==>创建索引{}完成！运行结果：{}".format(index, ret))
        except Exception as e:
            info = traceback.print_exc()
            logger.info("==>创建索引{}异常！运行结果：{}".format(index, info))
            logger.error(info) 
    
    def settingIndex(self, index, body):
        '''
        配置索引
        '''
        url = "/{}/_settings".format(index)
        ret = self.esClient.put(url, body=body)
        logger.info("==>配置索引{}完成！配置项：{}，运行结果：{}".format(index, body, ret))

    @logger.catch
    def start(self):
        logger.info("=================Init::start()::初始化开始！=================")
        indexs = None
        for table in self.tables:
            # 初始化ES索引
            initMapping = table['initMapping']
            if initMapping == True:
                desIndex = table['desIndex']
                mapping = table['mapping']
                startSetting = table['startSetting']
                self.delIndex(desIndex)
                self.createIndex(desIndex, mapping)
                self.settingIndex(desIndex, json.dumps(startSetting)) 
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

    @logger.catch
    def recover(self):
        logger.info("=================Init::recover()::恢复开始！=================")
        indexs = None
        for table in self.tables:
            # 初始化ES索引
            initMapping = table['initMapping']
            if initMapping == True:
                desIndex = table['desIndex']
                finishedSetting = table['finishedSetting']
                self.settingIndex(desIndex, json.dumps(finishedSetting)) 
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
        logger.info("=================Init::recover()::恢复完成！=================")
        
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

    
