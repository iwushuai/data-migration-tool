# 环境要求

本脚本使用Python3.9.9版本，并且依赖如下第三方库：

```python
concurrent 
	from concurrent.futures import ThreadPoolExecutor, thread
base64
	import base64
pyDes   
	from pyDes import *
elasticsearch 版本7.10
	from elasticsearch import Elasticsearch, RequestsHttpConnection
	from elasticsearch import Transport
	from elasticsearch.exceptions import NotFoundError
traceback	
	import traceback
pyyaml
	import yaml 
loguru
	from loguru import logger
pymysql
	import pymysql
	from pymysql.cursors import DictCursor
DBUtils 版本1.3
	from DBUtils.PooledDB import PooledDB
requests
	import requests
```

# 数据模型

数据模型定义考虑数据建模、索引定义，指定分片数量、副本数量。注意：ElasticSearch7.x默认不再支持指定索引类型，默认_doc。

考虑到GreateDB与ElasticSearch字段类型差异，自定义对应规则为：

- bigint -> type:long
- tinyint -> type:long
- varchar -> type:keyword
- datetime -> type:date, format:"yyyy-MM-dd HH:mm:ss"
- varchar(500) -> type:text, fielddata: true

本次迁移涉及serv_biz_code、sur_subscriber、sur_subscriber_detail、sur_subscription_detail、sur_subscriber_instance、sur_subscription_parameter、sur_subscriber_subscription、sur_member_subscription七张数据库表，数据建模结果如下：

```
PUT /billing_serv_biz_code
{
	"settings": {
		"number_of_shards": 6,
		"number_of_replicas": 1
	},
	"mappings": {
		"properties": {
			"ID": {
				"type": "long"
			},
			"EC_CODE": {
				"type": "keyword"
			},
			"SERV_CODE": {
				"type": "keyword"
			},
			"BIZ_CODE": {
				"type": "keyword"
			},
			"PROD_ORDER_ID": {
				"type": "keyword"
			},
			"ORDER_ID": {
				"type": "keyword"
			},
			"BIZ_CODE_APPLY": {
				"type": "long"
			},
			"PROV_CODE": {
				"type": "keyword"
			},
			"PRODUCT_CODE": {
				"type": "keyword"
			},
			"SERVICE_CODE": {
				"type": "keyword"
			},
			"EFFECTIVE_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"EXPIRY_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"EC_GROUP_ID": {
				"type": "keyword"
			},
			"SERVICE_TYPE": {
				"type": "keyword"
			},
			"SEND_PROV": {
				"type": "keyword"
			},
			"PROD_ORDER_MODE": {
				"type": "keyword"
			},
			"SUB_GROUP_FLAG": {
				"type": "long"
			},
			"CARRY_TYPE": {
				"type": "keyword"
			},
			"SIGN_ENTITY": {
				"type": "keyword"
			},
			"PARENT_ORDER_ID": {
				"type": "keyword"
			},
			"LOCON_FLAG": {
				"type": "long"
			},
			"ORDER_LEVEL": {
				"type": "long"
			}
		}
	}
}`
```

```
PUT /billing_sur_member_subscription
{
	"settings": {
		"number_of_shards": 6,
		"number_of_replicas": 1
	},
	"mappings": {
		"properties": {
			"SUBSCRIPTION_ID": {
				"type": "long"
			},
			"EFFECTIVE_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"EXPIRY_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"SUBSCRIBER_ID": {
				"type": "long"
			},
			"MEMBER_NUMBER": {
				"type": "keyword"
			},
			"MEM_GRP_NUMBER": {
				"type": "keyword"
			},
			"CREATION_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"CHANGE_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"DESCRIPTION": {
				"type": "text",
				"fielddata": true
			},
			"SUBSCRIPTION_CATEGORY": {
				"type": "long"
			},
			"SUBSCRIBED_OFFER_ID": {
				"type": "long"
			}
		}
	}
}
```

```
PUT /billing_sur_subscriber
{
	"settings": {
		"number_of_shards": 6,
		"number_of_replicas": 1
	},
	"mappings": {
		"properties": {
			"SUBSCRIBER_ID": {
				"type": "long"
			},
			"EFFECTIVE_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"EXPIRY_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"SUBSCRIBER_NUMBER": {
				"type": "keyword"
			},
			"CREATION_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"EXTERNAL_SUBSCRIBER_ID": {
				"type": "keyword"
			},
			"CUSTOMER_ID": {
				"type": "long"
			},
			"INSTANCE_ID": {
				"type": "long"
			},
			"ID": {
				"type": "long"
			}
		}
	}
}
```

```
PUT /billing_sur_subscriber_detail
{
	"settings": {
		"number_of_shards": 6,
		"number_of_replicas": 1
	},
	"mappings": {
		"properties": {
			"SUBSCRIBER_ID": {
				"type": "long"
			},
			"EFFECTIVE_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"EXPIRY_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"SUBSCRIBER_STATUS_CODE": {
				"type": "keyword"
			},
			"CHANGE_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"SUBSCRIBER_DETAIL_ID": {
				"type": "long"
			},
			"SEGMENT_CODE": {
				"type": "keyword"
			}
		}
	}
}
```

```
PUT /billing_sur_subscriber_instance
{
	"settings": {
		"number_of_shards": 6,
		"number_of_replicas": 1
	},
	"mappings": {
		"properties": {
			"SUBSCRIPTION_ID": {
				"type": "long"
			},
			"EFFECTIVE_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"EXPIRY_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"INSTANCE_ID": {
				"type": "long"
			},
			"CREATION_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"CHANGE_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"DESCRIPTION": {
				"type": "text",
				"fielddata": true
			},
			"SUBSCRIPTION_CATEGORY": {
				"type": "long"
			},
			"SUBSCRIBED_OFFER_ID": {
				"type": "long"
			}
		}
	}
}
```

```
PUT /billing_sur_subscriber_subscription
{
	"settings": {
		"number_of_shards": 6,
		"number_of_replicas": 1
	},
	"mappings": {
		"properties": {
			"SUBSCRIPTION_ID": {
				"type": "long"
			},
			"EFFECTIVE_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"EXPIRY_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"SUBSCRIBER_ID": {
				"type": "long"
			},
			"CREATION_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"CHANGE_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"DESCRIPTION": {
				"type": "text",
				"fielddata": true
			},
			"SUBSCRIPTION_CATEGORY": {
				"type": "long"
			},
			"SUBSCRIBED_OFFER_ID": {
				"type": "long"
			}
		}
	}
}
```

```
PUT /billing_sur_subscription_parameter
{
	"settings": {
		"number_of_shards": 6,
		"number_of_replicas": 1
	},
	"mappings": {
		"properties": {
			"PARAMETER_ID": {
				"type": "long"
			},
			"DESCRIPTION": {
				"type": "text",
				"fielddata": true
			},
			"PARAMETER_VALUE": {
				"type": "text",
				"fielddata": true
			},
			"VALUE_UNIT_BASE_ID": {
				"type": "long"
			},
			"PARAMETER_DEF_ID": {
				"type": "long"
			},
			"SUBSCRIPTION_ID": {
				"type": "long"
			}
		}
	}
}
```

```
PUT /billing_sur_subscription_detail
{
	"settings": {
		"number_of_shards": 6,
		"number_of_replicas": 1
	},
	"mappings": {
		"properties": {
			"SUBSCRIPTION_ID": {
				"type": "long"
			},
			"EFFECTIVE_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"EXPIRY_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"CHANGE_DATE": {
				"type": "date",
				"format": "yyyy-MM-dd HH:mm:ss"
			},
			"SUBSCRIPTION_DETAIL_ID": {
				"type": "long"
			},
			"SUBSCRIPTION_STATUS_CODE": {
				"type": "keyword"
			}
		}
	}
}
```



# 如何使用

测试域主机`appop@10.248.46.88`，切换`root`用户。

配置脚本参数

```
vim ./conf/billing.yml
# 业务配置
business:
  # 线程名称前缀
  threadnameprefix: billing_thread
  # 最大开启线程数
  threadmaxnum: 5
  # 最大读取行数
  readmaxline: 10
  # 迁移表配置。srcTable-迁移原始表、desIndex-迁移目标表、initMapping-初始化数据模型、mapping-数据模型
  tables: 
    - {'srcTable': 'serv_biz_code', 'desIndex': 'billing_serv_biz_code', 'initMapping': True, 'mapping': {"settings":{"number_of_shards":6,"number_of_replicas":1},"mappings":{"properties":{"ID":{"type":"long"},"EC_CODE":{"type":"keyword"},"SERV_CODE":{"type":"keyword"},"BIZ_CODE":{"type":"keyword"},"PROD_ORDER_ID":{"type":"keyword"},"ORDER_ID":{"type":"keyword"},"BIZ_CODE_APPLY":{"type":"long"},"PROV_CODE":{"type":"keyword"},"PRODUCT_CODE":{"type":"keyword"},"SERVICE_CODE":{"type":"keyword"},"EFFECTIVE_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"EXPIRY_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"EC_GROUP_ID":{"type":"keyword"},"SERVICE_TYPE":{"type":"keyword"},"SEND_PROV":{"type":"keyword"},"PROD_ORDER_MODE":{"type":"keyword"},"SUB_GROUP_FLAG":{"type":"long"},"CARRY_TYPE":{"type":"keyword"},"SIGN_ENTITY":{"type":"keyword"},"PARENT_ORDER_ID":{"type":"keyword"},"LOCON_FLAG":{"type":"long"},"ORDER_LEVEL":{"type":"long"}}}}}
    - {'srcTable': 'sur_member_subscription', 'desIndex': 'billing_sur_member_subscription', 'initMapping': True, 'mapping': {"settings":{"number_of_shards":6,"number_of_replicas":1},"mappings":{"properties":{"SUBSCRIPTION_ID":{"type":"long"},"EFFECTIVE_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"EXPIRY_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"SUBSCRIBER_ID":{"type":"long"},"MEMBER_NUMBER":{"type":"keyword"},"MEM_GRP_NUMBER":{"type":"keyword"},"CREATION_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"CHANGE_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"DESCRIPTION":{"type":"text","fielddata":true},"SUBSCRIPTION_CATEGORY":{"type":"long"},"SUBSCRIBED_OFFER_ID":{"type":"long"}}}} }
    - {'srcTable': 'sur_subscriber', 'desIndex': 'billing_sur_subscriber', 'initMapping': True, 'mapping': {"settings":{"number_of_shards":6,"number_of_replicas":1},"mappings":{"properties":{"SUBSCRIBER_ID":{"type":"long"},"EFFECTIVE_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"EXPIRY_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"SUBSCRIBER_NUMBER":{"type":"keyword"},"CREATION_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"EXTERNAL_SUBSCRIBER_ID":{"type":"keyword"},"CUSTOMER_ID":{"type":"long"},"INSTANCE_ID":{"type":"long"},"ID":{"type":"long"}}}} }
    - {'srcTable': 'sur_subscriber_detail', 'desIndex': 'billing_sur_subscriber_detail', 'initMapping': True, 'mapping': {"settings":{"number_of_shards":6,"number_of_replicas":1},"mappings":{"properties":{"SUBSCRIBER_ID":{"type":"long"},"EFFECTIVE_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"EXPIRY_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"SUBSCRIBER_STATUS_CODE":{"type":"keyword"},"CHANGE_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"SUBSCRIBER_DETAIL_ID":{"type":"long"},"SEGMENT_CODE":{"type":"keyword"}}}} }
    - {'srcTable': 'sur_subscriber_instance', 'desIndex': 'billing_sur_subscriber_instance', 'initMapping': True, 'mapping': {"settings":{"number_of_shards":6,"number_of_replicas":1},"mappings":{"properties":{"SUBSCRIPTION_ID":{"type":"long"},"EFFECTIVE_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"EXPIRY_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"INSTANCE_ID":{"type":"long"},"CREATION_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"CHANGE_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"DESCRIPTION":{"type":"text","fielddata":true},"SUBSCRIPTION_CATEGORY":{"type":"long"},"SUBSCRIBED_OFFER_ID":{"type":"long"}}}} }
    - {'srcTable': 'sur_subscriber_subscription', 'desIndex': 'billing_sur_subscriber_subscription', 'initMapping': True, 'mapping': {"settings":{"number_of_shards":6,"number_of_replicas":1},"mappings":{"properties":{"SUBSCRIPTION_ID":{"type":"long"},"EFFECTIVE_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"EXPIRY_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"SUBSCRIBER_ID":{"type":"long"},"CREATION_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"CHANGE_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"DESCRIPTION":{"type":"text","fielddata":true},"SUBSCRIPTION_CATEGORY":{"type":"long"},"SUBSCRIBED_OFFER_ID":{"type":"long"}}}} }
    - {'srcTable': 'sur_subscription_parameter', 'desIndex': 'billing_sur_subscription_parameter', 'initMapping': True, 'mapping': {"settings":{"number_of_shards":6,"number_of_replicas":1},"mappings":{"properties":{"PARAMETER_ID":{"type":"long"},"DESCRIPTION":{"type":"text","fielddata":true},"PARAMETER_VALUE":{"type":"text","fielddata":true},"VALUE_UNIT_BASE_ID":{"type":"long"},"PARAMETER_DEF_ID":{"type":"long"},"SUBSCRIPTION_ID":{"type":"long"}}}} }
    - {'srcTable': 'sur_subscription_detail', 'desIndex': 'billing_sur_subscription_detail', 'initMapping': True, 'mapping': {"settings":{"number_of_shards":6,"number_of_replicas":1},"mappings":{"properties":{"SUBSCRIPTION_ID":{"type":"long"},"EFFECTIVE_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"EXPIRY_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"CHANGE_DATE":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"SUBSCRIPTION_DETAIL_ID":{"type":"long"},"SUBSCRIPTION_STATUS_CODE":{"type":"keyword"}}}} } 
  

# MYSQL 数据库配置 
mysql:
  host: 10.248.22.138
  port: 3306
  user: billing
  password: '80b4d877550de350ff4bd650625f923e'
  dbname: boss_billing
  charset: utf8
  # 池中的初始空闲连接数（0 表示启动时没有建立连接）
  mincached: 2
  # 池中最大空闲连接数（0 或 None 表示无限池大小）
  maxcached: 1
  # 允许的最大连接数（0 或 None 表示任意数量的连接）
  maxconnections: 2
  # 允许线程共享的最大连接数（默认0）
  maxshared: 2
  # 超过最大值的行为（默认False） True-阻塞等待
  blocking: True

# ElasticSearch 搜索引擎配置
es:
  hosts:
  - { 'host':'10.248.50.224', 'port': 9001, 'http_auth': '014b58793dd2fed9ce982fcdac87e22a0333595e261ee867', 'timeout': 120}
  - { 'host':'10.248.50.224', 'port': 9002, 'http_auth': '014b58793dd2fed9ce982fcdac87e22a0333595e261ee867', 'timeout': 120}
  - { 'host':'10.248.50.224', 'port': 9003, 'http_auth': '014b58793dd2fed9ce982fcdac87e22a0333595e261ee867', 'timeout': 120}
```

创建数据模型（支持多次运行）

`python3 init.py`

开始运行作业

`nohup python3 start.py &`

查看运行日志

示例：`tail -10f ./logs/billing-20220810-info.log`