#!/usr/bin/python3
# encoding: utf-8
'''
Descripttion: 
Author: wushuai
version: 1.0.0
Date: 2022-07-26 21:19:06
LastEditors: wushuai
LastEditTime: 2022-11-03 18:45:14
'''
from multiprocessing import Lock
from encodings import utf_8
import pymysql
from pymysql.cursors import DictCursor
from DBUtils.PooledDB import PooledDB
import traceback
from common.Common import logger, conf_reader

class MysqlClient(object):

    """
    MYSQL数据库对象，负责产生数据库连接 , 此类中的连接采用连接池实现获取连接对象：conn = Mysql.getConn()
            释放连接对象;conn.close()或del conn
    """
    POOL = None
        
    def __init__(self, host, port, user, password, dbname=None, charset=None, mincached=None, maxcached=None, maxconnections=None, maxshared=None, blocking=None):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = str(password)
        self.dbname = dbname
        self.charset = charset
        self.mincached = int(mincached)
        self.maxcached = int(maxcached)
        self.maxconnections = int(maxconnections)
        self.conn = None
        self.cursor = None
        self.maxshared = maxshared
        self.blocking = blocking
        self.__init_pool()

    def __init_pool(self):
        '''
        初始化连接池到当前对象，并且返回
        '''
        if self.POOL is None:
            self.POOL = PooledDB(creator=pymysql,
                              mincached=self.mincached,
                              maxcached=self.maxcached,
                              host=self.host,
                              port=self.port,
                              user=self.user,
                              passwd=self.password,
                              db=self.dbname,
                              use_unicode=False,
                              maxconnections=self.maxconnections,
                              maxshared=self.maxshared,
                              charset=self.charset,
                              cursorclass=DictCursor,
                              blocking=self.blocking)
        
    def __commit(self, conn, cursor):
        """
        @summary: 结束事务，释放资源回连接池
        """
        conn.commit()
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    def __rollback(self, conn, cursor):
        """
        @summary: 回滚事务
        """
        conn.rollback()
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    def getAll(self, sql, param=None):
        """
        @summary: 执行查询，并取出所有结果集
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list(字典对象)/boolean 查询到的结果集
        """
        # 从线程池获取新连接
        conn = self.POOL.connection()
        cursor = conn.cursor()

        try:
            if param is None:
                count = cursor.execute(sql)
            else:
                count = cursor.execute(sql, param)
            if count > 0:
                result = cursor.fetchall()
            else:
                result = False
            # 事务提交
            self.__commit(conn, cursor)
        except Exception as e:
            # 事务回滚
            self.__rollback(conn, cursor)
            # 抛出异常
            logger.error(traceback.print_exc())
            raise e
        finally:
            # 返回结果
            return result

    def getOne(self, sql, param=None):
        """
        @summary: 执行查询，并取出第一条
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        # 从线程池获取新连接
        conn = self.POOL.connection()
        cursor = conn.cursor()

        result = None
        try:
            count = None
            if param is None:
                count = cursor.execute(sql)
            else:
                count = cursor.execute(sql, param)
            if count > 0:
                result = cursor.fetchone()
            else:
                result = False
            # 事务提交
            self.__commit(conn, cursor)
        except Exception as e:
            # 事务回滚
            self.__rollback(conn, cursor)
            # 抛出异常
            logger.err
            raise e
        finally:
            # 返回结果
            return result

    def getMany(self, sql, num, param=None):
        """
        @summary: 执行查询，并取出num条结果
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param num:取得的结果条数
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        # 从线程池获取新连接
        conn = self.POOL.connection()
        cursor = conn.cursor()

        try:
            if param is None:
                count = cursor.execute(sql)
            else:
                count = cursor.execute(sql, param)

            if count > 0:
                result = cursor.fetchmany(num)
            else:
                result = False
            # 事务提交
            self.__commit(conn, cursor)
        except Exception as e:
            # 事务回滚
            self.__rollback(conn, cursor)
            # 抛出异常
            logger.error(traceback.print_exc())
            raise e
        finally:
            # 返回结果
            return result

    def insertMany(self, sql, values):
        """
        @summary: 向数据表插入多条记录
        @param sql:要插入的ＳＱＬ格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @return: count 受影响的行数
        """
        # 从线程池获取新连接
        conn = self.POOL.connection()
        cursor = conn.cursor()

        try:
            count = cursor.executemany(sql, values)
            self.__commit(conn, cursor)
        except Exception as e:
            # 事务回滚
            self.__rollback(conn, cursor)
            # 抛出异常
            logger.error(traceback.print_exc())
            raise e
        finally:
            # 返回结果
            return count

    def update(self, sql, param=None):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query(sql, param)

    def insert(self, sql, param=None):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query( sql, param)

    def delete(self, sql, param=None):
        """
        @summary: 删除数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要删除的条件 值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query(sql, param)

    def __query(self, sql, param=None):
        # 从线程池获取新连接
        conn = self.POOL.connection()
        cursor = conn.cursor()

        try:
            if param is None:
                count = cursor.execute(sql)
            else:
                count = cursor.execute(sql, param)
            self.__commit(conn, cursor)
        except Exception as e:
            # 事务回滚
            self.__rollback(conn, cursor)
            # 抛出异常
            logger.error(traceback.print_exc())
            raise e
        finally:
            # 返回结果
            return count
