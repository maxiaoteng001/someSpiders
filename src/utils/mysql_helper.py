import pymysql
import logging
import traceback
import json


class MysqlHelper(object):
    """
    封装pymysql, 默认编码utf8mb4
    传入dbconfig, 或在mysql的bo_lib.mysql_cache中配置dbconfig, 传入group即可
    创建连接时可以指定database, 不传入时, insert/insert_many需要 insert('db_name.table_name', {})

    eg: 
        dbconfig = {
            'host': 'localhost',
            "port": 3306,
            'user': 'root',
            'password': 'root',
        }
        client = MysqlHelper(dbconfig)
        client.insert('db_name.table_name', {})
    """

    def __init__(self, dbconfig=None):
        self.database = None
        if dbconfig:
            self._init_by_dbconfig(dbconfig)
        else:
            dbconfig = {
                'host': 'localhost',
                "port": 3306,
                'user': 'root',
                'password': 'root',
            }
            self._init_by_dbconfig(dbconfig)
        self._conn = None
        self._connect()

    @property
    def logger(self):
        return logging.getLogger()

    def _init_by_dbconfig(self, dbconfig):
        self.host = dbconfig.get('host')
        self.port = dbconfig.get('port')
        self.user = dbconfig.get('user')
        self.password = dbconfig.get('password')
        self.database = dbconfig.get('database')

    def _connect(self):
        try:
            if self.database is None:
                self._conn = pymysql.connect(host=self.host, port=self.port,
                                         user=self.user, password=self.password,
                                         charset='utf8mb4'
                                         )
            else:
                self._conn = pymysql.connect(host=self.host, port=self.port,
                                         user=self.user, password=self.password,
                                         database=self.database, charset='utf8mb4'
                                         )
        except pymysql.Error as e:
            self.logger.error(e)

    def query(self, sql):
        '''
        兼容之前的查询, 以及len()操作
        如果为空则返回()
        '''
        try:
            self._conn.ping(reconnect=True)
            with self._conn.cursor(cursor=pymysql.cursors.DictCursor) as _cursor:
                _cursor.execute(sql)
                result = _cursor.fetchall()
                self._conn.commit()
        except pymysql.Error as e:
            result = None
            self.logger.error(e)
        finally:
            return result

    def query_for_long(self, sql):
        '''
        return: _cursor
        只在查询大量数据时使用
        _cursor在使用结束后关闭, 否则报错
        '''
        try:
            self._conn.ping(reconnect=True)
            _cursor = self._conn.cursor(cursor=pymysql.cursors.SSDictCursor)
            _cursor.execute(sql)
            result = _cursor
        except pymysql.Error as e:
            result = None
            self.logger.error(e)
        finally:
            return result

    def insert(self, table, data, retry_times=3):
        # 检查数据格式, dict/list 序列化
        self.data_serialize(data)
        placeholders = ', '.join(['%s'] * len(data))  # 按照dict长度返回如：%s, %s 的占位符
        columns = ', '.join(data.keys())  # 按照dict返回列名，如：age, name
        param = tuple(data.values())
        for _ in range(retry_times):
            try:
                insert_sql = "INSERT IGNORE INTO {} ({}) VALUES ( {} )".format(
                    table, columns, placeholders)  # INSERT INTO mytable ( age, name ) VALUES ( %s, %s )
                self._conn.ping(reconnect=True)
                with self._conn.cursor(cursor=pymysql.cursors.DictCursor) as _cursor:
                    _cursor.execute(insert_sql, param)
                    self._conn.commit()
                break
            except Exception as e:
                self.logger.exception("{}, {}, {}".format(
                    data, e, traceback.format_exc()))
                self.rollback()

    def insert_many(self, table, datas, batch_size=10000, retry_times=3):
        '''
        确保所有的数据包含相同的key
        '''
        if datas == []:
            return
        # 检查数据格式, dict/list 序列化
        for data in datas:
            self.data_serialize(data)
        # 按照dict长度返回如：%s, %s 的占位符
        placeholders = ', '.join(['%s'] * len(datas[0]))
        basic_keys = datas[0].keys()
        columns = ', '.join(datas[0].keys())
        # 按batch_size设定批量插入, 避免一次写入过多数据
        for i in range(0, len(datas), batch_size):
            sub_datas = datas[i:i+batch_size]
            param = []
            for data in sub_datas:
                # param的顺序要保持一致
                param.append(tuple(data.get(k) for k in basic_keys))
            for _ in range(retry_times):
                try:
                    insert_sql = "INSERT IGNORE INTO {} ({}) VALUES ( {} )".format(
                        table, columns, placeholders)  # INSERT INTO mytable ( age, name ) VALUES ( %s, %s )
                    # 批量插入
                    self._conn.ping(reconnect=True)
                    with self._conn.cursor(cursor=pymysql.cursors.DictCursor) as _cursor:
                        _cursor.executemany(insert_sql, param)
                        self._conn.commit()
                    break
                except Exception as e:
                    self.logger.exception("{}, {}, {}".format(
                        datas[0], e, traceback.format_exc()))
                    self.rollback()

    @staticmethod
    def data_serialize(data):
        '''
        dict/list 序列化
        '''
        for k, v in data.items():
            if isinstance(v, list) or isinstance(v, dict):
                data[k] = json.dumps(v)
        return data

    def rollback(self):
        self._conn.rollback()

    def __del__(self):
        try:
            self._conn.close()
        except pymysql.Error as e:
            self.logger.error(e)

    def close(self):
        self.__del__()


if __name__ == "__main__":
    # from bo_lib.general import MysqlHelper
    dbconfig = {
        'host': 'localhost',
        "port": 3306,
        'user': 'root',
        'password': 'root',
    }

    client = MysqlHelper(dbconfig)
    client.query('select * from db_name.table_name')
    client.insert('db_name.table_name', {})
    client.insert_many('db_name.table_name', [])
