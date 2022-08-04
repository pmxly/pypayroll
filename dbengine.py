#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from sqlalchemy import create_engine
from sqlalchemy import exc
from sqlalchemy import Table, MetaData
from .confreader import conf
from sqlalchemy.pool import NullPool

"""
获取数据库连接配置
"""

__all__ = ['DataBaseAlchemy', 'HubDataBaseAlchemy']


class DataBaseAlchemy:
    __slots__ = ['_conn',
                 '_engine',
                 '_metadata',
                 '__username',
                 '__password',
                 '__ip',
                 '__port',
                 '__db_name',
                 '__pool_size',
                 '__max_overflow'
                 ]

    def __init__(self):
        print('---------DataBaseAlchemy Init---------')
        self._conn = None
        self._engine = None
        self._metadata = None
        self.__username = conf.get('config', 'db_username')
        self.__password = conf.get('config', 'db_password')
        self.__ip = conf.get('config', 'db_ip')
        self.__port = conf.getint('config', 'db_port')
        self.__db_name = conf.get('config', 'db_name')
        self.__pool_size = conf.getint('config', 'db_pool_size')
        self.__max_overflow = conf.getint('config', 'db_max_overflow')

    def get_username(self):
        return self.__username

    def get_password(self):
        return self.__password

    def get_ip(self):
        return self.__ip

    def get_port(self):
        return self.__port

    def get_db_name(self):
        return self.__db_name

    def get_pool_size(self):
        return self.__pool_size

    def get_max_overflow(self):
        return self.__max_overflow

    def __get_db_engine(self):
        engine = create_engine(
            'mysql+pymysql://' + self.get_username() + ':' + self.get_password() + '@' + self.get_ip() + ':' + str(
                self.get_port()) + '/' + self.get_db_name() + '?charset=utf8mb4', pool_size=self.get_pool_size(),
            max_overflow=self.get_max_overflow(), echo=False, encoding='utf-8', pool_recycle=14400)
        # engine = create_engine(
        #     'mysql+pymysql://' + self.get_username() + ':' + self.get_password() + '@' + self.get_ip() + ':' + str(
        #         self.get_port()) + '/' + self.get_db_name() + '?charset=utf8mb4', poolclass=NullPool, encoding='utf-8')
        return engine

    @property
    def conn(self):
        # if self._conn is None:
        #     self._conn = self.__get_db_engine().connect()
        # if self._conn.closed:
        #     self._conn = self.__get_db_engine().connect()

        if self._conn is None:
            self._conn = self.engine.connect()
            # self._conn.execute = self.new_execute
        elif self._conn.closed:
            self._conn = self.engine.connect()
            # self._conn.execute = self.new_execute
        elif self._conn.invalidated:
            self._conn = self.engine.connect()
            # self._conn.execute = self.new_execute
        return self._conn

    @property
    def engine(self):
        if self._engine is None:
            self._engine = self.__get_db_engine()
        return self._engine

    def get_tables(self, table_name, schema_name=None):
        table_dic = {}
        if self._metadata is None:
            self._metadata = MetaData(bind=self.engine)
        if schema_name is None:
            schema_name = self.get_db_name()
        for k, v in table_name.items():
            table_dic[k] = Table(v, self._metadata, autoload=True, schema=schema_name)
        return table_dic

    def get_table(self, table_name, schema_name=None):
        if self._metadata is None:
            self._metadata = MetaData(bind=self.engine)
        if schema_name is None:
            schema_name = self.get_db_name()
        return Table(table_name, self._metadata, autoload=True, schema=schema_name)

    def new_execute(self, obj, *multiparams, **params):
        conn = self.engine.connect()
        try:
            # suppose the database has been restarted.
            re = conn.execute(obj, *multiparams, **params)
            conn.close()
            return re
        except exc.DBAPIError as e:
            # an exception is raised, Connection is invalidated.
            if e.connection_invalidated:
                print("Connection was invalidated!")
            conn = self.engine.connect()
            re = conn.execute(obj, *multiparams, **params)
            conn.close()
            return re
        except exc.OperationalError as oe:
            print("OperationalError!")
            conn = self.engine.connect()
            re = conn.execute(obj, *multiparams, **params)
            conn.close()
            return re


class HubDataBaseAlchemy:
    __slots__ = ['_conn',
                 '_engine',
                 '_metadata',
                 '__username',
                 '__password',
                 '__ip',
                 '__port',
                 '__db_name',
                 '__pool_size',
                 '__max_overflow'
                 ]

    def __init__(self):
        self._conn = None
        self._engine = None
        self._metadata = None
        self.__username = conf.get('config', 'db_username')
        self.__password = conf.get('config', 'db_password')
        self.__ip = conf.get('config', 'db_ip')
        self.__port = conf.getint('config', 'db_port')
        self.__db_name = conf.get('config', 'db_name')
        self.__pool_size = conf.getint('config', 'db_pool_size')
        self.__max_overflow = conf.getint('config', 'db_max_overflow')

    def get_username(self):
        return self.__username

    def get_password(self):
        return self.__password

    def get_ip(self):
        return self.__ip

    def get_port(self):
        return self.__port

    def get_db_name(self):
        return self.__db_name

    def get_pool_size(self):
        return self.__pool_size

    def get_max_overflow(self):
        return self.__max_overflow

    def __get_db_engine(self):
        engine = create_engine(
            'mysql+pymysql://' + self.get_username() + ':' + self.get_password() + '@' + self.get_ip() + ':' + str(
                self.get_port()) + '/' + self.get_db_name() + '?charset=utf8mb4', poolclass=NullPool, encoding='utf-8')
        return engine

    @property
    def conn(self):
        if self._conn is None:
            self._conn = self.engine.connect()
        elif self._conn.closed:
            self._conn = self.engine.connect()
        elif self._conn.invalidated:
            self._conn = self.engine.connect()
        return self._conn

    @property
    def engine(self):
        if self._engine is None:
            self._engine = self.__get_db_engine()
        return self._engine

    def get_tables(self, table_name, schema_name=None):
        table_dic = {}
        if self._metadata is None:
            self._metadata = MetaData(bind=self.engine)
        if schema_name is None:
            schema_name = self.get_db_name()
        for k, v in table_name.items():
            table_dic[k] = Table(v, self._metadata, autoload=True, schema=schema_name)
        return table_dic

    def get_table(self, table_name, schema_name=None):
        if self._metadata is None:
            self._metadata = MetaData(bind=self.engine)
        if schema_name is None:
            schema_name = self.get_db_name()
        return Table(table_name, self._metadata, autoload=True, schema=schema_name)

    def new_execute(self, obj, *multiparams, **params):
        conn = self.engine.connect()
        try:
            # suppose the database has been restarted.
            re = conn.execute(obj, *multiparams, **params)
            conn.close()
            return re
        except exc.DBAPIError as e:
            # an exception is raised, Connection is invalidated.
            if e.connection_invalidated:
                print("Connection was invalidated!")
            conn = self.engine.connect()
            re = conn.execute(obj, *multiparams, **params)
            conn.close()
            return re
        except exc.OperationalError as oe:
            print("OperationalError!")
            conn = self.engine.connect()
            re = conn.execute(obj, *multiparams, **params)
            conn.close()
            return re
