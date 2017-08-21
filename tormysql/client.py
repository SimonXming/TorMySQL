# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

'''
MySQL asynchronous client.
'''

from tornado.ioloop import IOLoop
from tornado.gen import Future
from .util import async_call_method
from .connections import Connection
from .cursor import Cursor


class Client(object):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._connection = None
        self._closed = False
        self._close_callback = None

        if "cursorclass" in kwargs and issubclass(kwargs["cursorclass"], Cursor):
            kwargs["cursorclass"] = kwargs["cursorclass"].__delegate_class__

    def connect(self):
        # 入口函数
        # 设置 future 占位符
        future = Future()

        # 定义回调函数
        def on_connected(connection_future):
            if connection_future._exc_info is None:
                future.set_result(self)
            else:
                future.set_exc_info(connection_future.exc_info())
        self._connection = Connection(defer_connect = True, *self._args, **self._kwargs)
        self._connection.set_close_callback(self.connection_close_callback)

        # 用 greenlet 包装 self._connection.connect 并返回 future
        # 要使 async_call_method 包装后的函数有非阻塞的特性，必须达成以下要求
        # 1. 函数可以访问 父greenlet
        # 2. 函数中所有 IO 操作均支持非阻塞(比如: 非阻塞由 socket 的 non-blocking 特性支持)
        # 3. 函数中执行 IO 操作后立即将运行权交还给主函数(父greenlet, 如：ioloop 时间循环)(greenlet.switch)
        # 4. 函数中所有 IO 操作均返回 Future
        # 5. Future.callback 运行后立即将运行权(greenlet.switch)返回给当前函数(greenlet.current)，完成当前函数的剩余部分
        connection_future = async_call_method(self._connection.connect)

        # 当 connection_future 状态为 finished, 调用 on_connected()
        # finished => 调用 connection_future.set_result()
        IOLoop.current().add_future(connection_future, on_connected)
        return future

    def connection_close_callback(self):
        self._closed = True
        if self._close_callback and callable(self._close_callback):
            close_callback, self._close_callback = self._close_callback, None
            close_callback(self)

    def set_close_callback(self, callback):
        self._close_callback = callback

    def close(self):
        if self._closed:
            return
        return async_call_method(self._connection.close)

    def autocommit(self, value):
        return async_call_method(self._connection.autocommit, value)

    def begin(self):
        return async_call_method(self._connection.begin)

    def commit(self):
        return async_call_method(self._connection.commit)

    def rollback(self):
        return async_call_method(self._connection.rollback)

    def show_warnings(self):
        return async_call_method(self._connection.show_warnings)

    def select_db(self, db):
        return async_call_method(self._connection.select_db, db)

    def cursor(self, cursor_cls=None):
        if cursor_cls is None:
            cursor_cls = self._connection.cursorclass

        cursor = self._connection.cursor(
            cursor_cls.__delegate_class__ if cursor_cls and issubclass(cursor_cls, Cursor) else cursor_cls
        )

        if issubclass(cursor_cls, Cursor):
            return cursor_cls(cursor)
        else:
            return cursor.__tormysql_class__(cursor)

    def query(self, sql, unbuffered=False):
        return async_call_method(self._connection.query, sql, unbuffered)

    def next_result(self):
        return async_call_method(self._connection.next_result)

    def kill(self, thread_id):
        return async_call_method(self._connection.kill, thread_id)

    def ping(self, reconnect=True):
        return async_call_method(self._connection.ping, reconnect)

    def set_charset(self, charset):
        return async_call_method(self._connection.set_charset, charset)

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self.cursor()

    def __exit__(self, *exc_info):
        del exc_info
        self.close()

    def __str__(self):
        return str(self._connection)
