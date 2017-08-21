# -*- encoding: utf-8 -*-
import time
import sys
import uuid
import requests
import random
# import trollius as asyncio
# from trollius import From
import pymysql.cursors
import greenlet
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

from tornado.options import define, options
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.concurrent import Future


connection = pymysql.connect(
    host='localhost',
    user='test',
    password='test',
    db='test',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)


def async_call_method(fun, *args, **kwargs):
    future = Future()
    # 定义一个闭包 finish
    print("get in", fun)

    def finish():
        try:
            print("start func", fun)
            result = fun(*args, **kwargs)
            print("done func", fun)
            if future._callbacks:
                IOLoop.current().add_callback(future.set_result, result)
            else:
                future.set_result(result)
        except:
            if future._callbacks:
                IOLoop.current().add_callback(future.set_exc_info, sys.exc_info())
            else:
                future.set_exc_info(sys.exc_info())
    # greenlet 初始化时会创建了一个的树结构
    # 创建一个 greenlet
    child_gr = greenlet.greenlet(finish)
    # 将执行权交给 child_gr
    # finish() 开始执行
    print("55", id(child_gr))
    child_gr.switch()
    return future


def blocking_func():
    print("blocking start")
    child_gr = greenlet.getcurrent()
    print("64", id(child_gr))
    print("65", child_gr.parent)
    time.sleep(10)
    # gen.sleep(5)
    print("blocking done")


def request_block():
    def req():
        requests.get("http://www.google.com")
    child_gr = greenlet.greenlet(req)
    child_gr.switch()


def slow_query():
    # Connect to the database
    with connection.cursor() as cursor:
        # Create a new record
        sql = "select * from secrets ORDER BY secret_name ASC, secret_value ASC, secret_repo_id DESC;"
        cursor.execute(sql)
        result = cursor.fetchall()
        # sql = "INSERT INTO `secrets` (`secret_id`, `secret_repo_id`, `secret_name`, `secret_value`) VALUES (%s, %s, %s, %s)"
        # cursor.execute(sql, (random.randint(0, 100000000), random.randint(0, 100000000), str(uuid.uuid4().hex), str(uuid.uuid4().hex)))

    # connection is not autocommit by default. So you must commit to save
    # your changes.
    connection.commit()


define("port", default=8888, help="run on the given port", type=int)


class MainHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self):
        print("~~~~~~~ receive request MainHandler")
        yield async_call_method(blocking_func)
        self.write("Hello, world")


class TestHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self):
        yield async_call_method(slow_query)
        self.write("Hello, world")


def main():
    tornado.options.parse_command_line()
    application = tornado.web.Application([
        (r"/", MainHandler),
        (r"/test", TestHandler),
    ])
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()


def main2():
    f = async_call_method(blocking_func)


if __name__ == "__main__":
    main2()

